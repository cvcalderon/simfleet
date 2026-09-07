import math
from uuid import uuid4
import json
import asyncio

from loguru import logger
from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from spade.behaviour import State
from spade.message import Message

from simfleet.communications.protocol import (
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination
)
from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_MOVING_TO_CUSTOMER, \
    TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_IN_CUSTOMER_PLACE, TRANSPORT_MOVING_TO_DESTINATION, \
    TRANSPORT_ARRIVED_AT_DESTINATION, CUSTOMER_IN_TRANSPORT, CUSTOMER_IN_DEST, TRANSPORT_WAITING_FOR_RETURN , \
    TRANSPORT_MOVING_TO_RETURN

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    PROPOSE_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    REQUEST_PERFORMATIVE,
)

# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class TaxiStrategyBehaviour(State):
    """
    Base class to define the transport strategy for a taxi.
    This class should be inherited and extended to create custom strategies.
    Subclasses must override the `run` coroutine to define specific behaviors.

    Methods:
        async on_start():
            Logs the beginning of the strategy execution.
        async on_end():
            Logs the end of the strategy execution.
        async send_proposal(customer_id, content=None):
            Sends a transport proposal to a customer.
        async cancel_proposal(agent_id, content=None):
            Cancels a previously sent proposal to a customer.
        async run():
            Abstract method that must be implemented by subclasses.
    """


    METRICS_MODALITY = "taxi"
    _METRICS_SERVICE_CONTEXT_ATTR = "_metrics_service_context"
    _METRICS_PENDING_MOVEMENT_ATTR = "_metrics_pending_movement"
    _METRICS_MOVEMENT_PHASES = {"approach", "service", "auxiliary"}

    def _metrics_modality(self):
        modality = self.METRICS_MODALITY
        if modality is None:
            raise ValueError(
                "A concrete metrics strategy must declare METRICS_MODALITY."
            )
        return modality

    def get_service_context(self):
        return getattr(
            self.agent,
            self._METRICS_SERVICE_CONTEXT_ATTR,
            None,
        )

    def create_service_context(
        self,
        service_id=None,
        user_id=None,
        transport_id=None,
        origin=None,
        destination=None,
        emit_requested=False,
    ):
        current = self.get_service_context()
        if current is not None:
            requested_id = str(service_id) if service_id is not None else None
            if (
                current.get("terminal_status") is None
                and (
                    requested_id is None
                    or current.get("service_id") == requested_id
                )
            ):
                return current
            logger.warning(
                "Agent[{}]: Refusing to replace metrics service context [{}] "
                "with [{}] before it is cleared.".format(
                    self.agent.name,
                    current.get("service_id"),
                    requested_id,
                )
            )
            return None

        if service_id is None:
            if not emit_requested:
                logger.warning(
                    "Agent[{}]: A transport-side metrics context requires "
                    "the customer-created service_id.".format(self.agent.name)
                )
                return None
            service_id = str(uuid4())
        else:
            service_id = str(service_id)

        if not emit_requested and user_id is None:
            logger.warning(
                "Agent[{}]: A transport-side metrics context requires user_id.".format(
                    self.agent.name
                )
            )
            return None

        context = {
            "service_id": service_id,
            "modality": self._metrics_modality(),
            "user_id": self.agent.bare_jid(
                user_id if user_id is not None else self.agent.jid
            ),
            "transport_id": self.agent.bare_jid(transport_id),
            "origin": origin,
            "destination": destination,
            "requested": True,
            "request_emitted": False,
            "assigned": False,
            "started": False,
            "terminal_status": None,
            "pending_movement": None,
        }
        setattr(
            self.agent,
            self._METRICS_SERVICE_CONTEXT_ATTR,
            context,
        )

        if emit_requested:
            details = self._service_event_details(context)
            details["origin"] = origin
            details["destination"] = destination
            self.agent.events_store.emit(
                event_type="service_requested",
                details=details,
            )
            context["request_emitted"] = True

        return context

    def get_or_create_service_context(self, **kwargs):
        context = self.get_service_context()
        service_id = kwargs.get("service_id")
        if context is not None:
            if (
                service_id is None
                or context.get("service_id") == str(service_id)
            ):
                return context
            logger.warning(
                "Agent[{}]: Message/service context mismatch: [{}] != [{}].".format(
                    self.agent.name,
                    context.get("service_id"),
                    service_id,
                )
            )
            return None
        return self.create_service_context(**kwargs)

    def clear_service_context(self):
        context = self.get_service_context()
        if context is None:
            return True
        if context.get("terminal_status") is None:
            logger.warning(
                "Agent[{}]: Refusing to clear unfinished metrics service [{}].".format(
                    self.agent.name,
                    context.get("service_id"),
                )
            )
            return False
        if context.get("pending_movement") is not None:
            logger.warning(
                "Agent[{}]: Refusing to clear metrics service [{}] with a "
                "pending movement.".format(
                    self.agent.name,
                    context.get("service_id"),
                )
            )
            return False
        setattr(
            self.agent,
            self._METRICS_SERVICE_CONTEXT_ATTR,
            None,
        )
        return True

    def _service_event_details(self, context=None):
        context = context or self.get_service_context()
        if context is None:
            return None
        return {
            "modality": context["modality"],
            "service_id": context["service_id"],
            "user_id": context.get("user_id"),
            "transport_id": context.get("transport_id"),
        }

    def add_service_identifiers(
        self,
        content=None,
        context=None,
        transport_id=None,
    ):
        context = context or self.get_service_context()
        if context is None:
            raise ValueError("Cannot propagate identifiers without a service context.")
        result = dict(content or {})
        if transport_id is not None:
            context["transport_id"] = self.agent.bare_jid(transport_id)
        result.update(self._service_event_details(context))
        return result

    def message_matches_service(self, content, context=None):
        context = context or self.get_service_context()
        if context is None or not isinstance(content, dict):
            return False

        for key in ("service_id", "modality", "user_id"):
            if content.get(key) is None:
                return False

        if str(content["service_id"]) != context["service_id"]:
            return False
        if content["modality"] != context["modality"]:
            return False
        if self.agent.bare_jid(content["user_id"]) != context.get("user_id"):
            return False

        expected_transport = context.get("transport_id")
        received_transport = content.get("transport_id")
        if expected_transport is not None:
            if received_transport is None:
                return False
            if self.agent.bare_jid(received_transport) != expected_transport:
                return False
        return True

    def mark_service_assigned(self, transport_id):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        transport_id = self.agent.bare_jid(transport_id)
        if transport_id is None:
            return False
        if context.get("assigned"):
            return context.get("transport_id") == transport_id
        context["transport_id"] = transport_id
        context["assigned"] = True
        return True

    def mark_service_started(self, transport_id=None):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        if transport_id is not None:
            transport_id = self.agent.bare_jid(transport_id)
            if (
                context.get("transport_id") is not None
                and context.get("transport_id") != transport_id
            ):
                return False
            context["transport_id"] = transport_id
        if context.get("transport_id") is None:
            return False
        context["assigned"] = True
        context["started"] = True
        return True

    def mark_service_completed(self):
        """Mirror a successful terminal event emitted by the customer."""
        context = self.get_service_context()
        if context is None:
            return False
        if context.get("terminal_status") is not None:
            return context.get("terminal_status") == "completed"
        if not context.get("started"):
            return False
        context["terminal_status"] = "completed"
        return True

    def assign_service(self, transport_id, extra_details=None):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        transport_id = self.agent.bare_jid(transport_id)
        if transport_id is None:
            return False
        if context.get("assigned"):
            return False

        context["transport_id"] = transport_id
        context["assigned"] = True
        details = self._service_event_details(context)
        details.update(extra_details or {})
        self.agent.events_store.emit(
            event_type="service_assigned",
            details=details,
        )
        return True

    def start_service(self, transport_id=None, extra_details=None):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        if context.get("started"):
            return False
        if transport_id is not None:
            context["transport_id"] = self.agent.bare_jid(transport_id)
        if context.get("transport_id") is None:
            return False

        context["started"] = True
        details = self._service_event_details(context)
        details.update(extra_details or {})
        self.agent.events_store.emit(
            event_type="service_started",
            details=details,
        )
        return True

    def complete_service(self, extra_details=None):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        if not context.get("started"):
            logger.warning(
                "Agent[{}]: Refusing to complete service [{}] before it starts.".format(
                    self.agent.name,
                    context.get("service_id"),
                )
            )
            return False

        context["terminal_status"] = "completed"
        details = self._service_event_details(context)
        details.update(extra_details or {})
        self.agent.events_store.emit(
            event_type="service_completed",
            details=details,
        )
        return True

    def fail_service(self, failure_reason=None, extra_details=None):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False

        context["terminal_status"] = "failed"
        details = self._service_event_details(context)
        if failure_reason is not None:
            details["failure_reason"] = failure_reason
        details.update(extra_details or {})
        self.agent.events_store.emit(
            event_type="service_failed",
            details=details,
        )
        return True

    def set_pending_movement(
        self,
        phase,
        distance_m,
        extra_details=None,
        require_service=True,
    ):
        if phase not in self._METRICS_MOVEMENT_PHASES:
            raise ValueError("Invalid metrics movement phase: {}".format(phase))
        if (
            isinstance(distance_m, bool)
            or not isinstance(distance_m, (int, float))
            or not math.isfinite(distance_m)
            or distance_m < 0
        ):
            raise ValueError("distance_m must be a finite non-negative number.")

        context = self.get_service_context()
        if require_service and context is None:
            return False
        if getattr(self.agent, self._METRICS_PENDING_MOVEMENT_ATTR, None) is not None:
            logger.warning(
                "Agent[{}]: Refusing to overwrite a pending metrics movement.".format(
                    self.agent.name
                )
            )
            return False

        details = {
            "modality": self._metrics_modality(),
            "transport_id": None,
            "user_id": None,
            "service_id": None,
            "phase": phase,
            "distance_m": float(distance_m),
        }
        if context is not None:
            details.update(self._service_event_details(context))
        else:
            details["transport_id"] = self.agent.bare_jid(self.agent.jid)
        details.update(extra_details or {})

        pending = {"details": details}
        setattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            pending,
        )
        if context is not None:
            context["pending_movement"] = pending
        return True

    def complete_pending_movement(self):
        pending = getattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            None,
        )
        if pending is None:
            return False

        self.agent.events_store.emit(
            event_type="movement_completed",
            details=dict(pending["details"]),
        )
        context = self.get_service_context()
        if context is not None and context.get("pending_movement") is pending:
            context["pending_movement"] = None
        setattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            None,
        )
        return True

    def discard_pending_movement(self):
        pending = getattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            None,
        )
        if pending is None:
            return False
        context = self.get_service_context()
        if context is not None and context.get("pending_movement") is pending:
            context["pending_movement"] = None
        setattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            None,
        )
        return True


    _METRICS_PENDING_OFFER_ATTR = "_metrics_pending_offer"
    _METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR = "_metrics_active_message_context"

    def _validate_metrics_service_request(self, content):
        """Validate a strict schema-1.0 Taxi-like request before proposing."""
        if not isinstance(content, dict):
            return False
        for key in (
            "service_id",
            "modality",
            "user_id",
            "customer_id",
            "origin",
            "dest",
        ):
            if content.get(key) is None:
                return False
        if content.get("modality") != self._metrics_modality():
            return False
        if self.agent.bare_jid(content.get("user_id")) != self.agent.bare_jid(
            content.get("customer_id")
        ):
            return False
        if content.get("transport_id") is not None:
            return False
        return True

    def store_pending_offer(self, content):
        if not self._validate_metrics_service_request(content):
            return None
        pending = {
            "service_id": str(content["service_id"]),
            "modality": content["modality"],
            "user_id": self.agent.bare_jid(content["user_id"]),
            "customer_id": self.agent.bare_jid(content["customer_id"]),
            "transport_id": self.agent.bare_jid(self.agent.jid),
            "origin": content["origin"],
            "destination": content["dest"],
        }
        setattr(self.agent, self._METRICS_PENDING_OFFER_ATTR, pending)
        return pending

    def get_pending_offer(self):
        return getattr(self.agent, self._METRICS_PENDING_OFFER_ATTR, None)

    def clear_pending_offer(self):
        setattr(self.agent, self._METRICS_PENDING_OFFER_ATTR, None)
        return True

    def pending_offer_message_details(self):
        pending = self.get_pending_offer()
        if pending is None:
            return None
        return {
            "service_id": pending["service_id"],
            "modality": pending["modality"],
            "user_id": pending["user_id"],
            "transport_id": pending["transport_id"],
        }

    def message_matches_pending_offer(self, content):
        pending = self.get_pending_offer()
        if pending is None or not isinstance(content, dict):
            return False
        for key in ("service_id", "modality", "user_id", "transport_id", "customer_id"):
            if content.get(key) is None:
                return False
        return (
            str(content["service_id"]) == pending["service_id"]
            and content["modality"] == pending["modality"]
            and self.agent.bare_jid(content["user_id"]) == pending["user_id"]
            and self.agent.bare_jid(content["transport_id"]) == pending["transport_id"]
            and self.agent.bare_jid(content["customer_id"]) == pending["customer_id"]
        )

    def activate_pending_offer(self, content):
        if not self.message_matches_pending_offer(content):
            return None
        pending = dict(self.get_pending_offer())
        setattr(self.agent, self._METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR, pending)
        self.clear_pending_offer()
        return pending

    def get_active_message_context(self):
        return getattr(
            self.agent,
            self._METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR,
            None,
        )

    def active_service_message_details(self):
        active = self.get_active_message_context()
        if active is None:
            return None
        return {
            "service_id": active["service_id"],
            "modality": active["modality"],
            "user_id": active["user_id"],
            "transport_id": active["transport_id"],
        }

    def add_active_service_identifiers(self, content=None):
        details = self.active_service_message_details()
        if details is None:
            return dict(content or {})
        result = dict(content or {})
        result.update(details)
        return result

    def message_matches_active_service(self, content):
        active = self.get_active_message_context()
        if active is None or not isinstance(content, dict):
            return False
        for key in ("service_id", "modality", "user_id", "transport_id"):
            if content.get(key) is None:
                return False
        return (
            str(content["service_id"]) == active["service_id"]
            and content["modality"] == active["modality"]
            and self.agent.bare_jid(content["user_id"]) == active["user_id"]
            and self.agent.bare_jid(content["transport_id"]) == active["transport_id"]
        )

    def clear_active_message_context(self):
        setattr(self.agent, self._METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR, None)
        return True

    async def on_start(self):

        logger.debug(
            "Agent[{}]: Strategy {} started.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def on_end(self):

        logger.debug(
            "Agent[{}]: Strategy {} finished.".format(
                self.agent.name, type(self).__name__
            )
        )


    async def assigned_taxicustomer(
        self,
        customer_id,
        origin=None,
        dest=None
    ):
        self.agent.add_assigned_customer(
            customer_id,
            origin,
            dest
        )

        self.agent.set_busy()

    async def unassigned_taxicustomer(self):
        self.agent.remove_assigned_customer()


    async def send_proposal(self, customer_id, content=None):
        """
        Send a ``spade.message.Message`` with a proposal to a customer to pick up him.
        If the content is empty the proposal is sent without content.

        Args:
            customer_id (str): the id of the customer
            content (dict, optional): the optional content of the message
        """
        if content is None:
            content = {}
        logger.info(
            "Agent[{}]: The agent sent proposal to [{}]".format(self.agent.name, customer_id)
        )
        reply = Message()
        reply.to = customer_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", PROPOSE_PERFORMATIVE)
        reply.body = json.dumps(content)
        await self.send(reply)

    async def cancel_proposal(self, agent_id, content=None):
        """
        Send a ``spade.message.Message`` to cancel a proposal.
        If the content is empty the proposal is sent without content.

        Args:
            agent_id (str): the id of the customer
            content (dict, optional): the optional content of the message
        """
        if content is None:
            content = {}
        content = self.add_active_service_identifiers(content)
        logger.info(
            "Agent[{}]: The agent sent cancel proposal to [{}]".format(
                self.agent.name, agent_id
            )
        )
        reply = Message()
        reply.to = agent_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", CANCEL_PERFORMATIVE)
        reply.body = json.dumps(content)
        await self.send(reply)


    async def inform_customer(self, customer_id, status, data=None):
        """
        Sends a message to inform the customer of the transport's new status.

        Args:
            customer_id (str): The ID of the customer.
            status (int): The new status code.
            data (dict, optional): Additional information about the status.
        """
        if data is None:
            data = {}
        data = self.add_active_service_identifiers(data)
        msg = Message()
        msg.to = customer_id
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        data["status"] = status
        msg.body = json.dumps(data)
        await self.send(msg)

    async def cancel_customer(self, customer_id, data=None):
        """
        Cancels the assignment of a customer and informs them via a message.

        Args:
            customer_id (str): The ID of the customer.
            data (dict, optional): Additional cancellation-related information.
        """
        logger.error(
            "Agent[{}]: The agent could not get a path to customer [{}].".format(
                self.agent.agent_id, self.agent.get("current_customer")
            )
        )
        if data is None:
            data = {}
        data = self.add_active_service_identifiers(data)
        reply = Message()
        reply.to = customer_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", CANCEL_PERFORMATIVE)
        reply.body = json.dumps(data)
        logger.debug(
            "Agent[{}]: The agent sent cancel proposal to customer [{}]".format(
                self.agent.agent_id, customer_id
            )
        )
        await self.send(reply)

    async def request_return_position(self):
        fleetmanager = self.agent.get_registration_fleet()

        if not fleetmanager:
            logger.warning(
                "Agent[{}]: No fleet manager configured for taxi return.".format(
                    self.agent.name
                )
            )
            return

        msg = Message()

        msg.to = str(fleetmanager)
        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        msg.set_metadata(
            "performative",
            REQUEST_PERFORMATIVE
        )

        msg.body = json.dumps(
            {
                "request_type": "taxi_return",
                "position": self.agent.get_position(),
            }
        )

        logger.debug(
            "Agent[{}]: Requesting return point from [{}] at position {}.".format(
                self.agent.name,
                fleetmanager,
                self.agent.get_position()
            )
        )

        await self.send(msg)

    async def run(self):
        raise NotImplementedError

# ==================================================================
# -------------------------End Behaviour----------------------------
# ==================================================================


################################################################
#                                                              #
#                        Taxi Strategy                         #
#                                                              #
################################################################

class TaxiWaitingState(TaxiStrategyBehaviour):
    """
        Represents the 'Waiting' state for the electric taxi. The taxi is waiting to receive a transport request.

        Methods:
            on_start(): Sets the initial state to 'TRANSPORT_WAITING' and logs the state.
            run(): Handles incoming messages, processes transport requests, and transitions to the next state.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING)
            return
        logger.debug("Agent[{}]: The agent received: {}".format(self.agent.jid, msg.body))
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")
        if performative == REQUEST_PERFORMATIVE:
            if self.store_pending_offer(content) is None:
                logger.warning(
                    "Agent[{}]: Ignoring request without valid schema-1.0 identifiers.".format(
                        self.agent.name
                    )
                )
                self.set_next_state(TRANSPORT_WAITING)
                return



            await self.send_proposal(
                content["customer_id"],
                self.pending_offer_message_details(),
            )
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return

        else:
            self.set_next_state(TRANSPORT_WAITING)
            return


class TaxiWaitingForApprovalState(TaxiStrategyBehaviour):
    """
        Represents the state where the taxi is waiting for approval from a customer or station.
        After making a transport offer, the taxi waits for a response (approval or refusal).

        Methods:
            on_start(): Logs the transition to 'Waiting For Approval'.
            run(): Handles incoming approval or refusal messages and transitions accordingly.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING_FOR_APPROVAL

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            if not self.message_matches_pending_offer(content):
                logger.warning(
                    "Agent[{}]: Ignoring stale or mismatched acceptance.".format(
                        self.agent.name
                    )
                )
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
                return
            active = self.activate_pending_offer(content)
            # Handle acceptance by the customer or station
            try:
                context = self.create_service_context(
                    service_id=active["service_id"],
                    user_id=active["user_id"],
                    transport_id=active["transport_id"],
                    origin=active["origin"],
                    destination=active["destination"],
                    emit_requested=False,
                )
                if context is None:
                    raise RuntimeError("Unable to create Taxi metrics service context.")
                if not self.assign_service(self.agent.jid):
                    raise RuntimeError("Unable to emit Taxi service assignment.")
                logger.debug(
                    "Agent[{}]: The agent got accept from [{}]".format(
                        self.agent.name, content["customer_id"]
                    )
                )


                await self.inform_customer(
                    customer_id=content["customer_id"], status=TRANSPORT_MOVING_TO_CUSTOMER
                )

                await self.assigned_taxicustomer(
                    customer_id=content["customer_id"],
                    origin=content["origin"], dest=content["dest"]
                )

                (
                    distance,
                    osrm_duration,
                    speed_based_duration,
                ) = await self.agent.move_to(
                    content["origin"]
                )

                if not self.set_pending_movement("approach", distance):
                    logger.warning(
                        "Agent[{}]: Could not register Taxi approach movement.".format(
                            self.agent.name
                        )
                    )

                self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER
                self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
                return


            except PathRequestException:
                logger.error(
                    "Agent[{}]: The agent could not get a path to customer [{}]. Cancelling...".format(
                        self.agent.name, content["customer_id"]
                    )
                )

                self.fail_service("approach_route_failed")
                self.discard_pending_movement()
                await self.cancel_proposal(
                    content["customer_id"],
                    {
                        "terminal_status": "failed",
                        "failure_reason": "approach_route_failed",
                    },
                )
                self.clear_service_context()
                self.clear_active_message_context()
                self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING)
                return

            except AlreadyInDestination:

                self.set_pending_movement("approach", 0)
                self.complete_pending_movement()
                await self.inform_customer(
                    customer_id=content["customer_id"], status=TRANSPORT_IN_CUSTOMER_PLACE
                )
                self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
                self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
                return

            except Exception as e:
                logger.error(
                    "Unexpected error in agent [{}]: {}".format(
                        self.agent.name, e
                    )
                )

                self.fail_service("approach_unexpected_error")
                self.discard_pending_movement()
                await self.cancel_proposal(
                    content["customer_id"],
                    {
                        "terminal_status": "failed",
                        "failure_reason": "approach_unexpected_error",
                    },
                )
                self.clear_service_context()
                self.clear_active_message_context()
                self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING)
                return

        elif performative == REFUSE_PERFORMATIVE:
            if not self.message_matches_pending_offer(content):
                logger.warning(
                    "Agent[{}]: Ignoring stale or mismatched refusal.".format(
                        self.agent.name
                    )
                )
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
                return
            logger.debug(
                "Agent[{}]: The agent got refusal from customer/station".format(self.agent.name)
            )
            self.clear_pending_offer()
            self.set_next_state(TRANSPORT_WAITING)
            return

        else:
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return

class TaxiMovingToCustomerState(TaxiStrategyBehaviour):
    """
        Represents the state where the taxi is moving towards the customer to pick them up.

        Methods:
            on_start(): Logs the transition to 'Moving To Customer'.
            run(): Handles the movement to the customer and manages unexpected issues during the trip.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER
        logger.debug("{} in Transport Moving To Customer State".format(self.agent.jid))

    async def run(self):

        customers = self.get("assigned_customer")
        customer_id = next(iter(customers.items()))[0]

        try:

            if not self.agent.is_in_destination():

                msg = await self.receive(timeout=2)

                if msg:

                    performative = msg.get_metadata("performative")

                    if performative == REQUEST_PERFORMATIVE:
                        self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
                        return

                    elif performative == REFUSE_PERFORMATIVE:
                        try:
                            content = json.loads(msg.body)
                        except (json.JSONDecodeError, TypeError):
                            self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
                            return
                        if not self.message_matches_active_service(content):
                            logger.warning(
                                "Agent[{}]: Ignoring stale refusal while moving to customer.".format(
                                    self.agent.name
                                )
                            )
                            self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
                            return
                        logger.debug(
                            "Agent[{}]: The agent got refusal from customer/station".format(
                                self.agent.name
                            )
                        )

                        self.fail_service("customer_cancelled_before_pickup")
                        self.discard_pending_movement()
                        await self.cancel_proposal(
                            customer_id,
                            {
                                "terminal_status": "failed",
                                "failure_reason": "customer_cancelled_before_pickup",
                            },
                        )
                        self.clear_service_context()
                        self.clear_active_message_context()
                        self.agent.remove_assigned_customer()
                        self.agent.status = TRANSPORT_WAITING
                        self.agent.set_available()
                        self.set_next_state(TRANSPORT_WAITING)
                        return

                else:
                    self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
            else:
                logger.info(
                    "Agent[{}]: The agent has arrived to destination. Status: {}".format(
                        self.agent.agent_id, self.agent.status
                    )
                )
                self.complete_pending_movement()
                await self.inform_customer(
                    customer_id=customer_id, status=TRANSPORT_IN_CUSTOMER_PLACE
                )
                self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
                self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
                return

        except PathRequestException:
            logger.error(
                "Agent[{}]: The agent could not get a path to customer [{}]. Cancelling...".format(
                    self.agent.name, customer_id
                )
            )
            self.fail_service("approach_route_failed")
            self.discard_pending_movement()
            await self.cancel_proposal(
                customer_id,
                {
                    "terminal_status": "failed",
                    "failure_reason": "approach_route_failed",
                },
            )
            self.clear_service_context()
            self.clear_active_message_context()
            self.agent.remove_assigned_customer()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        except AlreadyInDestination:

            if not self.complete_pending_movement():
                self.set_pending_movement("approach", 0)
                self.complete_pending_movement()
            await self.inform_customer(
                customer_id=customer_id, status=TRANSPORT_IN_CUSTOMER_PLACE
            )
            self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
            return
        except Exception as e:
            logger.error(
                "Unexpected error in transport [{}]: {}".format(self.agent.name, e)
            )
            self.fail_service("approach_unexpected_error")
            self.discard_pending_movement()
            await self.cancel_proposal(
                customer_id,
                {
                    "terminal_status": "failed",
                    "failure_reason": "approach_unexpected_error",
                },
            )
            self.clear_service_context()
            self.clear_active_message_context()
            self.agent.remove_assigned_customer()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return


class TaxiArrivedAtCustomerState(TaxiStrategyBehaviour):
    """
        Represents the state where the taxi has arrived at the customer's location.

        Methods:
            on_start(): Logs the transition to 'Arrived At Customer'.
            run(): Handles the pickup of the customer and begins the journey to their destination.
        """

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER

    async def run(self):

        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
            return
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")

        if performative in (INFORM_PERFORMATIVE, CANCEL_PERFORMATIVE) and not self.message_matches_active_service(content):
            logger.warning(
                "Agent[{}]: Ignoring stale message at customer pickup.".format(
                    self.agent.name
                )
            )
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
            return

        if performative == INFORM_PERFORMATIVE:
            if "status" in content:
                status = content["status"]

                if status == CUSTOMER_IN_TRANSPORT:

                    customers = self.get("assigned_customer")
                    customer_id = next(iter(customers.items()))[0]
                    dest = next(iter(customers.items()))[1]["destination"]

                    try:
                        logger.debug(
                            "Agent[{}]: Customer [{}] in transport.".format(self.agent.name, customer_id)
                        )

                        self.agent.add_customer_in_transport(
                            customer_id=customer_id, dest=dest
                        )
                        if not self.start_service(self.agent.jid):
                            raise RuntimeError("Unable to emit Taxi service start.")


                        await self.unassigned_taxicustomer()

                        logger.info(
                            "Agent[{}]: The agent on route to [{}] destination.".format(self.agent.name, customer_id)
                        )


                        (
                            distance,
                            osrm_duration,
                            speed_based_duration,
                        ) = await self.agent.move_to(dest)

                        if not self.set_pending_movement("service", distance):
                            logger.warning(
                                "Agent[{}]: Could not register Taxi service movement.".format(
                                    self.agent.name
                                )
                            )

                        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
                        self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)

                    except PathRequestException:

                        self.fail_service("service_route_failed")
                        self.discard_pending_movement()
                        await self.cancel_customer(
                            customer_id=customer_id,
                            data={
                                "terminal_status": "failed",
                                "failure_reason": "service_route_failed",
                            },
                        )
                        self.clear_service_context()
                        self.clear_active_message_context()

                        if customer_id in self.agent.get("current_customer"):
                            self.agent.remove_customer_in_transport(customer_id)

                        self.agent.status = TRANSPORT_WAITING
                        self.agent.set_available()
                        self.set_next_state(TRANSPORT_WAITING)
                        return

                    except AlreadyInDestination:
                        self.set_pending_movement("service", 0)
                        self.complete_pending_movement()
                        await self.inform_customer(
                            customer_id=customer_id, status=CUSTOMER_IN_DEST
                        )
                        self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION
                        self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
                        return

                    except Exception as e:

                        logger.error(
                            "Unexpected error in transport [{}]: {}".format(
                                self.agent.name, e
                            )
                        )
                        self.fail_service("service_unexpected_error")
                        self.discard_pending_movement()
                        await self.cancel_customer(
                            customer_id=customer_id,
                            data={
                                "terminal_status": "failed",
                                "failure_reason": "service_unexpected_error",
                            },
                        )

                        if customer_id in self.agent.get("current_customer"):
                            self.agent.remove_customer_in_transport(customer_id)

                        self.clear_service_context()
                        self.clear_active_message_context()
                        self.agent.status = TRANSPORT_WAITING
                        self.agent.set_available()
                        self.set_next_state(TRANSPORT_WAITING)
                        return


        elif performative == CANCEL_PERFORMATIVE:
            self.fail_service("customer_cancelled_at_pickup")
            self.discard_pending_movement()
            self.clear_service_context()
            self.clear_active_message_context()

            if self.agent.get("assigned_customer"):
                self.agent.remove_assigned_customer()

            current_customers = self.agent.get("current_customer")

            for customer_id in list(current_customers.keys()):
                self.agent.remove_customer_in_transport(customer_id)

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)

            return
        else:
            self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
            return


class TaxiMovingToCustomerDestState(TaxiStrategyBehaviour):
    """
        Represents the state where the taxi is transporting the customer to their destination.

        Methods:
            on_start(): Logs the transition to 'Moving To Destination'.
            run(): Manages the trip to the customer's destination.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION


    async def run(self):

        customers = self.get("current_customer")
        customer_id = next(iter(customers.items()))[0]

        try:

            if not self.agent.is_in_destination():
                await asyncio.sleep(1)
                self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)
            else:
                logger.info(
                    "Agent[{}]: The agent has arrived to destination. Status: {}".format(
                        self.agent.agent_id, self.agent.status
                    )
                )


                self.complete_pending_movement()
                await self.inform_customer(
                    customer_id=customer_id, status=CUSTOMER_IN_DEST
                )
                self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION
                self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)

        except PathRequestException:
            logger.error(
                "Agent[{}]: The agent could not get a path to customer [{}]. Cancelling...".format(
                    self.agent.name, customer_id
                )
            )
            self.fail_service("service_route_failed")
            self.discard_pending_movement()
            await self.cancel_customer(
                customer_id=customer_id,
                data={
                    "terminal_status": "failed",
                    "failure_reason": "service_route_failed",
                },
            )
            self.clear_service_context()
            self.clear_active_message_context()

            if customer_id in self.agent.get("current_customer"):
                self.agent.remove_customer_in_transport(customer_id)

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        except AlreadyInDestination:

            if not self.complete_pending_movement():
                self.set_pending_movement("service", 0)
                self.complete_pending_movement()
            await self.inform_customer(
                customer_id=customer_id, status=CUSTOMER_IN_DEST
            )
            self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION
            self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
            return

        except Exception as e:
            logger.error(
                "Unexpected error in transport [{}]: {}".format(
                    self.agent.name, e
                )
            )

            self.fail_service("service_unexpected_error")
            self.discard_pending_movement()
            await self.cancel_customer(
                customer_id=customer_id,
                data={
                    "terminal_status": "failed",
                    "failure_reason": "service_unexpected_error",
                },
            )
            self.clear_service_context()
            self.clear_active_message_context()

            if customer_id in self.agent.get("current_customer"):
                self.agent.remove_customer_in_transport(customer_id)

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

class TaxiArrivedAtCustomerDestState(TaxiStrategyBehaviour):
    """
        Represents the state where the taxi has arrived at the customer's destination.

        Methods:
            on_start(): Logs the transition to 'Arrived At Destination'.
            run(): Handles the process of dropping the customer off and resets the taxi to 'Waiting' state.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION

    async def run(self):

        customers = self.get("current_customer")
        customer_id = next(iter(customers.items()))[0]

        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
            return
        else:
            content = json.loads(msg.body)
            performative = msg.get_metadata("performative")

            if performative in (INFORM_PERFORMATIVE, CANCEL_PERFORMATIVE) and not self.message_matches_active_service(content):
                logger.warning(
                    "Agent[{}]: Ignoring stale message at service completion.".format(
                        self.agent.name
                    )
                )
                self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
                return

            if performative == INFORM_PERFORMATIVE:
                if "status" in content:
                    status = content["status"]

                    if status == CUSTOMER_IN_DEST:

                        if not self.mark_service_completed():
                            logger.warning(
                                "Agent[{}]: Could not mirror completed Taxi service.".format(
                                    self.agent.name
                                )
                            )
                        self.agent.remove_customer_in_transport(customer_id)
                        self.clear_active_message_context()

                        self.agent.increment_completed_assignments()
                        self.agent.status = TRANSPORT_WAITING_FOR_RETURN
                        self.agent.set_busy()

                        logger.debug(
                            "Agent[{}]: The agent has completed the service for customer [{}].".format(
                                self.agent.agent_id, customer_id
                            )
                        )
                        self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
                        return


            elif performative == CANCEL_PERFORMATIVE:
                self.fail_service("customer_cancelled_at_destination")
                self.discard_pending_movement()
                self.clear_service_context()
                self.clear_active_message_context()

                if customer_id in self.agent.get("current_customer"):
                    self.agent.remove_customer_in_transport(customer_id)

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING)

                return
            else:
                self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
                return

class TaxiWaitingForReturnState(TaxiStrategyBehaviour):
    """
    Represents the state where the taxi waits for a return point
    assigned by the FleetManager.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_WAITING_FOR_RETURN
        self.return_requested = False

    async def run(self):

        if self.agent.get_return_position():
            self.set_next_state(TRANSPORT_MOVING_TO_RETURN)
            return

        if not self.return_requested:
            await self.request_return_position()
            self.return_requested = True

        msg = await self.receive(timeout=5)

        if not msg:
            self.return_requested = False
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        performative = msg.get_metadata("performative")

        if performative != INFORM_PERFORMATIVE:
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: Invalid return point response.".format(
                    self.agent.name
                )
            )

            self.return_requested = False
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        if content.get("request_type") != "taxi_return":
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        return_position = content.get("return_position")

        if return_position is None:
            self.return_requested = False
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        self.agent.set_return_position(
            return_position
        )

        logger.info(
            "Agent[{}]: Return position received: {}".format(
                self.agent.name,
                return_position
            )
        )

        try:
            (
                distance,
                osrm_duration,
                speed_based_duration,
            ) = await self.agent.move_to(
                return_position
            )
            if not self.set_pending_movement(
                "auxiliary", distance, require_service=False
            ):
                logger.warning(
                    "Agent[{}]: Could not register Taxi return movement.".format(
                        self.agent.name
                    )
                )

            self.agent.status = TRANSPORT_MOVING_TO_RETURN
            self.set_next_state(
                TRANSPORT_MOVING_TO_RETURN
            )
            return

        except AlreadyInDestination:
            logger.info(
                "Agent[{}]: The taxi is already at the return point {}.".format(
                    self.agent.name,
                    return_position
                )
            )

            self.set_pending_movement("auxiliary", 0, require_service=False)
            self.complete_pending_movement()
            self.clear_service_context()
            self.agent.clear_return_position()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        except PathRequestException:
            logger.error(
                "Agent[{}]: The taxi could not get a path to return point {}.".format(
                    self.agent.name,
                    return_position
                )
            )

            self.agent.clear_return_position()

            self.return_requested = False

            self.agent.status = TRANSPORT_WAITING_FOR_RETURN

            self.set_next_state(
                TRANSPORT_WAITING_FOR_RETURN
            )
            return

        except Exception as e:
            logger.error(
                "Unexpected error returning taxi [{}]: {}".format(
                    self.agent.name,
                    e
                )
            )

            self.agent.clear_return_position()
            self.return_requested = False
            self.agent.status = TRANSPORT_WAITING_FOR_RETURN

            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return


class TaxiMovingToReturnState(TaxiStrategyBehaviour):
    """
    Represents the state where the taxi moves to its assigned
    return point before becoming available again.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_MOVING_TO_RETURN

    async def run(self):

        return_position = self.agent.get_return_position()

        if return_position is None:
            logger.warning(
                "Agent[{}]: No return position available while returning.".format(
                    self.agent.name
                )
            )
            self.discard_pending_movement()

            self.agent.status = TRANSPORT_WAITING_FOR_RETURN

            self.set_next_state(
                TRANSPORT_WAITING_FOR_RETURN
            )
            return

        if not self.agent.is_in_destination():

            await asyncio.sleep(1)

            self.set_next_state(
                TRANSPORT_MOVING_TO_RETURN
            )
            return

        logger.info(
            "Agent[{}]: The taxi has arrived at return point {}.".format(
                self.agent.name,
                return_position
            )
        )

        self.complete_pending_movement()
        self.clear_service_context()
        self.agent.clear_return_position()

        self.agent.status = TRANSPORT_WAITING
        self.agent.set_available()

        self.set_next_state(
            TRANSPORT_WAITING
        )
        return


class FSMTaxiBehaviour(FSMSimfleetBehaviour):
    """
    Represents the Finite State Machine (FSM) strategy for the electric taxi agent.
    This class manages the different states and transitions for the taxi based on its behavior,
    including waiting for customers, moving to charging stations, and traveling to destinations.

    Methods:
        setup(): Initializes all states and defines transitions between them.
    """

    def setup(self):
        """
        Sets up the FSM by adding states and defining transitions.
        This method creates the states the electric taxi can be in and
        specifies the valid transitions between these states.
        """

        # Add states to the FSM
        self.add_state(TRANSPORT_WAITING, TaxiWaitingState(), initial=True)
        self.add_state(TRANSPORT_WAITING_FOR_APPROVAL, TaxiWaitingForApprovalState())
        self.add_state(TRANSPORT_MOVING_TO_CUSTOMER, TaxiMovingToCustomerState())
        self.add_state(TRANSPORT_ARRIVED_AT_CUSTOMER, TaxiArrivedAtCustomerState())
        self.add_state(TRANSPORT_MOVING_TO_DESTINATION, TaxiMovingToCustomerDestState())
        self.add_state(TRANSPORT_ARRIVED_AT_DESTINATION, TaxiArrivedAtCustomerDestState())
        self.add_state(TRANSPORT_WAITING_FOR_RETURN, TaxiWaitingForReturnState())
        self.add_state(TRANSPORT_MOVING_TO_RETURN, TaxiMovingToReturnState())

        # Define transitions between states

        # Transitions related to the 'Waiting' state
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING)  # Remains in waiting if no new action
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING_FOR_APPROVAL)  # When a customer accepts a proposal

        # Transitions from 'Waiting For Approval' state
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_WAITING_FOR_APPROVAL)  # Keep waiting for approval
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_WAITING)  # If the proposal is refused
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_MOVING_TO_CUSTOMER)  # If the customer accepts
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_ARRIVED_AT_CUSTOMER)  # Direct arrival scenario

        # Transitions from 'Moving To Customer' state
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_MOVING_TO_CUSTOMER)  # Still moving
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_WAITING)  # Encounter an issue, go back to waiting
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_ARRIVED_AT_CUSTOMER)  # Successfully arrive

        # Transitions from 'Arrived At Customer' state
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_ARRIVED_AT_CUSTOMER)  # Waiting at customer's location
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_MOVING_TO_DESTINATION)  # Begin journey to destination
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_ARRIVED_AT_DESTINATION)  # Direct destination arrival
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_WAITING)  # Cancel and return to waiting

        # Transitions from 'Moving To Destination' state
        self.add_transition(TRANSPORT_MOVING_TO_DESTINATION, TRANSPORT_MOVING_TO_DESTINATION)  # Still moving to destination
        self.add_transition(TRANSPORT_MOVING_TO_DESTINATION, TRANSPORT_WAITING)  # An issue encountered, return to waiting
        self.add_transition(TRANSPORT_MOVING_TO_DESTINATION, TRANSPORT_ARRIVED_AT_DESTINATION)  # Arrival at destination

        # Transitions from 'Arrived At Destination' state
        self.add_transition(TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_ARRIVED_AT_DESTINATION)  # Stay at destination
        self.add_transition(TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_WAITING)  # Drop customer and return to waiting
        self.add_transition(TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_MOVING_TO_RETURN)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_WAITING)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_MOVING_TO_RETURN)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_WAITING)


        # Additional transitions for customer movement and destination states
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_MOVING_TO_CUSTOMER)  # Still en route to customer
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_WAITING)  # Return to waiting if issue arises
