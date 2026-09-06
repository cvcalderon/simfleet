import math
from uuid import uuid4
import json
from loguru import logger
from asyncio import CancelledError
from spade.message import Message

from simfleet.common.lib.customers.models.taxicustomer import TaxiCustomerStrategyBehaviour
from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    PROPOSE_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
)
from simfleet.utils.helpers import new_random_position
from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_IN_CUSTOMER_PLACE, CUSTOMER_WAITING, \
    CUSTOMER_IN_TRANSPORT, CUSTOMER_IN_DEST, CUSTOMER_ASSIGNED


################################################################
#                                                              #
#                       Customer Strategy                      #
#                                                              #
################################################################

class AcceptFirstRequestBehaviour(TaxiCustomerStrategyBehaviour):
    """
    A customer strategy behavior that accepts the first proposal it receives from a transport agent.
    It defines how the customer agent interacts with transport agents and handles different communication protocols.

    Inherits from:
        TaxiCustomerStrategyBehaviour: A base class for customer behaviors in the electric taxi fleet system.

    Methods:
        run(): The main coroutine responsible for the strategy's execution. It listens for messages
               and reacts based on the agent's current status and the message's performative.
    """


    METRICS_MODALITY = None
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

    def mark_service_failed(self, failure_reason=None):
        """Mirror a terminal failure emitted by the assigned transport."""
        context = self.get_service_context()
        if context is None:
            return False
        if context.get("terminal_status") is not None:
            return context.get("terminal_status") == "failed"
        context["terminal_status"] = "failed"
        if failure_reason is not None:
            context["failure_reason"] = failure_reason
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

    def reset_service_assignment(self):
        """Reset a pre-start assignment while keeping the same logical request."""
        context = self.get_service_context()
        if (
            context is None
            or context.get("terminal_status") is not None
            or context.get("started")
        ):
            return False
        context["transport_id"] = None
        context["assigned"] = False
        return True

    def _request_identity_matches(self, content):
        """Match a proposal to the open request without binding a transport yet."""
        context = self.get_service_context()
        if context is None or not isinstance(content, dict):
            return False
        for key in ("service_id", "modality", "user_id", "transport_id"):
            if content.get(key) is None:
                return False
        return (
            str(content["service_id"]) == context.get("service_id")
            and content["modality"] == context.get("modality")
            and self.agent.bare_jid(content["user_id"]) == context.get("user_id")
        )

    def _message_sender_matches_transport(self, content, sender):
        if not isinstance(content, dict) or content.get("transport_id") is None:
            return False
        return self.agent.bare_jid(content["transport_id"]) == self.agent.bare_jid(sender)

    def _service_message_content(self, content=None, transport_id=None):
        context = self.get_service_context()
        if context is None:
            raise ValueError("Cannot build a service message without an open context.")
        result = dict(content or {})
        details = self._service_event_details(context)
        if transport_id is not None:
            details["transport_id"] = self.agent.bare_jid(transport_id)
        result.update(details)
        return result

    async def send_service_request(self):
        """Send/retry the current logical request with the schema-1.0 identifiers."""
        if not self.agent.customer_dest:
            self.agent.customer_dest = new_random_position(
                self.agent.boundingbox,
                self.agent.route_host,
                self.route_profile,
            )

        origin = self.agent.get("current_pos")
        destination = self.agent.customer_dest
        context = self.get_or_create_service_context(
            user_id=self.agent.jid,
            origin=origin,
            destination=destination,
            emit_requested=True,
        )
        if context is None:
            return False

        content = self._service_message_content(
            {
                "customer_id": str(self.agent.jid),
                "origin": origin,
                "dest": destination,
            }
        )
        await super().send_request(content=content)
        return True

    async def accept_transport(self, transport_id):
        """Accept a proposal and propagate the canonical service identifiers."""
        reply = Message()
        reply.to = str(transport_id)
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", ACCEPT_PERFORMATIVE)
        content = self._service_message_content(
            {
                "customer_id": str(self.agent.jid),
                "origin": self.agent.get("current_pos"),
                "dest": self.agent.customer_dest,
            },
            transport_id=transport_id,
        )
        reply.body = json.dumps(content)
        await self.send(reply)
        self.agent.set_transport_assigned(str(transport_id))
        logger.info(
            "Agent[{}]: The agent accepted proposal from transport [{}]".format(
                self.agent.name, transport_id
            )
        )

    async def refuse_transport(self, transport_id):
        """Refuse one candidate without changing the selected transport context."""
        reply = Message()
        reply.to = str(transport_id)
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", REFUSE_PERFORMATIVE)
        content = self._service_message_content(
            {
                "customer_id": str(self.agent.jid),
                "origin": self.agent.get("current_pos"),
                "dest": self.agent.customer_dest,
            },
            transport_id=transport_id,
        )
        reply.body = json.dumps(content)
        await self.send(reply)
        logger.info(
            "Agent[{}]: The agent refused proposal from transport [{}]".format(
                self.agent.name, transport_id
            )
        )

    async def inform_transport(self, transport_id, status, data=None):
        """Inform the assigned transport while preserving service correlation."""
        reply = Message()
        reply.to = str(transport_id)
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", INFORM_PERFORMATIVE)
        payload = dict(data or {})
        payload["status"] = status
        payload = self._service_message_content(payload, transport_id=transport_id)
        reply.body = json.dumps(payload)
        await self.send(reply)
        if status != CUSTOMER_IN_DEST:
            self.agent.set_transport_assigned(str(transport_id))
        else:
            self.agent.clear_transport_assigned()
        logger.info(
            "Agent[{}]: The agent informs the transport [{}]".format(
                self.agent.name, transport_id
            )
        )

    async def run(self):
        """Run the strict schema-1.0 Taxi-like customer negotiation."""

        if self.agent.status is None:
            self.agent.status = CUSTOMER_WAITING
            return

        # fleet_type remains operational: it is used only to discover the fleet.
        if self.agent.get_fleetmanagers() is None:
            fleetmanager_list = await self.agent.get_list_agent_position(
                self.agent.fleet_type,
                self.agent.get_fleetmanagers(),
            )
            self.agent.set_fleetmanagers(fleetmanager_list)
            return

        if self.agent.status == CUSTOMER_WAITING:
            await self.send_service_request()

        try:
            msg = await self.receive(timeout=5)

            if not msg:
                return

            performative = msg.get_metadata("performative")
            transport_id = msg.sender
            try:
                content = json.loads(msg.body)
            except (json.JSONDecodeError, TypeError):
                logger.warning(
                    "Agent[{}]: Ignoring service message with invalid JSON.".format(
                        self.agent.name
                    )
                )
                return

            logger.debug(
                "Agent[{}]: The agent informed of: {}".format(
                    self.agent.name, content
                )
            )

            if performative == PROPOSE_PERFORMATIVE:
                if not (
                    self._request_identity_matches(content)
                    and self._message_sender_matches_transport(content, transport_id)
                ):
                    logger.warning(
                        "Agent[{}]: Ignoring stale or malformed proposal from [{}].".format(
                            self.agent.name, transport_id
                        )
                    )
                    return

                if self.agent.status == CUSTOMER_WAITING:
                    if not self.mark_service_assigned(transport_id):
                        return
                    logger.debug(
                        "Agent[{}]: The agent received proposal from transport [{}]".format(
                            self.agent.name, transport_id
                        )
                    )
                    await self.accept_transport(transport_id)
                    self.agent.status = CUSTOMER_ASSIGNED
                else:
                    await self.refuse_transport(transport_id)
                return

            if performative == CANCEL_PERFORMATIVE:
                context = self.get_service_context()
                if not (
                    context is not None
                    and context.get("assigned")
                    and self.message_matches_service(content)
                    and self._message_sender_matches_transport(content, transport_id)
                ):
                    logger.warning(
                        "Agent[{}]: Ignoring stale cancellation from [{}].".format(
                            self.agent.name, transport_id
                        )
                    )
                    return
                if self.agent.transport_assigned == str(transport_id):
                    logger.warning(
                        "Agent[{}]: The agent received a CANCEL from Transport [{}].".format(
                            self.agent.name, transport_id
                        )
                    )
                    if content.get("terminal_status") == "failed":
                        if self.mark_service_failed(content.get("failure_reason")):
                            self.agent.clear_transport_assigned()
                            self.clear_service_context()
                            # A later cycle may start a new logical request with a new service_id.
                            self.agent.status = CUSTOMER_WAITING
                    elif self.reset_service_assignment():
                        self.agent.clear_transport_assigned()
                        self.agent.status = CUSTOMER_WAITING
                return

            if performative != INFORM_PERFORMATIVE:
                return

            context = self.get_service_context()
            if not (
                context is not None
                and context.get("assigned")
                and self.message_matches_service(content)
                and self._message_sender_matches_transport(content, transport_id)
            ):
                logger.warning(
                    "Agent[{}]: Ignoring stale service update from [{}].".format(
                        self.agent.name, transport_id
                    )
                )
                return

            status = content.get("status")

            if status == TRANSPORT_MOVING_TO_CUSTOMER:
                logger.info(
                    "Agent[{}]: The agent waiting for transport.".format(
                        self.agent.name
                    )
                )
                return

            if status == TRANSPORT_IN_CUSTOMER_PLACE:
                if not self.mark_service_started(transport_id):
                    return
                self.agent.status = CUSTOMER_IN_TRANSPORT
                logger.info(
                    "Agent[{}]: The agent in transport.".format(self.agent.name)
                )
                await self.inform_transport(transport_id, CUSTOMER_IN_TRANSPORT)
                return

            if status == CUSTOMER_IN_DEST:
                self.agent.status = CUSTOMER_IN_DEST
                if not self.complete_service():
                    return
                await self.inform_transport(transport_id, CUSTOMER_IN_DEST)
                logger.info(
                    "Agent[{}]: The agent arrived to destination.".format(
                        self.agent.name
                    )
                )
                self.clear_service_context()
                if self.agent.should_stop_completed_modal_strategy():
                    self.kill()

                return

        except CancelledError:
            logger.debug("Cancelling async tasks...")

        except Exception as e:
            logger.error(
                "EXCEPTION in {} of agent [{}]: {}".format(
                    type(self).__name__, self.agent.name, e
                )
            )


class AcceptFirstTaxiRequestBehaviour(AcceptFirstRequestBehaviour):
    """Explicit Taxi specialization of the shared negotiation strategy."""

    METRICS_MODALITY = "taxi"


class AcceptFirstDeliveryRequestBehaviour(AcceptFirstRequestBehaviour):
    """Explicit Delivery specialization of the shared negotiation strategy."""

    METRICS_MODALITY = "delivery"


class AcceptFirstElectricTaxiRequestBehaviour(AcceptFirstRequestBehaviour):
    """Explicit Electric Taxi specialization of the shared negotiation strategy."""

    METRICS_MODALITY = "electric_taxi"
