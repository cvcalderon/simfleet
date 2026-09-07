import math
from uuid import uuid4
import json

from loguru import logger
from spade.message import Message
from spade.behaviour import State

from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_BOOKED, TRANSPORT_MOVING_TO_DESTINATION, \
    TRANSPORT_IN_DEST, TRANSPORT_IN_CUSTOMER_PLACE, CUSTOMER_IN_DEST, CUSTOMER_IN_TRANSPORT

from simfleet.communications.protocol import (
    PROPOSE_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    REQUEST_PROTOCOL,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination,
)


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class SharingStrategyBehaviour(State):


    METRICS_MODALITY = "sharing"
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
        """Mirror the successful terminal event emitted by the sharing customer."""
        context = self.get_service_context()
        if context is None:
            return False
        if context.get("terminal_status") is not None:
            return context.get("terminal_status") == "completed"
        if not context.get("started"):
            return False
        context["terminal_status"] = "completed"
        return True

    def mark_service_failed(self, failure_reason=None):
        """Mirror a terminal failure that is publicly emitted by the customer."""
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
        if failure_reason is not None:
            context["failure_reason"] = failure_reason
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

    def _request_identity_valid(self, content, sender=None):
        """Validate a schema-1.0 free-floating sharing booking request."""
        if not isinstance(content, dict):
            return False
        for key in ("service_id", "modality", "user_id", "transport_id"):
            if content.get(key) is None:
                return False
        if content.get("modality") != self.METRICS_MODALITY:
            return False
        if self.agent.bare_jid(content.get("transport_id")) != self.agent.bare_jid(self.agent.jid):
            return False
        if sender is not None and self.agent.bare_jid(content.get("user_id")) != self.agent.bare_jid(sender):
            return False
        customer_id = content.get("customer_id")
        if customer_id is not None and self.agent.bare_jid(customer_id) != self.agent.bare_jid(content.get("user_id")):
            return False
        return True

    def _message_sender_matches_user(self, content, sender):
        if not isinstance(content, dict) or content.get("user_id") is None:
            return False
        return self.agent.bare_jid(content.get("user_id")) == self.agent.bare_jid(sender)

    def _service_message_content(self, content=None):
        context = self.get_service_context()
        if context is None:
            raise ValueError("Cannot build a sharing service message without an open context.")
        result = dict(content or {})
        result.update(self._service_event_details(context))
        return result

    def _request_reply_content(self, request_content, content=None):
        """Build a refusal for a request that is not the active service context."""
        result = dict(content or {})
        if isinstance(request_content, dict):
            for key in ("service_id", "modality", "user_id", "transport_id"):
                if request_content.get(key) is not None:
                    result[key] = request_content[key]
        return result

    async def on_start(self):
        logger.debug(
            "Strategy {} started in transport {}".format(
                type(self).__name__,
                self.agent.name
            )
        )


    async def pick_up_customer(self, customer_id, origin, dest):

        self.agent.add_customer_in_transport(
            customer_id=customer_id, origin=origin, dest=dest
        )

        if not self.agent.is_customer_in_transport():
            try:
                # try to pick up the customer and move towards its destination
                self.set("customer_in_transport", customer_id)
                await self.agent.move_to(dest)
            except PathRequestException:
                # if there is no path to customer's destination, cancel it
                await self.cancel_customer()
                self.agent.status = TRANSPORT_WAITING
            else:
                await self.inform_customer(customer_id, TRANSPORT_IN_CUSTOMER_PLACE)
                self.agent.status = TRANSPORT_MOVING_TO_DESTINATION

                logger.info("Transport {} has picked up the customer {}.".format(
                    self.agent.agent_id, customer_id))

    async def accept_customer(self, customer_id):

        reply = Message()

        reply.to = str(customer_id)
        reply.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        reply.set_metadata(
            "performative",
            ACCEPT_PERFORMATIVE
        )

        reply.body = json.dumps(
            self._service_message_content(
                {
                    "transport_id": str(self.agent.jid),
                    "position": self.agent.get_position(),
                }
            )
        )

        await self.send(reply)

        logger.info(
            "Transport {} accepted booking from customer {}".format(
                self.agent.name,
                customer_id
            )
        )

    async def refuse_customer(self, customer_id, request_content=None):

        reply = Message()

        reply.to = str(customer_id)
        reply.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        reply.set_metadata(
            "performative",
            REFUSE_PERFORMATIVE
        )

        if request_content is None:
            payload = self._service_message_content(
                {
                    "transport_id": str(self.agent.jid),
                    "position": self.agent.get_position(),
                }
            )
        else:
            payload = self._request_reply_content(
                request_content,
                {
                    "transport_id": str(self.agent.jid),
                    "position": self.agent.get_position(),
                },
            )
        reply.body = json.dumps(payload)

        await self.send(reply)

        logger.info(
            "Transport {} refused booking from customer {}".format(
                self.agent.name,
                customer_id
            )
        )

    async def inform_customer(
        self,
        customer_id,
        status,
        data=None
    ):

        if data is None:
            data = {}

        msg = Message()

        msg.to = str(customer_id)
        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        msg.set_metadata(
            "performative",
            INFORM_PERFORMATIVE
        )

        data["status"] = status

        msg.body = json.dumps(
            self._service_message_content(data)
        )

        await self.send(msg)

    async def cancel_customer(
        self,
        customer_id,
        data=None
    ):

        if data is None:
            data = {}

        msg = Message()

        msg.to = str(customer_id)
        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        msg.set_metadata(
            "performative",
            CANCEL_PERFORMATIVE
        )

        msg.body = json.dumps(
            self._service_message_content(data)
        )

        await self.send(msg)

        logger.debug(
            "Agent[{}]: Cancelled booking with customer [{}].".format(
                self.agent.agent_id,
                customer_id
            )
        )
    async def deassign_customer(self):
        """
        Triggered when, by any reason, a customer cancels their already accepted booking
        """
        # Delete saved values (destination, etc.) belonging to booked customer
        self.agent.set("current_customer", None)
        self.agent.current_customer_orig = None
        self.agent.current_customer_dest = None

    async def run(self):
        raise NotImplementedError


# ==================================================================
# -------------------------End Behaviour----------------------------
# ==================================================================


################################################################
#                                                              #
#               Distributed Transport Strategy                 #
#                                                              #
################################################################
class SharingWaitingState(SharingStrategyBehaviour):
    """Available free-floating sharing vehicle waiting for a booking request."""

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING
        logger.debug(
            "Agent[{}]: The sharing transport is waiting.".format(
                self.agent.name
            )
        )

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING)
            return

        if (
            msg.get_metadata("protocol") != REQUEST_PROTOCOL
            or msg.get_metadata("performative") != PROPOSE_PERFORMATIVE
        ):
            self.set_next_state(TRANSPORT_WAITING)
            return

        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: Invalid sharing booking request.".format(
                    self.agent.name
                )
            )
            self.set_next_state(TRANSPORT_WAITING)
            return

        if not self._request_identity_valid(content, msg.sender):
            logger.warning(
                "Agent[{}]: Ignoring malformed or stale sharing booking request from [{}].".format(
                    self.agent.name,
                    msg.sender,
                )
            )
            self.set_next_state(TRANSPORT_WAITING)
            return

        customer_id = content.get("customer_id") or content.get("user_id")
        dest = content.get("dest")
        if customer_id is None or dest is None:
            self.set_next_state(TRANSPORT_WAITING)
            return

        context = self.get_or_create_service_context(
            service_id=content.get("service_id"),
            user_id=content.get("user_id"),
            transport_id=self.agent.jid,
            origin=content.get("origin"),
            destination=dest,
            emit_requested=False,
        )
        if context is None:
            self.set_next_state(TRANSPORT_WAITING)
            return

        # Assignment is publicly owned by the customer after it validates the
        # ACCEPT response.  The vehicle mirrors the accepted booking only.
        self.mark_service_assigned(self.agent.jid)
        origin = self.agent.get_position()
        self.agent.add_assigned_customer(
            customer_id=customer_id,
            origin=origin,
            dest=dest,
        )
        self.agent.set_busy()
        await self.accept_customer(customer_id)

        logger.info(
            "Agent[{}]: Sharing transport booked by customer [{}].".format(
                self.agent.name,
                customer_id,
            )
        )
        self.agent.status = TRANSPORT_BOOKED
        self.set_next_state(TRANSPORT_BOOKED)
        return


class SharingBookedState(SharingStrategyBehaviour):
    """Booked vehicle waiting for the assigned customer to reach it."""

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_BOOKED
        logger.debug(
            "Agent[{}]: The sharing transport is booked.".format(
                self.agent.name
            )
        )

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_BOOKED)
            return

        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: Invalid message received while sharing transport is booked.".format(
                    self.agent.name
                )
            )
            self.set_next_state(TRANSPORT_BOOKED)
            return

        performative = msg.get_metadata("performative")
        protocol = msg.get_metadata("protocol")
        if protocol != REQUEST_PROTOCOL:
            self.set_next_state(TRANSPORT_BOOKED)
            return

        customer_id = content.get("customer_id") or content.get("user_id")
        assigned_customers = self.agent.get("assigned_customer") or {}

        # A second customer may hold an outdated candidate list.  Refuse that
        # request with its own identifiers; never bind it to the active context.
        if performative == PROPOSE_PERFORMATIVE:
            if customer_id is not None and self._request_identity_valid(content, msg.sender):
                await self.refuse_customer(customer_id, request_content=content)
            self.set_next_state(TRANSPORT_BOOKED)
            return

        context = self.get_service_context()
        if not (
            context is not None
            and self.message_matches_service(content)
            and self._message_sender_matches_user(content, msg.sender)
            and customer_id is not None
            and str(customer_id) in assigned_customers
        ):
            logger.warning(
                "Agent[{}]: Ignoring stale sharing booking update from [{}].".format(
                    self.agent.name,
                    msg.sender,
                )
            )
            self.set_next_state(TRANSPORT_BOOKED)
            return

        if performative == CANCEL_PERFORMATIVE:
            reason = content.get("failure_reason") or "customer_cancelled_before_start"
            self.mark_service_failed(reason)
            self.discard_pending_movement()
            self.agent.remove_assigned_customer()
            self.clear_service_context()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        if performative != INFORM_PERFORMATIVE:
            self.set_next_state(TRANSPORT_BOOKED)
            return

        customer_data = assigned_customers[str(customer_id)]
        origin = customer_data.get("origin")
        dest = customer_data.get("destination")
        if dest is None:
            reason = "missing_service_destination"
            self.mark_service_failed(reason)
            await self.cancel_customer(
                customer_id,
                {"terminal_status": "failed", "failure_reason": reason},
            )
            self.agent.remove_assigned_customer()
            self.clear_service_context()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        # The user has physically reached the reserved vehicle.  This is the
        # real start of vehicle use, independent of whether the service leg has
        # positive or zero distance.
        if not self.start_service(self.agent.jid):
            self.set_next_state(TRANSPORT_BOOKED)
            return

        self.agent.add_customer_in_transport(
            customer_id=customer_id,
            origin=origin,
            dest=dest,
        )
        self.agent.remove_assigned_customer()

        try:
            distance, _, _ = await self.agent.move_to(dest)
            if not self.set_pending_movement("service", distance):
                raise RuntimeError("Unable to register sharing service movement.")

        except AlreadyInDestination:
            self.set_pending_movement("service", 0)
            self.complete_pending_movement()
            await self.inform_customer(
                customer_id=customer_id,
                status=CUSTOMER_IN_DEST,
            )
            self.mark_service_completed()
            self.agent.remove_customer_in_transport(customer_id)
            self.agent.increment_completed_assignments()
            self.clear_service_context()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        except PathRequestException:
            reason = "service_route_failed"
            self.mark_service_failed(reason)
            self.discard_pending_movement()
            await self.cancel_customer(
                customer_id,
                {"terminal_status": "failed", "failure_reason": reason},
            )
            self.agent.remove_customer_in_transport(customer_id)
            self.clear_service_context()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        except Exception as e:
            logger.error(
                "Unexpected error in sharing transport [{}]: {}".format(
                    self.agent.name,
                    e,
                )
            )
            reason = "service_unexpected_error"
            self.mark_service_failed(reason)
            self.discard_pending_movement()
            await self.cancel_customer(
                customer_id,
                {"terminal_status": "failed", "failure_reason": reason},
            )
            self.agent.remove_customer_in_transport(customer_id)
            self.clear_service_context()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        await self.inform_customer(
            customer_id=customer_id,
            status=CUSTOMER_IN_TRANSPORT,
        )
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
        self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)
        return


class SharingMovingToDestinationState(SharingStrategyBehaviour):
    """Sharing vehicle physically carrying the user to the final destination."""

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
        logger.debug(
            "Agent[{}]: The sharing transport is moving to the customer destination.".format(
                self.agent.name
            )
        )

    async def run(self):
        customers = self.agent.get("current_customer") or {}
        context = self.get_service_context()
        if not customers or context is None:
            logger.warning(
                "Agent[{}]: Sharing transport is moving without an active service.".format(
                    self.agent.name
                )
            )
            if context is not None:
                customer_id = context.get("user_id")
                reason = "missing_active_customer"
                self.mark_service_failed(reason)
                self.discard_pending_movement()
                if customer_id is not None:
                    await self.cancel_customer(
                        customer_id,
                        {"terminal_status": "failed", "failure_reason": reason},
                    )
                self.clear_service_context()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        customer_id = next(iter(customers.keys()))

        if not self.agent.is_in_destination():
            msg = await self.receive(timeout=1)
            if msg:
                try:
                    content = json.loads(msg.body)
                except (json.JSONDecodeError, TypeError):
                    content = {}
                performative = msg.get_metadata("performative")
                protocol = msg.get_metadata("protocol")

                if (
                    protocol == REQUEST_PROTOCOL
                    and performative == PROPOSE_PERFORMATIVE
                ):
                    new_customer_id = content.get("customer_id") or content.get("user_id")
                    if (
                        new_customer_id is not None
                        and self._request_identity_valid(content, msg.sender)
                    ):
                        await self.refuse_customer(
                            new_customer_id,
                            request_content=content,
                        )

                elif (
                    protocol == REQUEST_PROTOCOL
                    and performative == CANCEL_PERFORMATIVE
                    and self.message_matches_service(content)
                    and self._message_sender_matches_user(content, msg.sender)
                ):
                    reason = content.get("failure_reason") or "customer_cancelled_service"
                    self.mark_service_failed(reason)
                    self.discard_pending_movement()
                    self.agent.remove_customer_in_transport(customer_id)
                    self.clear_service_context()
                    self.agent.status = TRANSPORT_WAITING
                    self.agent.set_available()
                    self.set_next_state(TRANSPORT_WAITING)
                    return

            self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)
            return

        if not self.complete_pending_movement():
            reason = "service_movement_missing"
            self.mark_service_failed(reason)
            await self.cancel_customer(
                customer_id,
                {"terminal_status": "failed", "failure_reason": reason},
            )
            self.clear_service_context()
            self.agent.remove_customer_in_transport(customer_id)
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        logger.info(
            "Agent[{}]: The sharing transport reached the destination with customer [{}].".format(
                self.agent.name,
                customer_id,
            )
        )
        await self.inform_customer(
            customer_id=customer_id,
            status=CUSTOMER_IN_DEST,
        )
        self.mark_service_completed()
        self.agent.remove_customer_in_transport(customer_id)
        self.agent.increment_completed_assignments()
        self.clear_service_context()
        self.agent.status = TRANSPORT_WAITING
        self.agent.set_available()
        self.set_next_state(TRANSPORT_WAITING)
        return


class FSMSharingStrategyBehaviour(FSMSimfleetBehaviour):
    """
    Finite State Machine behaviour for a free-floating sharing transport.
    """

    def setup(self):

        # States
        self.add_state(
            TRANSPORT_WAITING,
            SharingWaitingState(),
            initial=True
        )

        self.add_state(
            TRANSPORT_BOOKED,
            SharingBookedState()
        )

        self.add_state(
            TRANSPORT_MOVING_TO_DESTINATION,
            SharingMovingToDestinationState()
        )

        # Waiting
        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_BOOKED
        )

        # Booked
        self.add_transition(
            TRANSPORT_BOOKED,
            TRANSPORT_BOOKED
        )

        self.add_transition(
            TRANSPORT_BOOKED,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_BOOKED,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        # Moving to destination
        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_WAITING
        )


