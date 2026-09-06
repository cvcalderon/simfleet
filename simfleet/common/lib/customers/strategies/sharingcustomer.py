import math
from uuid import uuid4
import json

from loguru import logger
from spade.message import Message
from spade.behaviour import State

from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from simfleet.communications.protocol import (
    INFORM_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    PROPOSE_PERFORMATIVE,
)
from simfleet.utils.status import (
    CUSTOMER_WAITING,
    CUSTOMER_WAITING_FOR_APPROVAL,
    CUSTOMER_MOVING_TO_TRANSPORT,
    CUSTOMER_IN_TRANSPORT,
    CUSTOMER_IN_DEST,
)
from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination,
)


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class SharingCustomerStrategyBehaviour(State):


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

    def reset_service_assignment(self):
        """Reset a refused pre-start candidate without creating a new service."""
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
        """Match a booking response to the open sharing request."""
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
        return self.agent.bare_jid(content.get("transport_id")) == self.agent.bare_jid(sender)

    def _service_message_content(self, content=None, transport_id=None):
        context = self.get_service_context()
        if context is None:
            raise ValueError("Cannot build a sharing service message without an open context.")
        result = dict(content or {})
        details = self._service_event_details(context)
        if transport_id is not None:
            details["transport_id"] = self.agent.bare_jid(transport_id)
        result.update(details)
        return result

    async def _fail_and_stop(self, failure_reason):
        """Emit the one public sharing failure and terminate this customer."""
        self.discard_pending_movement()
        self.fail_service(failure_reason)
        self.clear_service_context()
        self.agent.clear_pending_transport()
        self.agent.clear_sharing_transport()
        self.agent.clear_transport_candidates()
        await self.agent.stop()

    async def on_start(self):
        """
        Initializes the logger and timers. Call to parent method if overloaded.
        """
        logger.debug("Strategy {} started in customer {}".format(type(self).__name__, self.agent.name))

    async def go_to_transport(self):
        transport_id = (
            self.agent.get_sharing_transport_id()
        )

        transport_position = (
            self.agent.get_sharing_transport_position()
        )

        if (
            transport_id is None
            or transport_position is None
        ):
            raise RuntimeError(
                "No current sharing transport is available to walk to."
            )

        logger.info(
            "Customer [{}]: Walking to sharing transport [{}].".format(
                self.agent.name,
                transport_id
            )
        )

        return await self.agent.move_to(
            transport_position
        )

    async def request_transport_candidates(
        self,
        fleetmanager_id
    ):
        context = self.get_service_context()
        if context is None:
            raise ValueError("Cannot request sharing candidates without an open service context.")

        content = self._service_message_content(
            {
                "request_type": "sharing_candidates",
                "customer_id": str(self.agent.jid),
                "origin": self.agent.get_position(),
            }
        )

        if self.agent.max_walking_dist is not None:
            content["max_walking_distance"] = (
                self.agent.max_walking_dist
            )

        msg = Message()

        msg.to = str(fleetmanager_id)

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            REQUEST_PERFORMATIVE
        )

        msg.body = json.dumps(content)

        logger.debug(
            "Customer [{}]: Requesting sharing candidates "
            "from fleet manager [{}].".format(
                self.agent.name,
                fleetmanager_id
            )
        )

        await self.send(msg)

    def select_transport(self):
        candidates = (
            self.agent.get_transport_candidates()
        )

        if not candidates:
            return None

        return min(
            candidates,
            key=lambda candidate: candidate["distance"]
        )

    async def request_transport_booking(
        self,
        transport
    ):
        transport_id = transport.get("jid")

        if transport_id is None:
            logger.warning(
                "Customer [{}]: Cannot book transport "
                "without a jid.".format(
                    self.agent.name
                )
            )
            return

        context = self.get_service_context()
        if context is None:
            logger.warning(
                "Customer [{}]: Cannot book a sharing transport without an open service.".format(
                    self.agent.name
                )
            )
            return

        content = self._service_message_content(
            {
                "customer_id": str(self.agent.jid),
                "origin": context.get("origin"),
                "dest": self.agent.customer_dest,
            },
            transport_id=transport_id,
        )

        msg = Message()

        msg.to = str(transport_id)

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            PROPOSE_PERFORMATIVE
        )

        msg.body = json.dumps(content)

        logger.info(
            "Customer [{}]: Requesting booking "
            "of sharing transport [{}].".format(
                self.agent.name,
                transport_id
            )
        )

        await self.send(msg)


    async def cancel_transport_booking(
        self,
        transport_id
    ):
        if transport_id is None:
            return

        msg = Message()

        msg.to = str(transport_id)

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            CANCEL_PERFORMATIVE
        )

        content = self._service_message_content(
            {
                "customer_id": str(self.agent.jid),
                "terminal_status": "failed",
            },
            transport_id=transport_id,
        )
        context = self.get_service_context()
        if context is not None and context.get("failure_reason") is not None:
            content["failure_reason"] = context.get("failure_reason")
        msg.body = json.dumps(content)

        logger.info(
            "Customer [{}]: Cancelling booking "
            "with sharing transport [{}].".format(
                self.agent.name,
                transport_id
            )
        )

        await self.send(msg)

    async def inform_transport_arrival(self):
        transport_id = (
            self.agent.get_sharing_transport_id()
        )

        if transport_id is None:
            logger.warning(
                "Customer [{}]: Cannot inform arrival "
                "without a current transport.".format(
                    self.agent.name
                )
            )
            return

        msg = Message()

        msg.to = str(transport_id)

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            INFORM_PERFORMATIVE
        )

        msg.body = json.dumps(
            self._service_message_content(
                {
                    "customer_id": str(self.agent.jid),
                    "status": CUSTOMER_IN_TRANSPORT,
                },
                transport_id=transport_id,
            )
        )

        logger.info(
            "Customer [{}]: Arrived at sharing transport [{}].".format(
                self.agent.name,
                transport_id
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
#                       Customer Strategy                      #
#                                                              #
################################################################
class SharingCustomerWaitingState(SharingCustomerStrategyBehaviour):
    """
    Search for an available free-floating sharing vehicle.

    A logical service starts when the first candidate query is sent.  Temporary
    absence of a FleetManager is treated as discovery/bootstrap latency and
    does not create demand yet.  Once candidate search starts, exhausting the
    candidate set is a terminal public service failure.
    """

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_WAITING
        logger.debug(
            "Agent[{}]: The sharing customer is waiting.".format(
                self.agent.name
            )
        )

    async def run(self):
        # Fleet discovery is operational.  Do not create a user service until
        # there is a FleetManager to which a candidate request can be sent.
        if not self.agent.get_fleetmanagers():
            logger.info(
                "Agent[{}]: Looking for sharing fleet managers.".format(
                    self.agent.name
                )
            )
            fleetmanagers = await self.agent.get_list_agent_position(
                self.agent.fleet_type,
                self.agent.get_fleetmanagers(),
            )
            self.agent.set_fleetmanagers(fleetmanagers)
            if not fleetmanagers:
                await self.agent.sleep(5)
            self.set_next_state(CUSTOMER_WAITING)
            return

        # Request candidates only when the local retry set is empty.  The
        # service context survives refusals, so a retry never creates a second
        # service_id.
        if not self.agent.get_transport_candidates():
            context = self.get_or_create_service_context(
                user_id=self.agent.jid,
                origin=self.agent.get_position(),
                destination=self.agent.customer_dest,
                emit_requested=True,
            )
            if context is None:
                await self._fail_and_stop("service_context_error")
                return

            fleetmanagers = self.agent.get_fleetmanagers() or {}
            for fleetmanager_id in fleetmanagers.keys():
                await self.request_transport_candidates(fleetmanager_id)

            candidates = {}
            responses = 0

            while responses < len(fleetmanagers):
                msg = await self.receive(timeout=5)
                if not msg:
                    break

                protocol = msg.get_metadata("protocol")
                performative = msg.get_metadata("performative")
                if (
                    protocol != REQUEST_PROTOCOL
                    or performative != INFORM_PERFORMATIVE
                ):
                    continue
                try:
                    content = json.loads(msg.body)
                except (json.JSONDecodeError, TypeError):
                    logger.warning(
                        "Agent[{}]: Invalid sharing candidates response.".format(
                            self.agent.name
                        )
                    )
                    continue
                if content.get("request_type") != "sharing_candidates":
                    continue
                if not self.message_matches_service(content):
                    logger.warning(
                        "Agent[{}]: Ignoring stale sharing candidate response from [{}].".format(
                            self.agent.name,
                            msg.sender,
                        )
                    )
                    continue

                responses += 1
                vehicles = content.get("vehicles", [])
                if not isinstance(vehicles, list):
                    continue
                for vehicle in vehicles:
                    if not isinstance(vehicle, dict):
                        continue
                    vehicle_id = vehicle.get("jid")
                    position = vehicle.get("position")
                    if vehicle_id is None or position is None:
                        continue
                    candidates[str(vehicle_id)] = vehicle

            self.agent.set_transport_candidates(list(candidates.values()))
            if not self.agent.get_transport_candidates():
                logger.info(
                    "Agent[{}]: No sharing transports are available for the request.".format(
                        self.agent.name
                    )
                )
                await self._fail_and_stop("no_usable_candidate")
                return

        valid_candidates = []
        for candidate in self.agent.get_transport_candidates():
            position = candidate.get("position")
            if position is None:
                continue
            if self.agent.can_walk(position):
                valid_candidates.append(candidate)

        self.agent.set_transport_candidates(valid_candidates)
        if not valid_candidates:
            logger.info(
                "Agent[{}]: No sharing transport is reachable on foot.".format(
                    self.agent.name
                )
            )
            await self._fail_and_stop("no_usable_candidate")
            return

        selected_transport = self.select_transport()
        if selected_transport is None:
            await self._fail_and_stop("no_usable_candidate")
            return

        transport_id = selected_transport.get("jid")
        if transport_id is None:
            self.agent.remove_transport_candidate(transport_id)
            await self._fail_and_stop("no_usable_candidate")
            return

        logger.info(
            "Agent[{}]: Selected sharing transport [{}].".format(
                self.agent.name,
                transport_id,
            )
        )
        self.agent.set_pending_transport(selected_transport)
        await self.request_transport_booking(selected_transport)
        self.agent.status = CUSTOMER_WAITING_FOR_APPROVAL
        self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
        return


class SharingCustomerWaitingForApprovalState(
    SharingCustomerStrategyBehaviour
):
    """Wait for the selected free-floating vehicle booking response."""

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_WAITING_FOR_APPROVAL
        logger.debug(
            "Agent[{}]: Waiting for sharing transport booking approval.".format(
                self.agent.name
            )
        )

    async def run(self):
        pending_transport = self.agent.get_pending_transport()
        if pending_transport is None:
            logger.warning(
                "Agent[{}]: Waiting for approval without a pending transport.".format(
                    self.agent.name
                )
            )
            await self._fail_and_stop("missing_pending_candidate")
            return

        pending_transport_id = self.agent.get_pending_transport_id()
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
            return

        if msg.get_metadata("protocol") != REQUEST_PROTOCOL:
            self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
            return
        if self.agent.bare_jid(msg.sender) != self.agent.bare_jid(pending_transport_id):
            logger.debug(
                "Agent[{}]: Ignoring booking response from transport [{}].".format(
                    self.agent.name,
                    msg.sender,
                )
            )
            self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
            return

        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: Invalid booking response from transport [{}].".format(
                    self.agent.name,
                    pending_transport_id,
                )
            )
            self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
            return

        performative = msg.get_metadata("performative")
        if performative not in (ACCEPT_PERFORMATIVE, REFUSE_PERFORMATIVE):
            self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
            return

        if not (
            self._request_identity_matches(content)
            and self._message_sender_matches_transport(content, msg.sender)
            and self.agent.bare_jid(content.get("transport_id"))
            == self.agent.bare_jid(pending_transport_id)
        ):
            logger.warning(
                "Agent[{}]: Ignoring stale or malformed sharing booking response from [{}].".format(
                    self.agent.name,
                    msg.sender,
                )
            )
            self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
            return

        if performative == REFUSE_PERFORMATIVE:
            logger.info(
                "Agent[{}]: Sharing transport [{}] refused the booking.".format(
                    self.agent.name,
                    pending_transport_id,
                )
            )
            self.agent.remove_transport_candidate(pending_transport_id)
            self.agent.clear_pending_transport()
            if not self.agent.get_transport_candidates():
                await self._fail_and_stop("no_usable_candidate")
                return
            self.agent.status = CUSTOMER_WAITING
            self.set_next_state(CUSTOMER_WAITING)
            return

        # ACCEPT: the validated booking is the unique assignment milestone for
        # this free-floating service.
        if not self.assign_service(pending_transport_id):
            await self._fail_and_stop("assignment_state_error")
            return

        logger.info(
            "Agent[{}]: Sharing transport [{}] accepted the booking.".format(
                self.agent.name,
                pending_transport_id,
            )
        )
        transport = dict(pending_transport)
        position = content.get("position")
        if position is not None:
            transport["position"] = position
        self.agent.set_sharing_transport(transport)
        self.agent.clear_pending_transport()

        try:
            distance, _, _ = await self.go_to_transport()
            if not self.set_pending_movement(
                "approach",
                distance,
                extra_details={"movement_mode": "walking"},
            ):
                raise RuntimeError("Unable to register sharing approach movement.")

        except AlreadyInDestination:
            self.set_pending_movement(
                "approach",
                0,
                extra_details={"movement_mode": "walking"},
            )
            self.complete_pending_movement()
            self.agent.clear_transport_candidates()
            await self.inform_transport_arrival()
            self.agent.status = CUSTOMER_IN_TRANSPORT
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        except PathRequestException:
            reason = "approach_route_failed"
            self.fail_service(reason)
            await self.cancel_transport_booking(pending_transport_id)
            self.discard_pending_movement()
            self.clear_service_context()
            self.agent.clear_sharing_transport()
            self.agent.clear_transport_candidates()
            await self.agent.stop()
            return

        except Exception as e:
            logger.error(
                "Unexpected error in sharing customer [{}]: {}".format(
                    self.agent.name,
                    e,
                )
            )
            reason = "approach_unexpected_error"
            self.fail_service(reason)
            await self.cancel_transport_booking(pending_transport_id)
            self.discard_pending_movement()
            self.clear_service_context()
            self.agent.clear_sharing_transport()
            self.agent.clear_transport_candidates()
            await self.agent.stop()
            return

        self.agent.clear_transport_candidates()
        self.agent.status = CUSTOMER_MOVING_TO_TRANSPORT
        self.set_next_state(CUSTOMER_MOVING_TO_TRANSPORT)
        return


class SharingCustomerMovingToTransportState(
    SharingCustomerStrategyBehaviour
):
    """Customer walking toward the already reserved sharing vehicle."""

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_MOVING_TO_TRANSPORT
        logger.debug(
            "Agent[{}]: The sharing customer is moving to the reserved transport.".format(
                self.agent.name
            )
        )

    async def run(self):
        transport_id = self.agent.get_sharing_transport_id()
        if transport_id is None:
            await self._fail_and_stop("missing_assigned_transport")
            return

        if not self.agent.is_in_destination():
            self.set_next_state(CUSTOMER_MOVING_TO_TRANSPORT)
            await self.agent.sleep(1)
            return

        if not self.complete_pending_movement():
            logger.error(
                "Agent[{}]: Sharing approach arrival has no pending movement.".format(
                    self.agent.name
                )
            )
            self.fail_service("approach_movement_missing")
            await self.cancel_transport_booking(transport_id)
            self.clear_service_context()
            self.agent.clear_sharing_transport()
            await self.agent.stop()
            return

        logger.info(
            "Agent[{}]: Customer reached sharing transport [{}].".format(
                self.agent.name,
                transport_id,
            )
        )
        await self.inform_transport_arrival()
        self.agent.status = CUSTOMER_IN_TRANSPORT
        self.set_next_state(CUSTOMER_IN_TRANSPORT)
        return


class SharingCustomerInTransportState(
    SharingCustomerStrategyBehaviour
):
    """Customer using the reserved free-floating sharing vehicle."""

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_TRANSPORT
        logger.debug(
            "Agent[{}]: The sharing customer is in the transport.".format(
                self.agent.name
            )
        )

    async def run(self):
        transport_id = self.agent.get_sharing_transport_id()
        if transport_id is None:
            await self._fail_and_stop("missing_assigned_transport")
            return

        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        if msg.get_metadata("protocol") != REQUEST_PROTOCOL:
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return
        if self.agent.bare_jid(msg.sender) != self.agent.bare_jid(transport_id):
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: Invalid message received from sharing transport [{}].".format(
                    self.agent.name,
                    transport_id,
                )
            )
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        if not (
            self.message_matches_service(content)
            and self._message_sender_matches_transport(content, msg.sender)
        ):
            logger.warning(
                "Agent[{}]: Ignoring stale sharing service message from [{}].".format(
                    self.agent.name,
                    msg.sender,
                )
            )
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        performative = msg.get_metadata("performative")
        if performative == CANCEL_PERFORMATIVE:
            reason = content.get("failure_reason") or "transport_cancelled_service"
            self.fail_service(reason)
            self.clear_service_context()
            self.agent.clear_sharing_transport()
            self.agent.clear_transport_candidates()
            await self.agent.stop()
            return

        if performative != INFORM_PERFORMATIVE:
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        status = content.get("status")
        if status == CUSTOMER_IN_TRANSPORT:
            self.mark_service_started(transport_id)
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        if status == CUSTOMER_IN_DEST:
            # The zero-distance service path may jump directly to destination,
            # so mirror the transport's start before closing the user service.
            self.mark_service_started(transport_id)
            if not self.complete_service():
                await self._fail_and_stop("terminal_state_error")
                return
            logger.info(
                "Agent[{}]: Customer reached the destination using sharing transport [{}].".format(
                    self.agent.name,
                    transport_id,
                )
            )
            self.agent.clear_sharing_transport()
            self.clear_service_context()
            self.agent.status = CUSTOMER_IN_DEST
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        self.set_next_state(CUSTOMER_IN_TRANSPORT)
        return


class SharingCustomerInDestState(
    SharingCustomerStrategyBehaviour
):
    """Terminal success state for a free-floating sharing customer."""

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_DEST
        self.agent.clear_pending_transport()
        self.agent.clear_sharing_transport()
        self.agent.clear_transport_candidates()
        logger.debug(
            "Agent[{}]: The sharing customer is at the destination.".format(
                self.agent.name
            )
        )

    async def run(self):
        logger.info(
            "Customer {} has reached their destination.".format(
                self.agent.name
            )
        )
        return


class FSMSharingCustomerStrategyBehaviour(
    FSMSimfleetBehaviour
):
    """
    Finite State Machine behaviour for a free-floating
    sharing customer.
    """

    async def on_end(self):
        """
        Finalize the Sharing strategy and notify customer orchestration.
        """
        await super().on_end()
        self.agent.notify_modal_completion()

    def setup(self):

        # States
        self.add_state(
            CUSTOMER_WAITING,
            SharingCustomerWaitingState(),
            initial=True
        )

        self.add_state(
            CUSTOMER_WAITING_FOR_APPROVAL,
            SharingCustomerWaitingForApprovalState()
        )

        self.add_state(
            CUSTOMER_MOVING_TO_TRANSPORT,
            SharingCustomerMovingToTransportState()
        )

        self.add_state(
            CUSTOMER_IN_TRANSPORT,
            SharingCustomerInTransportState()
        )

        self.add_state(
            CUSTOMER_IN_DEST,
            SharingCustomerInDestState()
        )

        # Waiting
        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING_FOR_APPROVAL
        )

        # Waiting for booking approval
        self.add_transition(
            CUSTOMER_WAITING_FOR_APPROVAL,
            CUSTOMER_WAITING_FOR_APPROVAL
        )

        self.add_transition(
            CUSTOMER_WAITING_FOR_APPROVAL,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_WAITING_FOR_APPROVAL,
            CUSTOMER_MOVING_TO_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_WAITING_FOR_APPROVAL,
            CUSTOMER_IN_TRANSPORT
        )

        # Moving to reserved transport
        self.add_transition(
            CUSTOMER_MOVING_TO_TRANSPORT,
            CUSTOMER_MOVING_TO_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_TRANSPORT,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_TRANSPORT,
            CUSTOMER_IN_TRANSPORT
        )

        # In transport
        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_IN_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_IN_DEST
        )
