import math
from uuid import uuid4
import json

from loguru import logger
from spade.behaviour import State
from spade.message import Message

from simfleet.communications.protocol import (
    ACCEPT_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    PROPOSE_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    REGISTER_PROTOCOL,
    REQUEST_PERFORMATIVE,
    REQUEST_PROTOCOL,
)

from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from simfleet.utils.helpers import (
    AlreadyInDestination,
    PathRequestException,
)

from simfleet.utils.status import (
    CUSTOMER_IN_DEST,
    CUSTOMER_IN_TRANSPORT,
    TRANSPORT_IN_DEST,
    TRANSPORT_MOVING_TO_DESTINATION,
    TRANSPORT_WAITING,
    TRANSPORT_WAITING_FOR_STATION_APPROVAL,
)


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================


class StationSharingStrategyBehaviour(State):


    METRICS_MODALITY = "station_sharing"
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

    def _request_identity_valid(self, content, sender=None):
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
            raise ValueError(
                "Cannot build a station-sharing service message without an open context."
            )
        result = dict(content or {})
        result.update(self._service_event_details(context))
        return result

    async def on_start(self):
        logger.debug(
            "Strategy {} started in station-sharing transport [{}]".format(
                type(self).__name__,
                self.agent.name
            )
        )

    async def inform_customer(
        self,
        customer_id,
        performative,
        content
    ):
        msg = Message()

        msg.to = str(
            customer_id
        )

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            performative
        )

        msg.body = json.dumps(
            self._service_message_content(content)
        )

        await self.send(
            msg
        )

    async def send_station_registration(
        self,
        station_id
    ):
        content = {
            "name": self.agent.name,
            "jid": str(self.agent.jid),
            "fleet_type": self.agent.fleet_type,
        }
        context = self.get_service_context()
        if context is not None:
            content.update(self._service_event_details(context))

        msg = Message()

        msg.to = str(
            station_id
        )

        msg.set_metadata(
            "protocol",
            REGISTER_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            REQUEST_PERFORMATIVE
        )

        msg.body = json.dumps(
            content
        )

        await self.send(
            msg
        )

        logger.debug(
            "Agent[{}]: Requested registration in station [{}].".format(
                self.agent.name,
                station_id
            )
        )

    async def run(self):
        raise NotImplementedError


# ==================================================================
# -------------------------End Behaviour----------------------------
# ==================================================================



################################################################
#                                                              #
#                  Station Transport Strategy                  #
#                                                              #
################################################################
class StationSharingWaitingState(
    StationSharingStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING
        logger.debug("{} in Station Sharing Waiting State".format(self.agent.jid))

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING)
            return
        if msg.get_metadata("protocol") != REQUEST_PROTOCOL:
            self.set_next_state(TRANSPORT_WAITING)
            return
        if msg.get_metadata("performative") != PROPOSE_PERFORMATIVE:
            self.set_next_state(TRANSPORT_WAITING)
            return

        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_WAITING)
            return

        if not self._request_identity_valid(content, msg.sender):
            logger.warning(
                "Agent[{}]: Ignoring malformed station-sharing trip request from [{}].".format(
                    self.agent.name,
                    msg.sender,
                )
            )
            self.set_next_state(TRANSPORT_WAITING)
            return

        customer_id = content.get("customer_id") or content.get("user_id")
        origin = content.get("origin")
        dest = content.get("dest")
        destination_station = content.get("destination_station")
        if customer_id is None or dest is None or destination_station is None:
            self.set_next_state(TRANSPORT_WAITING)
            return

        context = self.get_or_create_service_context(
            service_id=content.get("service_id"),
            user_id=content.get("user_id"),
            transport_id=self.agent.jid,
            origin=origin,
            destination=dest,
            emit_requested=False,
        )
        if context is None:
            self.set_next_state(TRANSPORT_WAITING)
            return

        # Assignment is publicly owned by the customer after the origin station
        # supplies this vehicle.  The transport mirrors it and owns service start.
        self.mark_service_assigned(self.agent.jid)
        if not self.start_service(self.agent.jid):
            self.mark_service_failed("service_start_state_error")
            self.clear_service_context()
            self.set_next_state(TRANSPORT_WAITING)
            return

        self.agent.set_origin_station(self.agent.get_registration_fleet())
        self.agent.set_destination_station(destination_station)
        self.agent.add_customer_in_transport(
            customer_id=customer_id,
            origin=origin,
            dest=dest,
        )
        self.agent.set_registration(False)

        try:
            distance, _, _ = await self.agent.move_to(dest)
            if not self.set_pending_movement("service", distance):
                raise RuntimeError("Unable to register station-sharing service movement.")

        except AlreadyInDestination:
            self.set_pending_movement("service", 0)
            self.complete_pending_movement()
            await self.send_station_registration(destination_station)
            self.agent.status = TRANSPORT_WAITING_FOR_STATION_APPROVAL
            self.set_next_state(TRANSPORT_WAITING_FOR_STATION_APPROVAL)
            return

        except PathRequestException:
            logger.error(
                "Agent[{}]: Could not calculate path to destination station [{}].".format(
                    self.agent.name,
                    destination_station,
                )
            )
            reason = "service_route_failed"
            self.mark_service_failed(reason)
            self.discard_pending_movement()
            await self.inform_customer(
                customer_id,
                REFUSE_PERFORMATIVE,
                {
                    "reason": "path_unavailable",
                    "failure_reason": reason,
                    "station": destination_station,
                },
            )
            #self.agent.remove_customer_in_transport(customer_id)
            #self.clear_service_context()
            #self.agent.status = TRANSPORT_IN_DEST
            #self.set_next_state(TRANSPORT_IN_DEST)

            origin_station = self.agent.get_origin_station()
            if origin_station is not None:
                # The bike never started the physical service movement.
                # Restore it to the station from which it was picked.
                self.agent.set_destination_station(origin_station)

                await self.send_station_registration(origin_station)

                self.agent.status = TRANSPORT_WAITING_FOR_STATION_APPROVAL
                self.set_next_state(TRANSPORT_WAITING_FOR_STATION_APPROVAL)
                return

            return

        except Exception as exc:
            logger.error(
                "Agent[{}]: Unexpected station-sharing service error: {}".format(
                    self.agent.name,
                    exc,
                )
            )
            reason = "service_unexpected_error"
            self.mark_service_failed(reason)
            self.discard_pending_movement()
            await self.inform_customer(
                customer_id,
                REFUSE_PERFORMATIVE,
                {"failure_reason": reason, "reason": reason},
            )
            self.agent.remove_customer_in_transport(customer_id)
            self.clear_service_context()
            self.agent.status = TRANSPORT_IN_DEST
            self.set_next_state(TRANSPORT_IN_DEST)
            return

        await self.inform_customer(
            customer_id,
            INFORM_PERFORMATIVE,
            {
                "status": CUSTOMER_IN_TRANSPORT,
                "transport_id": str(self.agent.jid),
            },
        )
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
        self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)


class StationSharingMovingToDestinationState(
    StationSharingStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
        logger.debug("{} moving to destination station".format(self.agent.jid))

    async def run(self):
        current_customers = self.agent.get("current_customer") or {}
        context = self.get_service_context()
        if not current_customers or context is None:
            if context is not None:
                self.mark_service_failed("missing_active_customer")
                self.discard_pending_movement()
                self.clear_service_context()
            self.agent.status = TRANSPORT_IN_DEST
            self.set_next_state(TRANSPORT_IN_DEST)
            return

        customer_id = next(iter(current_customers))
        if not self.agent.is_in_destination():
            self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)
            await self.agent.sleep(1)
            return

        if not self.complete_pending_movement():
            reason = "service_movement_missing"
            self.mark_service_failed(reason)
            await self.inform_customer(
                customer_id,
                REFUSE_PERFORMATIVE,
                {"failure_reason": reason, "reason": reason},
            )
            self.clear_service_context()
            self.agent.remove_customer_in_transport(customer_id)
            self.agent.status = TRANSPORT_IN_DEST
            self.set_next_state(TRANSPORT_IN_DEST)
            return

        destination_station = self.agent.get_destination_station()
        if destination_station is None:
            reason = "destination_station_missing"
            self.mark_service_failed(reason)
            await self.inform_customer(
                customer_id,
                REFUSE_PERFORMATIVE,
                {"failure_reason": reason, "reason": reason},
            )
            self.clear_service_context()
            self.agent.remove_customer_in_transport(customer_id)
            self.agent.status = TRANSPORT_IN_DEST
            self.set_next_state(TRANSPORT_IN_DEST)
            return

        await self.send_station_registration(destination_station)
        self.agent.status = TRANSPORT_WAITING_FOR_STATION_APPROVAL
        self.set_next_state(TRANSPORT_WAITING_FOR_STATION_APPROVAL)


class StationSharingWaitingForStationApprovalState(
    StationSharingStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING_FOR_STATION_APPROVAL
        logger.debug("{} waiting for destination station approval".format(self.agent.jid))

    async def run(self):

        context = self.get_service_context()
        recovering_failed_service = (
            context is not None
            and context.get("terminal_status") == "failed"
        )

        destination_station = self.agent.get_destination_station()
        if destination_station is None:
            self.agent.status = TRANSPORT_IN_DEST
            self.set_next_state(TRANSPORT_IN_DEST)
            return

        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING_FOR_STATION_APPROVAL)
            return
        if msg.get_metadata("protocol") != REGISTER_PROTOCOL:
            self.set_next_state(TRANSPORT_WAITING_FOR_STATION_APPROVAL)
            return
        if not self.agent.is_same_jid(msg.sender, destination_station):
            self.set_next_state(TRANSPORT_WAITING_FOR_STATION_APPROVAL)
            return

        current_customers = self.agent.get("current_customer") or {}
        if not current_customers:
            self.agent.status = TRANSPORT_IN_DEST
            self.set_next_state(TRANSPORT_IN_DEST)
            return
        customer_id = next(iter(current_customers))
        performative = msg.get_metadata("performative")

        if performative == ACCEPT_PERFORMATIVE:
            try:
                content = json.loads(msg.body)
            except (json.JSONDecodeError, TypeError):
                content = None

            self.agent.configure_registration(destination_station, False)
            self.agent.set_registration(True, content)

            if recovering_failed_service:
                self.agent.remove_customer_in_transport(customer_id)

                self.agent.clear_origin_station()
                self.agent.clear_destination_station()

                self.clear_service_context()

                self.agent.status = TRANSPORT_WAITING
                self.set_next_state(TRANSPORT_WAITING)

                logger.info(
                    "Agent[{}]: Restored to origin station [{}] "
                    "after failed service route.".format(
                        self.agent.name,
                        destination_station,
                    )
                )
                return

            await self.inform_customer(
                customer_id,
                INFORM_PERFORMATIVE,
                {
                    "status": CUSTOMER_IN_DEST,
                    "station": destination_station,
                    "station_id": destination_station,
                    "transport_id": str(self.agent.jid),
                },
            )

            self.mark_service_completed()
            self.agent.remove_customer_in_transport(customer_id)
            self.agent.increment_completed_assignments()
            self.agent.clear_origin_station()
            self.agent.clear_destination_station()
            self.clear_service_context()
            self.agent.status = TRANSPORT_WAITING
            self.set_next_state(TRANSPORT_WAITING)
            logger.info(
                "Agent[{}]: Registered successfully in destination station [{}].".format(
                    self.agent.name,
                    destination_station,
                )
            )
            return

        if performative == REFUSE_PERFORMATIVE:
            self.mark_service_failed("station_operation_failed")
            await self.inform_customer(
                customer_id,
                REFUSE_PERFORMATIVE,
                {
                    "reason": "no_slots_available",
                    "failure_reason": "station_operation_failed",
                    "failure_operation": "drop",
                    "station": destination_station,
                    "station_id": destination_station,
                    "transport_id": str(self.agent.jid),
                },
            )
            self.agent.remove_customer_in_transport(customer_id)
            self.clear_service_context()
            self.agent.status = TRANSPORT_IN_DEST
            self.set_next_state(TRANSPORT_IN_DEST)
            logger.warning(
                "Agent[{}]: Destination station [{}] is full.".format(
                    self.agent.name,
                    destination_station,
                )
            )
            return

        self.set_next_state(TRANSPORT_WAITING_FOR_STATION_APPROVAL)


class StationSharingInDestinationState(
    StationSharingStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_IN_DEST

        logger.warning(
            "Agent[{}]: Station-sharing transport "
            "finished with an unsuccessful trip.".format(
                self.agent.name
            )
        )

    async def run(self):

        await self.agent.stop()


class FSMStationSharingStrategyBehaviour(
    FSMSimfleetBehaviour
):

    def setup(self):

        self.add_state(
            TRANSPORT_WAITING,
            StationSharingWaitingState(),
            initial=True
        )

        self.add_state(
            TRANSPORT_MOVING_TO_DESTINATION,
            StationSharingMovingToDestinationState()
        )

        self.add_state(
            TRANSPORT_WAITING_FOR_STATION_APPROVAL,
            StationSharingWaitingForStationApprovalState()
        )

        self.add_state(
            TRANSPORT_IN_DEST,
            StationSharingInDestinationState()
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING_FOR_STATION_APPROVAL
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_IN_DEST
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_WAITING_FOR_STATION_APPROVAL
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_IN_DEST
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_STATION_APPROVAL,
            TRANSPORT_WAITING_FOR_STATION_APPROVAL
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_STATION_APPROVAL,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_STATION_APPROVAL,
            TRANSPORT_IN_DEST
        )
