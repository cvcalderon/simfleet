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
    REQUEST_PERFORMATIVE,
    REQUEST_PROTOCOL,
)

from simfleet.utils.abstractstrategies import (
    FSMSimfleetBehaviour
)

from simfleet.utils.helpers import (
    AlreadyInDestination,
    PathRequestException,
    distance_in_meters,
)

from simfleet.utils.status import (
    CUSTOMER_WAITING,
    CUSTOMER_MOVING_TO_TRANSPORT,
    CUSTOMER_IN_STATION,
    CUSTOMER_IN_TRANSPORT,
    CUSTOMER_MOVING_TO_DEST,
    CUSTOMER_IN_DEST,
)


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class StationSharingCustomerStrategyBehaviour(State):


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

    def _service_message_content(self, content=None, transport_id=None):
        context = self.get_service_context()
        if context is None:
            raise ValueError(
                "Cannot build a station-sharing service message without an open context."
            )
        result = dict(content or {})
        details = self._service_event_details(context)
        if transport_id is not None:
            details["transport_id"] = self.agent.bare_jid(transport_id)
        result.update(details)
        return result

    def _message_sender_matches_transport(self, content, sender):
        if not isinstance(content, dict) or content.get("transport_id") is None:
            return False
        return self.agent.bare_jid(content.get("transport_id")) == self.agent.bare_jid(sender)

    def _set_generic_trip_failure(self, reason):
        self.agent.trip_failed = True
        self.agent.failure_operation = None
        self.agent.failure_reason = reason
        if hasattr(self.agent, "failure_station_id"):
            self.agent.failure_station_id = None

    def _set_station_trip_failure(self, operation, station, reason):
        if operation not in ("pick", "drop"):
            raise ValueError("Station-sharing failure operation must be pick or drop.")
        station_id = station.get("jid") if isinstance(station, dict) else station
        if station_id is None:
            self._set_generic_trip_failure(
                "{}_station_missing".format(operation)
            )
            return False
        if not isinstance(station, dict):
            station = {"jid": station_id}
        self.agent.set_trip_failure(
            operation=operation,
            reason=reason,
            station=station,
        )
        return True

    async def on_start(self):
        logger.debug(
            "Strategy {} started in station-sharing "
            "customer [{}]".format(
                type(self).__name__,
                self.agent.name
            )
        )

    async def request_station_candidates(self):

        fleetmanagers = (
            self.agent.get_fleetmanagers()
        )

        if not fleetmanagers:
            return

        context = self.get_service_context()
        if context is None:
            raise ValueError(
                "Cannot request station-sharing candidates without an open service context."
            )

        content = self._service_message_content(
            {
                "request_type": "station_sharing_candidates",
                "origin": self.agent.get_position(),
                "dest": self.agent.customer_dest,
            }
        )

        if self.agent.max_walking_dist is not None:
            content["max_walking_distance"] = (
                self.agent.max_walking_dist
            )

        for fleetmanager_id in fleetmanagers.keys():

            msg = Message()

            msg.to = str(
                fleetmanager_id
            )

            msg.set_metadata(
                "protocol",
                REQUEST_PROTOCOL
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
            "Agent[{}]: Requested station-sharing "
            "candidates.".format(
                self.agent.name
            )
        )

    def select_station_pair(self):
        """
        Default baseline policy.

        Origin:
            nearest feasible station to the customer.

        Destination:
            nearest feasible station to the final destination.

        This method is intentionally isolated so it can later be
        replaced by inference, LLM reasoning or another policy.
        """

        origin_candidates = (
            self.agent.get_origin_station_candidates()
        )

        destination_candidates = (
            self.agent.get_destination_station_candidates()
        )

        if (
            not origin_candidates
            or not destination_candidates
        ):
            return None, None

        origin_position = (
            self.agent.get_position()
        )

        destination_position = (
            self.agent.customer_dest
        )

        origin_candidates = [
            station
            for station in origin_candidates
            if station.get("available_bikes", 0) > 0
            and station.get("position") is not None
        ]

        destination_candidates = [
            station
            for station in destination_candidates
            if station.get("available_docks", 0) > 0
            and station.get("position") is not None
        ]

        if (
            not origin_candidates
            or not destination_candidates
        ):
            return None, None

        origin_station = min(
            origin_candidates,
            key=lambda station: distance_in_meters(
                origin_position,
                station["position"]
            )
        )

        destination_station = min(
            destination_candidates,
            key=lambda station: distance_in_meters(
                destination_position,
                station["position"]
            )
        )

        return (
            origin_station,
            destination_station
        )

    async def request_bike(self):

        station_id = (
            self.agent.get_origin_station_id()
        )

        if station_id is None:
            return

        content = self._service_message_content(
            {
                "service_name": self.agent.fleet_type,
                "object_type": "customer",
            }
        )

        msg = Message()

        msg.to = str(
            station_id
        )

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
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
            "Agent[{}]: Requested a bike from "
            "station [{}].".format(
                self.agent.name,
                station_id
            )
        )

    async def start_transport_trip(
        self,
        transport_id
    ):

        destination_station = (
            self.agent.get_destination_station()
        )

        origin_station = (
            self.agent.get_origin_station()
        )

        if (
            origin_station is None
            or destination_station is None
        ):
            return False

        content = self._service_message_content(
            {
                "customer_id": str(self.agent.jid),
                "origin": origin_station.get(
                    "position"
                ),
                "dest": destination_station.get(
                    "position"
                ),
                "destination_station": (
                    destination_station.get("jid")
                ),
            },
            transport_id=transport_id,
        )

        msg = Message()

        msg.to = str(
            transport_id
        )

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            PROPOSE_PERFORMATIVE
        )

        msg.body = json.dumps(
            content
        )

        await self.send(
            msg
        )

        logger.info(
            "Agent[{}]: Started trip with transport [{}] "
            "to station [{}].".format(
                self.agent.name,
                transport_id,
                destination_station.get("jid")
            )
        )

        return True


# ==================================================================
# -------------------------End Behaviour----------------------------
# ==================================================================



################################################################
#                                                              #
#                  Station Transport Strategy                  #
#                                                              #
################################################################

class StationSharingCustomerWaitingState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_WAITING
        logger.debug(
            "{} in Station Sharing Customer Waiting State".format(self.agent.jid)
        )

    async def run(self):
        if self.get_service_context() is None:
            context = self.create_service_context(
                user_id=self.agent.jid,
                origin=self.agent.get_position(),
                destination=self.agent.customer_dest,
                emit_requested=True,
            )
            if context is None:
                logger.error(
                    "Agent[{}]: Could not create station-sharing service context.".format(
                        self.agent.name
                    )
                )
                await self.agent.stop()
                return
            self.agent.clear_trip_failure()

        if not self.agent.get_fleetmanagers():
            fleetmanager_list = await self.agent.get_list_agent_position(
                self.agent.fleet_type,
                self.agent.get_fleetmanagers(),
            )
            self.agent.set_fleetmanagers(fleetmanager_list)
            if not fleetmanager_list:
                await self.agent.sleep(5)
            self.set_next_state(CUSTOMER_WAITING)
            return

        if (
            not self.agent.get_origin_station_candidates()
            or not self.agent.get_destination_station_candidates()
        ):
            await self.request_station_candidates()
            origin_stations = {}
            destination_stations = {}
            fleetmanagers = self.agent.get_fleetmanagers()
            responses = 0

            while responses < len(fleetmanagers):
                msg = await self.receive(timeout=5)
                if not msg:
                    break
                if msg.get_metadata("protocol") != REQUEST_PROTOCOL:
                    continue
                if msg.get_metadata("performative") != INFORM_PERFORMATIVE:
                    continue
                try:
                    content = json.loads(msg.body)
                except (json.JSONDecodeError, TypeError):
                    continue
                if content.get("request_type") != "station_sharing_candidates":
                    continue
                if not self.message_matches_service(content):
                    logger.warning(
                        "Agent[{}]: Ignoring stale station-sharing candidate response from [{}].".format(
                            self.agent.name,
                            msg.sender,
                        )
                    )
                    continue

                responses += 1
                for station in content.get("origin_stations", []):
                    station_jid = station.get("jid")
                    if station_jid is not None:
                        origin_stations[str(station_jid)] = station
                for station in content.get("destination_stations", []):
                    station_jid = station.get("jid")
                    if station_jid is not None:
                        destination_stations[str(station_jid)] = station

            self.agent.set_origin_station_candidates(list(origin_stations.values()))
            self.agent.set_destination_station_candidates(list(destination_stations.values()))

        origin_station, destination_station = self.select_station_pair()
        if origin_station is None or destination_station is None:
            logger.info(
                "Agent[{}]: No feasible station pair available.".format(self.agent.name)
            )
            self.agent.clear_station_candidates()
            await self.agent.sleep(5)
            self.set_next_state(CUSTOMER_WAITING)
            return

        self.agent.set_origin_station(origin_station)
        self.agent.set_destination_station(destination_station)
        station_position = origin_station.get("position")
        logger.info(
            "Agent[{}]: Selected origin station [{}] and destination station [{}].".format(
                self.agent.name,
                origin_station.get("jid"),
                destination_station.get("jid"),
            )
        )

        try:
            distance, _, _ = await self.agent.move_to(station_position)
            if not self.set_pending_movement(
                "approach",
                distance,
                extra_details={"movement_mode": "walking"},
            ):
                raise RuntimeError("Unable to register station-sharing approach movement.")

        except AlreadyInDestination:
            self.set_pending_movement(
                "approach",
                0,
                extra_details={"movement_mode": "walking"},
            )
            self.complete_pending_movement()
            await self.request_bike()
            self.agent.status = CUSTOMER_IN_STATION
            self.set_next_state(CUSTOMER_IN_STATION)
            return

        except PathRequestException:
            logger.error(
                "Agent[{}]: Could not calculate walking path to origin station [{}].".format(
                    self.agent.name,
                    origin_station.get("jid"),
                )
            )
            self.discard_pending_movement()
            self._set_generic_trip_failure("approach_route_failed")
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        except Exception as exc:
            logger.error(
                "Agent[{}]: Unexpected station-sharing approach error: {}".format(
                    self.agent.name,
                    exc,
                )
            )
            self.discard_pending_movement()
            self._set_generic_trip_failure("approach_unexpected_error")
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        self.agent.status = CUSTOMER_MOVING_TO_TRANSPORT
        self.set_next_state(CUSTOMER_MOVING_TO_TRANSPORT)


class StationSharingCustomerMovingToStationState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_MOVING_TO_TRANSPORT
        logger.debug("{} moving to sharing station".format(self.agent.jid))

    async def run(self):
        if not self.agent.is_in_destination():
            self.set_next_state(CUSTOMER_MOVING_TO_TRANSPORT)
            await self.agent.sleep(1)
            return

        if not self.complete_pending_movement():
            logger.error(
                "Agent[{}]: Arrival at origin station has no pending movement.".format(
                    self.agent.name
                )
            )
            self._set_generic_trip_failure("approach_movement_missing")
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        await self.request_bike()
        self.agent.status = CUSTOMER_IN_STATION
        self.set_next_state(CUSTOMER_IN_STATION)


class StationSharingCustomerInStationState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_STATION
        logger.debug("{} waiting for a bike in sharing station".format(self.agent.jid))

    async def run(self):
        station_id = self.agent.get_origin_station_id()
        if station_id is None:
            self._set_generic_trip_failure("origin_station_missing")
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(CUSTOMER_IN_STATION)
            return
        if msg.get_metadata("protocol") != REQUEST_PROTOCOL:
            self.set_next_state(CUSTOMER_IN_STATION)
            return
        if not self.agent.is_same_jid(msg.sender, station_id):
            self.set_next_state(CUSTOMER_IN_STATION)
            return

        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            # Queue admission is not the assignment milestone.  The actual
            # vehicle identifier arrives in the following INFORM.
            self.set_next_state(CUSTOMER_IN_STATION)
            return

        if performative == REFUSE_PERFORMATIVE:
            logger.warning(
                "Agent[{}]: Origin station [{}] could not supply a vehicle.".format(
                    self.agent.name,
                    station_id,
                )
            )
            self._set_station_trip_failure(
                "pick",
                self.agent.get_origin_station(),
                "no_bikes_available",
            )
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        if performative != INFORM_PERFORMATIVE:
            self.set_next_state(CUSTOMER_IN_STATION)
            return

        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self._set_station_trip_failure(
                "pick",
                self.agent.get_origin_station(),
                "invalid_pick_response",
            )
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        transport_id = content.get("transport_id")
        if transport_id is None:
            self._set_station_trip_failure(
                "pick",
                self.agent.get_origin_station(),
                "missing_transport_id",
            )
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        if not self.assign_service(transport_id):
            self._set_generic_trip_failure("assignment_state_error")
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        self.agent.set_station_sharing_transport_id(transport_id)
        started = await self.start_transport_trip(transport_id)
        if not started:
            self._set_generic_trip_failure("transport_trip_not_started")
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        self.agent.clear_station_candidates()
        self.agent.status = CUSTOMER_IN_TRANSPORT
        self.set_next_state(CUSTOMER_IN_TRANSPORT)


class StationSharingCustomerInTransportState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_TRANSPORT
        logger.debug("{} in station-sharing transport".format(self.agent.jid))

    async def run(self):
        transport_id = self.agent.get_station_sharing_transport_id()
        if transport_id is None:
            self._set_generic_trip_failure("transport_missing")
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return
        if msg.get_metadata("protocol") != REQUEST_PROTOCOL:
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return
        if not self.agent.is_same_jid(msg.sender, transport_id):
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            content = {}

        if not (
            self.message_matches_service(content)
            and self._message_sender_matches_transport(content, msg.sender)
        ):
            logger.warning(
                "Agent[{}]: Ignoring stale station-sharing transport message from [{}].".format(
                    self.agent.name,
                    msg.sender,
                )
            )
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        performative = msg.get_metadata("performative")
        if performative == REFUSE_PERFORMATIVE:
            reason = content.get("failure_reason") or content.get("reason")
            operation = content.get("failure_operation")
            if reason == "no_slots_available" or operation == "drop":
                station_id = (
                    content.get("station_id")
                    or content.get("station")
                    or self.agent.get_destination_station_id()
                )
                station = self.agent.get_destination_station() or {"jid": station_id}
                self._set_station_trip_failure(
                    "drop",
                    station,
                    "no_slots_available",
                )
            else:
                self._set_generic_trip_failure(reason or "transport_failure")

            self.agent.clear_station_sharing_transport_id()
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        if performative != INFORM_PERFORMATIVE:
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        status = content.get("status")
        if status == CUSTOMER_IN_TRANSPORT:
            self.mark_service_started(transport_id)
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        if status != CUSTOMER_IN_DEST:
            self.set_next_state(CUSTOMER_IN_TRANSPORT)
            return

        # Zero-distance rides can reach destination without an intermediate
        # CUSTOMER_IN_TRANSPORT notification, so mirror the start here too.
        if not self.mark_service_started(transport_id):
            self._set_generic_trip_failure("service_start_state_error")
            self.agent.clear_station_sharing_transport_id()
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        self.agent.clear_station_sharing_transport_id()
        logger.info(
            "Agent[{}]: Vehicle successfully registered in destination station [{}].".format(
                self.agent.name,
                self.agent.get_destination_station_id(),
            )
        )

        try:
            distance, _, _ = await self.agent.move_to(self.agent.customer_dest)
            if not self.set_pending_movement(
                "service",
                distance,
                extra_details={"movement_mode": "walking"},
            ):
                raise RuntimeError("Unable to register final station-sharing walk.")

        except AlreadyInDestination:
            self.set_pending_movement(
                "service",
                0,
                extra_details={"movement_mode": "walking"},
            )
            self.complete_pending_movement()
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        except PathRequestException:
            self.discard_pending_movement()
            self._set_generic_trip_failure("final_walk_route_failed")
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        except Exception as exc:
            logger.error(
                "Agent[{}]: Unexpected final station-sharing walk error: {}".format(
                    self.agent.name,
                    exc,
                )
            )
            self.discard_pending_movement()
            self._set_generic_trip_failure("final_walk_unexpected_error")
            self.set_next_state(CUSTOMER_IN_DEST)
            return

        self.agent.status = CUSTOMER_MOVING_TO_DEST
        self.set_next_state(CUSTOMER_MOVING_TO_DEST)


class StationSharingCustomerMovingToDestinationState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_MOVING_TO_DEST
        logger.debug("{} walking to final destination".format(self.agent.jid))

    async def run(self):
        if not self.agent.is_in_destination():
            self.set_next_state(CUSTOMER_MOVING_TO_DEST)
            await self.agent.sleep(1)
            return

        if not self.complete_pending_movement():
            self._set_generic_trip_failure("final_walk_movement_missing")
        self.set_next_state(CUSTOMER_IN_DEST)


class StationSharingCustomerInDestState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_DEST
        self.agent.clear_station_sharing_transport_id()
        self.agent.clear_station_candidates()

        context = self.get_service_context()
        if context is not None:
            self.discard_pending_movement()
            if self.agent.trip_failed:
                operation = self.agent.failure_operation
                if operation in ("pick", "drop"):
                    station_id = getattr(self.agent, "failure_station_id", None)
                    if station_id is None:
                        station_id = (
                            self.agent.get_origin_station_id()
                            if operation == "pick"
                            else self.agent.get_destination_station_id()
                        )
                    extra = {
                        "failure_operation": operation,
                        "station_id": self.agent.bare_jid(station_id),
                    }
                    self.fail_service(
                        "station_operation_failed",
                        extra_details=extra,
                    )
                else:
                    self.fail_service(
                        self.agent.failure_reason or "station_sharing_failed"
                    )

                logger.warning(
                    "Agent[{}]: Station-sharing trip failed. Operation: {}. Reason: {}.".format(
                        self.agent.name,
                        self.agent.failure_operation,
                        self.agent.failure_reason,
                    )
                )
            else:
                if not self.complete_service():
                    self.fail_service("terminal_state_error")
                logger.info(
                    "Agent[{}]: Station-sharing trip completed successfully.".format(
                        self.agent.name
                    )
                )

            self.clear_service_context()

    async def run(self):
        if self.agent.trip_failed:
            await self.agent.stop()
        return


class FSMStationSharingCustomerStrategyBehaviour(
    FSMSimfleetBehaviour
):

    async def on_end(self):
        """
        Finalize the StationSharing strategy and notify customer orchestration.
        """
        await super().on_end()
        self.agent.notify_modal_completion()

    def setup(self):

        self.add_state(
            CUSTOMER_WAITING,
            StationSharingCustomerWaitingState(),
            initial=True
        )

        self.add_state(
            CUSTOMER_MOVING_TO_TRANSPORT,
            StationSharingCustomerMovingToStationState()
        )

        self.add_state(
            CUSTOMER_IN_STATION,
            StationSharingCustomerInStationState()
        )

        self.add_state(
            CUSTOMER_IN_TRANSPORT,
            StationSharingCustomerInTransportState()
        )

        self.add_state(
            CUSTOMER_MOVING_TO_DEST,
            StationSharingCustomerMovingToDestinationState()
        )

        self.add_state(
            CUSTOMER_IN_DEST,
            StationSharingCustomerInDestState()
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_MOVING_TO_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_IN_STATION
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_IN_DEST
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_TRANSPORT,
            CUSTOMER_MOVING_TO_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_TRANSPORT,
            CUSTOMER_IN_STATION
        )

        self.add_transition(
            CUSTOMER_IN_STATION,
            CUSTOMER_IN_STATION
        )

        self.add_transition(
            CUSTOMER_IN_STATION,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_IN_STATION,
            CUSTOMER_IN_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_IN_STATION,
            CUSTOMER_IN_DEST
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_IN_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_MOVING_TO_DEST
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_IN_DEST
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_DEST,
            CUSTOMER_MOVING_TO_DEST
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_DEST,
            CUSTOMER_IN_DEST
        )
