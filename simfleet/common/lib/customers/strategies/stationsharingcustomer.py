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
    """
    Base SPADE State shared by the StationSharing customer FSM.

    The customer owns the door-to-door lifecycle of one station-based shared
    mobility service identified by ``service_id``.

    The lifecycle coordinates three physical segments:

    - pedestrian approach from the customer origin to an origin station;
    - shared-vehicle movement between origin and destination stations;
    - pedestrian movement from the destination station to the customer's final
      destination.

    The customer creates ``service_id`` and owns public
    ``service_requested``, ``service_assigned``, ``service_completed``, and
    ``service_failed`` events. It also emits the pedestrian movement metrics.

    The selected StationSharing transport owns public ``service_started`` and
    the vehicle-service movement while mirroring assignment and terminal
    outcome locally.

    Station selection and station-operation failures are maintained separately
    from the canonical service context so pick and drop failures can be
    represented explicitly at terminal processing.

    This class is an FSM State helper. State registration and transitions
    belong to FSMStationSharingCustomerStrategyBehaviour.
    """

    METRICS_MODALITY = "station_sharing"
    _METRICS_SERVICE_CONTEXT_ATTR = "_metrics_service_context"
    _METRICS_PENDING_MOVEMENT_ATTR = "_metrics_pending_movement"
    _METRICS_MOVEMENT_PHASES = {"approach", "service", "auxiliary"}

    def _metrics_modality(self):
        """
        Return the canonical StationSharing modality.

        Returns:
            str: ``"station_sharing"``.

        Raises:
            ValueError: If a concrete strategy does not define
                ``METRICS_MODALITY``.
        """
        modality = self.METRICS_MODALITY
        if modality is None:
            raise ValueError(
                "A concrete metrics strategy must declare METRICS_MODALITY."
            )
        return modality

    def get_service_context(self):
        """
        Return the active canonical StationSharing service context.

        Returns:
            dict | None: Current schema-1.0 service context.
        """
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
        """
        Create and store one canonical StationSharing service context.

        The customer normally creates a new ``service_id`` and emits
        ``service_requested`` when its StationSharing FSM first enters service
        discovery.

        The context persists through FleetManager discovery, station selection,
        walking approach, vehicle assignment, station-to-station travel, and the
        final pedestrian leg.

        Args:
            service_id: Optional existing logical service identifier.
            user_id: StationSharing customer JID.
            transport_id: Optional assigned shared-vehicle JID.
            origin: Door-to-door customer origin.
            destination: Door-to-door final destination.
            emit_requested (bool): Whether to emit ``service_requested``.

        Returns:
            dict | None: Created or reusable service context.
        """
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
        """
        Return the matching StationSharing service context or create one.

        An existing service may be reused only when an explicitly supplied
        ``service_id`` matches the current logical request.

        Args:
            **kwargs: Arguments forwarded to ``create_service_context()``.

        Returns:
            dict | None: Matching or newly created context.
        """
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
        """
        Clear a terminal StationSharing service context when no movement remains.

        Unfinished services and contexts still owning pending movement are
        deliberately retained.

        Returns:
            bool: True when no service context remains.
        """
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
        """
        Build canonical identifiers shared by StationSharing service events.

        Returns:
            dict | None: Modality, service, user, and transport identifiers.
        """
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
        """
        Add canonical StationSharing identifiers to a copied outgoing payload.

        Args:
            content (dict | None): Existing payload.
            context (dict | None): Service context.
            transport_id: Optional assigned shared-vehicle identifier.

        Returns:
            dict: Payload extended with canonical identifiers.

        Raises:
            ValueError: If no service context is available.
        """
        context = context or self.get_service_context()
        if context is None:
            raise ValueError("Cannot propagate identifiers without a service context.")
        result = dict(content or {})
        if transport_id is not None:
            context["transport_id"] = self.agent.bare_jid(transport_id)
        result.update(self._service_event_details(context))
        return result

    def message_matches_service(self, content, context=None):
        """
        Validate that a message belongs to the active StationSharing service.

        Service ID, modality, user identity, and any already assigned transport
        identifier must match the local context.

        Returns:
            bool: True when the message belongs to the expected service.
        """
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
        """
        Mirror StationSharing assignment without emitting a public event.

        Returns:
            bool: True when the local assignment is valid.
        """
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
        """
        Mirror transport-owned StationSharing service start locally.

        The customer uses this after receiving service status from the assigned
        shared vehicle. No duplicate ``service_started`` event is emitted.

        Returns:
            bool: True when the local service can be marked started.
        """
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
        """
        Bind the vehicle supplied by the origin station and emit
        ``service_assigned`` exactly once.

        Assignment occurs only when the origin station reports the concrete
        transport identifier. Station queue admission alone is not an assignment
        milestone.

        Args:
            transport_id: Assigned StationSharing transport JID.
            extra_details (dict | None): Additional canonical fields.

        Returns:
            bool: True when assignment was newly emitted.
        """
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
        """
        Mark StationSharing vehicle use as started and emit ``service_started``.

        The helper remains available to custom strategies, although the current
        customer FSM does not normally own this public milestone. The selected
        transport emits ``service_started`` and the customer mirrors it through
        ``mark_service_started()``.

        Returns:
            bool: True when service start was newly emitted.
        """
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
        """
        Mark the complete door-to-door StationSharing service successful and emit
        ``service_completed``.

        Completion requires the service to have started and occurs only after all
        required post-vehicle pedestrian movement has also finished.

        Returns:
            bool: True when completion was newly emitted.
        """
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
        """
        Mark the StationSharing service failed and emit ``service_failed``.

        Terminal processing may include additional station-operation information,
        such as whether failure occurred while picking a vehicle or returning it
        to a destination station.

        Args:
            failure_reason (str | None): Canonical failure reason.
            extra_details (dict | None): Additional failure fields.

        Returns:
            bool: True when failure was newly emitted.
        """
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
        """
        Register one planned StationSharing customer movement for deferred metric
        emission.

        The current customer FSM uses:

        - ``phase="approach"`` with ``movement_mode="walking"`` for travel from
          the customer origin to the origin station;
        - ``phase="service"`` with ``movement_mode="walking"`` for the final walk
          from the destination station to the customer's door-to-door destination.

        The station-to-station vehicle movement is emitted by the assigned
        StationSharing transport.

        Returns:
            bool: True when the movement was registered.

        Raises:
            ValueError: If phase or distance violates the canonical movement
                contract.
        """
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
        """
        Emit the pending StationSharing customer movement as
        ``movement_completed``.

        Returns:
            bool: True when one pending movement existed and was emitted.
        """
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
        """
        Discard an incomplete StationSharing customer movement without emitting a
        metric.

        Returns:
            bool: True when one pending movement existed.
        """
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
        """
        Build a StationSharing message carrying canonical service identifiers.

        Before vehicle assignment, transport ID may remain unset. After the origin
        station supplies a concrete vehicle, ``transport_id`` correlates direct
        customer-to-transport messages with that same service.

        Args:
            content (dict | None): Additional payload fields.
            transport_id: Optional StationSharing transport JID.

        Returns:
            dict: Payload containing canonical service identifiers.

        Raises:
            ValueError: If no open service context exists.
        """
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
        """
        Verify that a StationSharing transport identifier matches the XMPP sender.

        Returns:
            bool: True when payload and sender identify the same bare JID.
        """
        if not isinstance(content, dict) or content.get("transport_id") is None:
            return False
        return self.agent.bare_jid(content.get("transport_id")) == self.agent.bare_jid(sender)

    def _set_generic_trip_failure(self, reason):
        """
        Record a non-station-specific failure for terminal StationSharing handling.

        The failure is stored on the customer agent and converted into the
        canonical ``service_failed`` event when CUSTOMER_IN_DEST performs terminal
        processing.

        Args:
            reason (str): Failure reason.
        """
        self.agent.trip_failed = True
        self.agent.failure_operation = None
        self.agent.failure_reason = reason
        if hasattr(self.agent, "failure_station_id"):
            self.agent.failure_station_id = None

    def _set_station_trip_failure(self, operation, station, reason):
        """
        Record a StationSharing pick or drop failure for terminal processing.

        Supported operations are:

        ``pick``
            Failure while obtaining a vehicle from the origin station.

        ``drop``
            Failure while registering the vehicle at the destination station.

        The selected station is retained so the terminal ``service_failed`` event
        can include ``failure_operation`` and ``station_id``.

        Args:
            operation (str): ``"pick"`` or ``"drop"``.
            station: Station descriptor or station JID.
            reason (str): Operational failure reason.

        Returns:
            bool: True when station-specific failure information was stored.

        Raises:
            ValueError: If operation is neither ``pick`` nor ``drop``.
        """
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
        """
        Request feasible origin and destination stations from all known
        StationSharing FleetManagers.

        The query carries the open ``service_id``, current customer position,
        final destination, and optional maximum walking distance.

        FleetManagers are expected to return separate origin-station and
        destination-station candidate collections.

        Raises:
            ValueError: If no StationSharing service context is open.
        """
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
        Select the baseline origin and destination StationSharing station pair.

        Origin candidates must expose a position and at least one available
        vehicle. Destination candidates must expose a position and at least one
        available dock.

        The baseline policy selects independently:

        - the feasible origin station nearest to the customer's current position;
        - the feasible destination station nearest to the customer's final
          destination.

        It does not jointly optimize the complete origin-destination pair.

        The method is intentionally isolated so alternative policies may replace
        this baseline without changing the surrounding FSM.

        Returns:
            tuple: ``(origin_station, destination_station)`` or ``(None, None)``
            when no feasible pair exists.
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
        """
        Request one shared vehicle from the selected origin station.

        The request is sent directly to the origin station through
        REQUEST_PROTOCOL / REQUEST_PERFORMATIVE and carries the open canonical
        service identifiers.

        Station admission or queue acceptance does not itself establish
        ``service_assigned``. Assignment occurs only when the station later reports
        the concrete transport identifier.
        """
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
        """
        Request the assigned StationSharing vehicle to begin the station-to-station
        service leg.

        The message supplies the customer identifier, origin-station position,
        destination-station position, destination-station JID, and canonical
        service identifiers.

        The current protocol represents this direct customer-to-vehicle trip
        request as REQUEST_PROTOCOL / PROPOSE_PERFORMATIVE.

        Args:
            transport_id: Vehicle JID supplied by the origin station.

        Returns:
            bool: True when the trip request was sent, or False when either
            selected station is missing.
        """
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
    """
    Discover a feasible StationSharing station pair and start pedestrian
    approach to the origin station.

    Entering this state creates the canonical StationSharing service context
    and emits ``service_requested`` before FleetManager discovery begins.

    FleetManagers provide origin-station and destination-station candidates.
    The customer selects an origin station with vehicle availability near its
    current position and a destination station with dock availability near its
    final destination.

    Temporary absence of FleetManagers or a feasible station pair is not
    terminal. The same ``service_id`` remains open while discovery is retried.

    Once a pair is selected, pedestrian travel to the origin station is
    registered as ``phase="approach"`` with
    ``movement_mode="walking"``.

    If the customer is already at the origin station, a zero-distance approach
    movement is emitted immediately and vehicle acquisition begins.

    Walking-route failures are stored as terminal trip failure and execution
    advances to ``CUSTOMER_IN_DEST`` for canonical failure processing.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_WAITING
        logger.debug(
            "{} in Station Sharing Customer Waiting State".format(self.agent.jid)
        )

    async def run(self):
        """
        Create or reuse the StationSharing service and resolve a feasible station
        pair.

        The canonical ``service_requested`` event is emitted before FleetManager
        discovery.

        When no FleetManager is available, discovery is retried after an
        operational delay without replacing the open service context.

        Station candidate responses are correlated with the active service and
        merged by station JID.

        If no feasible origin/destination pair can currently be selected,
        candidates are cleared and the same service retries from
        ``CUSTOMER_WAITING``.

        A feasible pair starts pedestrian movement to the origin station and
        registers pending ``phase="approach"`` /
        ``movement_mode="walking"`` movement.

        AlreadyInDestination completes a zero-distance approach and requests a
        vehicle immediately.

        Route or unexpected walking errors are recorded as trip failure and
        deferred to terminal processing in ``CUSTOMER_IN_DEST``.
        """
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
    """
    Monitor pedestrian movement from the customer origin to the selected
    StationSharing origin station.

    Physical walking is performed by the customer movement infrastructure.

    While movement remains incomplete, execution stays in
    ``CUSTOMER_MOVING_TO_TRANSPORT``.

    Confirmed arrival emits the pending ``phase="approach"`` walking movement,
    requests one shared vehicle from the origin station, and advances to
    ``CUSTOMER_IN_STATION``.

    Physical arrival without a canonical pending approach movement is recorded
    as terminal ``approach_movement_missing`` failure and deferred to
    ``CUSTOMER_IN_DEST``.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_MOVING_TO_TRANSPORT
        logger.debug("{} moving to sharing station".format(self.agent.jid))

    async def run(self):
        """
        Monitor walking approach until the origin station is reached.

        Incomplete physical movement remains in
        ``CUSTOMER_MOVING_TO_TRANSPORT`` after a one-second asynchronous wait.

        Arrival requires the pending canonical approach movement to exist and be
        emitted successfully.

        The customer then requests a shared vehicle and enters
        ``CUSTOMER_IN_STATION``.

        Missing movement correlation is recorded as terminal trip failure.
        """
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
    """
    Wait at the origin station until a concrete shared vehicle is supplied.

    Station admission and vehicle assignment are separate milestones.

    ACCEPT_PERFORMATIVE means only that the customer's vehicle request has
    been admitted or queued. It does not emit ``service_assigned``.

    REFUSE_PERFORMATIVE represents an origin-station ``pick`` failure because
    no usable vehicle can be supplied.

    INFORM_PERFORMATIVE must provide the concrete ``transport_id``. Only then
    does the customer emit ``service_assigned``, store the selected vehicle,
    and send that vehicle the direct station-to-station trip request.

    Successful assignment advances to ``CUSTOMER_IN_TRANSPORT``.

    Invalid or incomplete vehicle-supply responses are treated as terminal
    pick failures and processed later in ``CUSTOMER_IN_DEST``.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_STATION
        logger.debug("{} waiting for a bike in sharing station".format(self.agent.jid))

    async def run(self):
        """
        Process origin-station admission, refusal, and concrete vehicle supply.

        Missing origin-station identity is terminal.

        Timeouts, unrelated protocols, and messages from another sender keep the
        customer in ``CUSTOMER_IN_STATION``.

        Station ACCEPT confirms queue/service admission only.

        Station REFUSE records a ``pick`` failure with
        ``no_bikes_available``.

        A valid INFORM must contain ``transport_id``. The concrete vehicle is then
        assigned through ``assign_service()``, stored as the active
        StationSharing transport, and asked to start the station-to-station trip.

        Successful trip setup clears station candidates and enters
        ``CUSTOMER_IN_TRANSPORT``.

        Assignment or trip-start setup failures are deferred to terminal canonical
        processing.
        """
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
    """
    Track the assigned StationSharing vehicle until successful destination
    station registration or transport failure.

    Only messages from the concrete assigned vehicle and matching canonical
    service context are accepted.

    ``CUSTOMER_IN_TRANSPORT`` mirrors the transport-owned
    ``service_started`` milestone locally.

    REFUSE_PERFORMATIVE records either a destination-station ``drop`` failure
    or a generic transport failure.

    ``CUSTOMER_IN_DEST`` means that the vehicle has successfully reached and
    registered in the destination station. It does not yet complete the
    customer's door-to-door service.

    After vehicle success, the customer clears the active vehicle and starts
    the final pedestrian leg from the destination station to its final
    destination.

    Zero-distance vehicle services are supported by mirroring service start
    when ``CUSTOMER_IN_DEST`` arrives without a previous
    ``CUSTOMER_IN_TRANSPORT`` notification.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_TRANSPORT
        logger.debug("{} in station-sharing transport".format(self.agent.jid))

    async def run(self):
        """
        Process active messages from the assigned StationSharing transport.

        Missing assigned transport is terminal.

        Timeouts, unrelated protocols, wrong senders, malformed or stale service
        messages keep the customer in ``CUSTOMER_IN_TRANSPORT``.

        A transport REFUSE records either a destination-station ``drop`` failure
        or a generic transport failure and advances to terminal processing.

        INFORM with ``CUSTOMER_IN_TRANSPORT`` mirrors ``service_started``.

        INFORM with ``CUSTOMER_IN_DEST`` first guarantees that local service-start
        state exists, supporting zero-distance vehicle trips.

        Successful vehicle completion does not emit ``service_completed``.
        Instead, the assigned vehicle is cleared and pedestrian movement to the
        customer's final destination begins.

        That final walk is registered as ``phase="service"`` with
        ``movement_mode="walking"``.
        """
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
    """
    Monitor the final pedestrian segment from the destination station to the
    customer's door-to-door destination.

    The movement belongs to the active StationSharing service and is recorded
    as ``phase="service"`` with ``movement_mode="walking"``.

    While walking remains incomplete, execution stays in
    ``CUSTOMER_MOVING_TO_DEST``.

    Confirmed arrival emits the pending walking ``movement_completed`` event
    and advances to terminal ``CUSTOMER_IN_DEST``.

    Arrival without a corresponding pending movement records
    ``final_walk_movement_missing`` failure before entering terminal
    processing.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_MOVING_TO_DEST
        logger.debug("{} walking to final destination".format(self.agent.jid))

    async def run(self):
        """
        Monitor the final walking leg until the customer's destination is reached.

        Incomplete movement remains in ``CUSTOMER_MOVING_TO_DEST`` after a
        one-second asynchronous wait.

        Arrival attempts to emit the pending canonical movement.

        Missing movement correlation records ``final_walk_movement_missing``.

        Successful or failed final walking always proceeds to
        ``CUSTOMER_IN_DEST``, where the complete door-to-door service receives its
        canonical terminal event.
        """
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
    """
    Terminal door-to-door state of the StationSharing customer FSM.

    All residual transport and station-candidate state is cleared on entry.

    This state owns the final canonical outcome of the complete service.

    When ``trip_failed`` is set:

    - station-specific ``pick`` or ``drop`` failures emit
      ``service_failed`` with
      ``failure_reason="station_operation_failed"``,
      ``failure_operation``, and ``station_id``;
    - generic trip failures emit ``service_failed`` with their stored reason.

    When no failure exists, ``service_completed`` is emitted only after
    approach walking, vehicle use, destination-station registration, and any
    required final pedestrian movement have all completed.

    The canonical service context is cleared after terminal processing.

    Failed legacy customers stop their agent in ``run()``. Successful terminal
    execution returns normally so the FSM can finish and notify multimodal
    orchestration.
    """
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
        """
        Finish the terminal StationSharing state.

        Failed trips stop the customer agent after the canonical failure was
        emitted during ``on_start()``.

        Successful trips return normally, allowing the FSM to terminate and its
        ``on_end()`` hook to notify modal orchestration.
        """
        if self.agent.trip_failed:
            await self.agent.stop()
        return


class FSMStationSharingCustomerStrategyBehaviour(
    FSMSimfleetBehaviour
):
    """
    Finite-state customer strategy for station-based shared mobility.

    The FSM coordinates six phases:

    ``CUSTOMER_WAITING``
        Create the service, discover station candidates, select an origin and
        destination station, and begin walking to the origin station.

    ``CUSTOMER_MOVING_TO_TRANSPORT``
        Monitor the pedestrian approach to the origin station.

    ``CUSTOMER_IN_STATION``
        Request and obtain a concrete shared vehicle from the origin station.

    ``CUSTOMER_IN_TRANSPORT``
        Track the assigned vehicle until successful destination-station
        registration or transport failure.

    ``CUSTOMER_MOVING_TO_DEST``
        Monitor the final pedestrian leg from the destination station to the
        customer's final destination.

    ``CUSTOMER_IN_DEST``
        Emit the canonical door-to-door completion or failure.

    The customer owns public ``service_requested``, ``service_assigned``,
    ``service_completed``, ``service_failed``, and both pedestrian movement
    segments.

    The StationSharing transport owns public ``service_started`` and the
    station-to-station vehicle movement.

    Successful FSM termination invokes ``notify_modal_completion()`` so
    MultiModalCustomerAgent can advance to its next itinerary leg. Legacy
    customers retain the default no-op completion hook.

    Generic FSM lifecycle instrumentation is inherited from
    FSMSimfleetBehaviour.
    """
    async def on_end(self):
        """
        Finalize the StationSharing customer FSM and notify modal orchestration.

        Generic FSM end instrumentation executes first. The customer's modal
        completion hook is then called so a multimodal itinerary may observe
        termination of this StationSharing leg.
        """
        await super().on_end()
        self.agent.notify_modal_completion()

    def setup(self):
        """
        Register StationSharing customer states and permitted transitions.
        """
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
