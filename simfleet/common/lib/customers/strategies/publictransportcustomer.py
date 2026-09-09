import math
from uuid import uuid4
import json

from loguru import logger
from spade.behaviour import State
from spade.message import Message

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
)

from simfleet.utils.status import (
    CUSTOMER_WAITING,
    CUSTOMER_WAITING_TO_MOVE,
    CUSTOMER_WAITING_FOR_JOURNEY,
    CUSTOMER_MOVING_TO_DEST,
    CUSTOMER_IN_STOP,
    CUSTOMER_WAITING_FOR_APPROVAL,
    CUSTOMER_IN_TRANSPORT,
    CUSTOMER_IN_DEST,
    CUSTOMER_JOURNEY_FAILED,
)

from simfleet.utils.helpers import (
    AlreadyInDestination,
    PathRequestException,
)

from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class PublicTransportCustomerStrategyBehaviour(
    State
):
    """
    Base SPADE State shared by the PublicTransport customer FSM.

    The customer owns one door-to-door PublicTransport service identified by a
    persistent ``service_id``. A selected journey may contain multiple walking
    and public-transport legs and may therefore use several different vehicles
    without creating additional logical services.

    The customer owns public ``service_requested``, ``service_started``,
    ``service_completed``, and ``service_failed`` events together with its
    pedestrian movement metrics.

    Each PublicTransport vehicle owns the public ``service_assigned`` event for
    the boarding it accepts. The customer mirrors those assignments locally
    through boarding-specific keys so several transfers can remain correlated
    with the same ``service_id``.

    The active service context tracks the current candidate or boarded vehicle,
    the unique vehicles used by the journey, boarding keys, lifecycle state,
    and pending customer movement.

    Journey discovery, stop-queue interaction, boarding requests, and canonical
    service correlation helpers are centralized here. Concrete state
    transitions belong to FSMPublicTransportCustomerStrategyBehaviour.
    """


    METRICS_MODALITY = "public_transport"
    _METRICS_SERVICE_CONTEXT_ATTR = "_metrics_service_context"
    _METRICS_PENDING_MOVEMENT_ATTR = "_metrics_pending_movement"
    _METRICS_MOVEMENT_PHASES = {"approach", "service", "auxiliary"}

    def _metrics_modality(self):
        """
        Return the canonical PublicTransport metrics modality.

        Returns:
            str: ``"public_transport"``.

        Raises:
            ValueError: If a concrete metrics strategy does not define a modality.
        """
        modality = self.METRICS_MODALITY
        if modality is None:
            raise ValueError(
                "A concrete metrics strategy must declare METRICS_MODALITY."
            )
        return modality

    def get_service_context(self):
        """
        Return the active door-to-door PublicTransport service context.

        Returns:
            dict | None: Current canonical service context.
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
        Create and store one canonical PublicTransport door-to-door service.

        A customer normally creates the ``service_id`` when journey requests are
        first sent and emits ``service_requested`` at that point.

        The same context persists across the complete selected journey, including
        walking access, transfers, multiple PublicTransport boardings, and final
        walking.

        ``transport_id`` represents the currently selected or active vehicle.
        ``transport_ids`` retains the unique vehicles used across boardings and
        ``assignment_keys`` prevents the same boarding from being mirrored twice.

        Args:
            service_id: Optional existing logical service identifier.
            user_id: PublicTransport customer JID.
            transport_id: Optional current vehicle JID.
            origin: Door-to-door journey origin.
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
            "transport_ids": [],
            "assignment_keys": set(),
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
        Return the matching PublicTransport service context or create one.

        The same context is reused throughout all legs and transfers of the
        journey. An explicitly supplied different ``service_id`` is rejected.

        Args:
            **kwargs: Arguments forwarded to ``create_service_context()``.

        Returns:
            dict | None: Matching or newly created service context.
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
        Clear a terminal PublicTransport service when no customer movement remains.

        An unfinished service or one still carrying pending movement is
        deliberately retained.

        Returns:
            bool: True when no active service context remains.
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
        Build canonical identifiers for PublicTransport customer service events.

        ``transport_id`` represents the vehicle currently correlated with the
        service and may change between PublicTransport legs.

        Returns:
            dict | None: Modality, service, user, and current transport
            identifiers.
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
        Add canonical PublicTransport identifiers to a copied outgoing payload.

        When ``transport_id`` is supplied, that vehicle becomes the current
        candidate or active transport in the service context. A later boarding
        refusal may clear that candidate without destroying the complete journey
        service.

        Args:
            content (dict | None): Existing payload.
            context (dict | None): Service context.
            transport_id: Optional current PublicTransport vehicle JID.

        Returns:
            dict: Payload extended with canonical service identifiers.

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
        Validate that a message belongs to the active PublicTransport service.

        Service ID, modality, and user ID must match the customer context.

        When a current candidate or boarded transport is established, the incoming
        ``transport_id`` must also identify that same vehicle.

        Returns:
            bool: True when the message belongs to the expected service and
            transport context.
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

    def mark_service_assigned(
        self,
        transport_id,
        boarding_key=None,
    ):
        """
        Mirror one PublicTransport boarding assignment without emitting an event.

        A single logical service may board several vehicles during transfers.

        ``boarding_key`` identifies one journey boarding independently from the
        vehicle JID. Repeated processing of the same key is ignored, while a new
        boarding may append another vehicle to ``transport_ids`` and update the
        current ``transport_id``.

        The public ``service_assigned`` event is emitted by the vehicle that
        accepts that boarding.

        Args:
            transport_id: Accepted PublicTransport vehicle JID.
            boarding_key: Optional boarding-specific correlation key.

        Returns:
            bool: True when a new local boarding assignment was recorded.
        """
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False

        transport_id = self.agent.bare_jid(
            transport_id
        )
        if transport_id is None:
            return False

        key = str(
            boarding_key
            if boarding_key is not None
            else transport_id
        )

        if key in context["assignment_keys"]:
            context["transport_id"] = transport_id
            return False

        context["assignment_keys"].add(
            key
        )

        if transport_id not in context["transport_ids"]:
            context["transport_ids"].append(
                transport_id
            )

        context["transport_id"] = transport_id
        context["assigned"] = True
        return True

    def clear_candidate_transport(self, transport_id=None):
        """
        Clear the current PublicTransport boarding candidate without terminating
        the complete service.

        This is used after boarding refusal or invalid candidate state so the
        customer may continue waiting for another compatible vehicle on the same
        journey leg.

        Historical ``transport_ids`` and boarding assignments remain untouched.

        Args:
            transport_id: Optional expected candidate vehicle JID.

        Returns:
            bool: True when the current candidate transport was cleared.
        """
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        if transport_id is not None:
            transport_id = self.agent.bare_jid(transport_id)
            if context.get("transport_id") != transport_id:
                return False
        context["transport_id"] = None
        return True

    def mark_service_started(self, transport_id=None):
        """
        Mirror PublicTransport service-start state without emitting an event.

        The helper remains available for custom strategies. The current
        PublicTransport customer FSM normally owns the public start milestone
        through ``start_service()`` after its first accepted boarding.

        Args:
            transport_id: Optional active PublicTransport vehicle JID.

        Returns:
            bool: True when local start state can be established.
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
        Emit a single customer-owned ``service_assigned`` event.

        This generic helper is retained for compatibility or custom strategies.

        The current PublicTransport customer FSM does not normally use it because
        each accepted boarding is publicly assigned by the corresponding vehicle
        and mirrored locally through ``mark_service_assigned()``.

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
        Mark the door-to-door PublicTransport service started and emit
        ``service_started`` exactly once.

        The current customer FSM calls this after an accepted boarding.

        For journeys containing transfers, later accepted boardings call the same
        helper again but no additional ``service_started`` event is emitted because
        the service is already marked started.

        Args:
            transport_id: Vehicle associated with the accepted boarding.
            extra_details (dict | None): Pattern, route, mode, stop, and leg
                metadata.

        Returns:
            bool: True only when the service start was newly emitted.
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
        Emit the successful terminal event for the complete PublicTransport
        door-to-door journey.

        Completion is allowed only after the service has started.

        The current customer FSM emits this after every selected journey leg,
        including any final walking leg, has been completed.

        Args:
            extra_details (dict | None): Optional final journey summary fields.

        Returns:
            bool: True when ``service_completed`` was newly emitted.
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
        Mark the complete PublicTransport journey failed and emit
        ``service_failed``.

        The canonical service remains singular even when the failure occurs after
        one or more successful boardings or transfers.

        Args:
            failure_reason (str | None): Canonical terminal failure reason.
            extra_details (dict | None): Additional diagnostic fields.

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
        Register one PublicTransport customer movement for deferred metric
        emission.

        Customer-side movement represents walking journey legs.

        Before the first successful boarding, walking is recorded as
        ``phase="approach"``.

        Once ``service_started`` has been emitted, subsequent transfer and final
        walking legs are recorded as ``phase="service"``.

        Walking movements additionally use ``movement_mode="walking"`` and normally
        set ``transport_id=None``.

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
        Emit the pending PublicTransport customer movement as
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
        Discard an incomplete PublicTransport customer movement without emitting a
        movement metric.

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

    async def on_start(
        self
    ):
        """
        Log entry into one concrete PublicTransport customer FSM state.

        Generic FSM lifecycle instrumentation belongs to
        FSMPublicTransportCustomerStrategyBehaviour.
        """
        logger.debug(
            "Strategy {} started in public transport customer {}".format(
                type(self).__name__,
                self.agent.name
            )
        )

    def get_stop_id(
        self,
        stop
    ):
        """
        Return the logical PublicTransport stop identifier from a stop descriptor.

        Args:
            stop (dict | None): Journey stop descriptor.

        Returns:
            object | None: Stop identifier.
        """
        if stop is None:
            return None

        return stop.get(
            "id"
        )

    def get_stop_jid(
        self,
        stop
    ):
        """
        Return the XMPP JID of a PublicTransport stop descriptor.

        Args:
            stop (dict | None): Journey stop descriptor.

        Returns:
            object | None: Stop JID.
        """
        if stop is None:
            return None

        return stop.get(
            "jid"
        )

    def select_journey(
        self,
        journeys=None
    ):
        """
        Select one feasible journey with the baseline lexicographic policy.

        Candidates are ordered by:

        1. fewer PublicTransport transfers;
        2. less total walking distance;
        3. fewer PublicTransport stops.

        This is a lexicographic preference, not a weighted cost function. A
        journey with fewer transfers therefore wins before walking distance is
        considered.

        Args:
            journeys (list | None): Candidate journeys. When omitted, candidates
                stored by the customer are used.

        Returns:
            dict | None: Selected journey or None when no candidate exists.
        """

        if journeys is None:

            journeys = (
                self.agent.get_journey_candidates()
            )

        if not journeys:
            return None

        return min(
            journeys,
            key=lambda journey: (
                journey.get(
                    "transfers",
                    float("inf")
                ),

                journey.get(
                    "walking_distance",
                    float("inf")
                ),

                journey.get(
                    "public_transport_stops",
                    float("inf")
                ),
            )
        )

    async def request_journeys(
        self,
        fleetmanager_id
    ):
        """
        Request PublicTransport journey candidates from one FleetManager.

        A valid request requires current customer origin and final destination.

        The first successfully prepared journey request creates the canonical
        ``service_id`` and emits ``service_requested``.

        The request carries walking constraints, maximum transfers, and canonical
        service identifiers through REQUEST_PROTOCOL / REQUEST_PERFORMATIVE.

        Args:
            fleetmanager_id: PublicTransport FleetManager JID.

        Returns:
            bool: True when the journey request was sent.
        """
        if fleetmanager_id is None:
            return False

        origin = (
            self.agent.get_position()
        )

        destination = (
            self.agent.get_target_position()
        )

        if (
            origin is None
            or destination is None
        ):

            logger.warning(
                "Public transport customer {} cannot request journeys "
                "without origin and destination.".format(
                    self.agent.name
                )
            )

            return False

        context = self.get_service_context()

        if context is None:
            context = self.create_service_context(
                origin=origin,
                destination=destination,
                emit_requested=True,
            )

        if context is None:
            return False

        content = self.add_service_identifiers(
            {
                "request_type":
                    "public_transport_journeys",

                "origin":
                    origin,

                "dest":
                    destination,

                "max_access_walking_distance":
                    self.agent.get_max_access_walking_distance(),

                "max_transfer_walking_distance":
                    self.agent.get_max_transfer_walking_distance(),

                "max_transfers":
                    self.agent.get_max_transfers(),
            },
            context=context,
        )

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

        logger.debug(
            "Public transport customer {} requesting journeys from {}".format(
                self.agent.name,
                fleetmanager_id
            )
        )

        await self.send(
            msg
        )

        return True

    async def request_stop_queue(
        self,
        stop,
        pattern_id,
        destination_stop
    ):
        """
        Request admission to the queue for one directional PublicTransport Pattern
        at a stop.

        The queue request is operational stop infrastructure rather than a
        canonical service lifecycle event.

        It identifies the directional ``pattern_id`` and the customer's intended
        destination stop through REQUEST_PROTOCOL / REQUEST_PERFORMATIVE.

        Args:
            stop: Origin-stop descriptor.
            pattern_id: Directional PublicTransport Pattern.
            destination_stop: Logical destination-stop identifier.

        Returns:
            bool: True when the queue request was sent.
        """
        stop_jid = (
            self.get_stop_jid(
                stop
            )
        )

        stop_id = (
            self.get_stop_id(
                stop
            )
        )

        if (
            stop_jid is None
            or stop_id is None
            or pattern_id is None
            or destination_stop is None
        ):

            logger.warning(
                "Public transport customer {} cannot enter stop queue: "
                "invalid stop or pattern data.".format(
                    self.agent.name
                )
            )

            return False

        content = {
            "service_name":
                pattern_id,

            "object_type":
                "customer",

            "args": {
                "pattern_id":
                    pattern_id,

                "destination_stop":
                    destination_stop,
            },
        }

        msg = Message()

        msg.to = str(
            stop_jid
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

        logger.debug(
            "Public transport customer {} requesting queue {} at stop {}".format(
                self.agent.name,
                pattern_id,
                stop_id
            )
        )

        await self.send(
            msg
        )

        return True

    async def cancel_stop_queue(
        self,
        stop,
        pattern_id
    ):
        """
        Cancel the customer's queue registration for one PublicTransport Pattern.

        REQUEST_PROTOCOL / CANCEL_PERFORMATIVE is sent directly to the stop using
        the directional ``pattern_id`` as service name.

        Args:
            stop: Stop descriptor.
            pattern_id: Pattern whose queue registration must be cancelled.

        Returns:
            bool: True when the cancellation request was sent.
        """
        stop_jid = (
            self.get_stop_jid(
                stop
            )
        )

        if (
            stop_jid is None
            or pattern_id is None
        ):
            return False

        msg = Message()

        msg.to = str(
            stop_jid
        )

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            CANCEL_PERFORMATIVE
        )

        msg.body = json.dumps(
            {
                "service_name":
                    pattern_id,
            }
        )

        await self.send(
            msg
        )

        return True

    async def request_boarding(
        self,
        vehicle_id,
        leg
    ):
        """
        Request boarding of one compatible PublicTransport vehicle.

        The current journey leg supplies Pattern, origin stop, destination stop,
        route metadata, and leg index.

        A deterministic ``boarding_key`` is built from leg index, Pattern, origin
        stop, and destination stop so repeated processing of the same boarding can
        be distinguished from later transfers under the same ``service_id``.

        The candidate vehicle becomes the current ``transport_id`` in the service
        context before REQUEST_PROTOCOL / REQUEST_PERFORMATIVE is sent.

        Args:
            vehicle_id: Candidate PublicTransport vehicle JID.
            leg (dict): Current PublicTransport journey leg.

        Returns:
            bool: True when the boarding request was sent.
        """
        if (
            vehicle_id is None
            or leg is None
        ):
            return False

        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False

        pattern_id = leg.get(
            "pattern_id"
        )

        origin_stop = (
            self.get_stop_id(
                leg.get(
                    "origin_stop"
                )
            )
        )

        destination_stop = (
            self.get_stop_id(
                leg.get(
                    "destination_stop"
                )
            )
        )

        if (
            pattern_id is None
            or origin_stop is None
            or destination_stop is None
        ):

            logger.warning(
                "Public transport customer {} cannot request boarding: "
                "invalid public transport leg.".format(
                    self.agent.name
                )
            )

            return False

        boarding_key = "{}:{}:{}:{}".format(
            self.agent.current_leg_index,
            pattern_id,
            origin_stop,
            destination_stop,
        )

        content = self.add_service_identifiers(
            {
                "request_type":
                    "public_transport_board",

                "pattern_id":
                    pattern_id,

                "origin_stop":
                    origin_stop,

                "destination_stop":
                    destination_stop,

                "leg_index":
                    self.agent.current_leg_index,

                "boarding_key":
                    boarding_key,
            },
            context=context,
            transport_id=vehicle_id,
        )

        msg = Message()

        msg.to = str(
            vehicle_id
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

        logger.debug(
            "Public transport customer {} requesting boarding to vehicle {} "
            "for pattern {} from {} to {}".format(
                self.agent.name,
                vehicle_id,
                pattern_id,
                origin_stop,
                destination_stop
            )
        )

        await self.send(
            msg
        )

        return True

    async def run(
        self
    ):
        """
        Execute the concrete PublicTransport customer FSM state.

        Raises:
            NotImplementedError: When a concrete state does not implement
                execution.
        """
        raise NotImplementedError


# ==================================================================
# ------------------------------------------------------------------
# ==================================================================




class PublicTransportCustomerWaitingToMoveState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Dispatch PublicTransport journey planning and execution.

    Before a journey has been selected, this state discovers PublicTransport
    FleetManagers and asks every known manager for journey candidates.

    FleetManager discovery is operational bootstrap and does not itself create
    the canonical service. ``service_requested`` is emitted only when at least
    one journey request can actually be sent.

    After a journey has been selected, the state becomes the central
    leg dispatcher:

    - a walking leg enters ``CUSTOMER_MOVING_TO_DEST``;
    - a PublicTransport leg enters ``CUSTOMER_IN_STOP``;
    - a finished journey enters terminal ``CUSTOMER_IN_DEST``.

    The selected journey remains the runtime source of truth and
    ``current_leg_index`` determines which leg is dispatched next.

    Unsupported leg types are currently treated as retryable configuration
    errors: the customer remains on the same leg and retries this dispatcher.
    """

    async def on_start(
        self
    ):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_WAITING_TO_MOVE
        )

    async def run(
        self
    ):
        """
        Plan a journey when necessary or dispatch the current journey leg.

        Without a selected journey, the customer first discovers PublicTransport
        FleetManagers. Absence of FleetManagers is retryable and does not create
        ``service_requested``.

        Once FleetManagers exist, journey requests are sent to every known manager.
        If none can be sent, execution retries later in
        ``CUSTOMER_WAITING_TO_MOVE``.

        At least one successfully sent request advances to
        ``CUSTOMER_WAITING_FOR_JOURNEY``.

        With an existing journey:

        - a finished journey enters ``CUSTOMER_IN_DEST``;
        - a walking leg enters ``CUSTOMER_MOVING_TO_DEST``;
        - a PublicTransport leg enters ``CUSTOMER_IN_STOP``.

        Unsupported leg types are logged and retried without advancing
        ``current_leg_index``.
        """
        #
        # No journey has been selected yet.
        #

        if self.agent.get_journey() is None:

            fleetmanagers = (
                self.agent.get_fleetmanagers()
            )

            #
            # Discover FleetManagers only when they
            # are not already known.
            #

            if not fleetmanagers:

                logger.info(
                    "Public transport customer {} "
                    "looking for FleetManagers.".format(
                        self.agent.name
                    )
                )

                fleetmanagers = (
                    await self.agent
                    .get_list_agent_position(
                        self.agent.fleet_type,
                        self.agent.get_fleetmanagers()
                    )
                )

                self.agent.set_fleetmanagers(
                    fleetmanagers
                )

            #
            # No Public Transport FleetManager exists
            # yet. Retry later.
            #

            if not fleetmanagers:

                logger.warning(
                    "Public transport customer {} "
                    "found no FleetManagers.".format(
                        self.agent.name
                    )
                )

                await self.agent.sleep(
                    5
                )

                self.set_next_state(
                    CUSTOMER_WAITING_TO_MOVE
                )

                return

            #
            # Ask every Public Transport FleetManager.
            #
            # The Customer will combine all candidate
            # journeys before selecting one.
            #

            requests_sent = 0

            for fleetmanager_id in (
                fleetmanagers.keys()
            ):

                sent = (
                    await self.request_journeys(
                        fleetmanager_id
                    )
                )

                if sent:
                    requests_sent += 1

            if requests_sent == 0:

                logger.warning(
                    "Public transport customer {} "
                    "could not send any journey request.".format(
                        self.agent.name
                    )
                )

                await self.agent.sleep(
                    5
                )

                self.set_next_state(
                    CUSTOMER_WAITING_TO_MOVE
                )

                return

            self.set_next_state(
                CUSTOMER_WAITING_FOR_JOURNEY
            )

            return

        #
        # Journey already selected.
        #
        # This state now acts as the dispatcher for
        # the current leg.
        #

        if self.agent.is_journey_finished():

            self.set_next_state(
                CUSTOMER_IN_DEST
            )

            return

        leg = (
            self.agent.get_current_leg()
        )

        if leg is None:

            self.set_next_state(
                CUSTOMER_IN_DEST
            )

            return

        leg_type = leg.get(
            "type"
        )

        #
        # Walking leg.
        #

        if leg_type == "walking":

            self.set_next_state(
                CUSTOMER_MOVING_TO_DEST
            )

            return

        #
        # Public Transport leg.
        #

        if leg_type == (
            "public_transport"
        ):

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        #
        # Malformed / unsupported journey.
        #
        # Do not mark the journey as completed because
        # the Customer has not actually reached its
        # destination.
        #

        logger.error(
            "Public transport customer {} "
            "has unsupported journey leg type {}.".format(
                self.agent.name,
                leg_type
            )
        )

        await self.agent.sleep(
            5
        )

        self.set_next_state(
            CUSTOMER_WAITING_TO_MOVE
        )


class PublicTransportCustomerWaitingForJourneyState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Collect PublicTransport journey responses and select one feasible journey.

    The customer expects up to one response from every currently known
    FleetManager but uses a bounded receive window. Missing responses do not
    prevent selection when other FleetManagers already supplied viable
    journeys.

    Valid journey lists are combined into one candidate collection and the
    baseline lexicographic selection policy chooses the preferred journey.

    Once selected, that journey becomes the customer's runtime source of truth
    and execution returns to ``CUSTOMER_WAITING_TO_MOVE`` for leg dispatch.

    If no feasible journey is available, execution enters terminal
    ``CUSTOMER_JOURNEY_FAILED``.
    """
    async def on_start(self):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_WAITING_FOR_JOURNEY
        )

    async def run(self):
        """
        Collect journey proposals from known PublicTransport FleetManagers.

        If FleetManagers disappeared before response collection begins, execution
        returns to ``CUSTOMER_WAITING_TO_MOVE`` so normal discovery can run again.

        Up to one response per expected FleetManager is collected within the
        receive window. REQUEST_PROTOCOL / INFORM_PERFORMATIVE messages containing
        ``request_type="public_transport_journeys"`` contribute their journey
        lists.

        A timeout ends collection but does not invalidate journeys already
        received.

        All returned journeys are stored as candidates and passed to
        ``select_journey()``.

        No selected journey enters ``CUSTOMER_JOURNEY_FAILED``. A successful
        selection resets journey execution state through ``set_journey()`` and
        returns to ``CUSTOMER_WAITING_TO_MOVE``.
        """
        fleetmanagers = (
            self.agent.get_fleetmanagers()
        )

        if not fleetmanagers:

            #
            # Bootstrap case.
            #
            # No polling is performed here.
            # The Customer returns to WAITING_TO_MOVE
            # so the normal FleetManager discovery
            # mechanism can be executed.
            #

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        journey_candidates = []

        expected_responses = len(
            fleetmanagers
        )

        received_responses = 0

        while (
            received_responses
            < expected_responses
        ):

            msg = await self.receive(
                timeout=5
            )

            if not msg:

                #
                # One or more FleetManagers did not
                # answer inside the response window.
                #

                break

            protocol = msg.get_metadata(
                "protocol"
            )

            performative = msg.get_metadata(
                "performative"
            )

            if (
                protocol != REQUEST_PROTOCOL
                or performative
                != INFORM_PERFORMATIVE
            ):

                continue

            try:

                content = (
                    json.loads(msg.body)
                    if msg.body
                    else {}
                )

            except (
                json.JSONDecodeError,
                TypeError
            ):

                logger.warning(
                    "Public transport customer {} "
                    "received an invalid journey "
                    "response.".format(
                        self.agent.name
                    )
                )

                continue

            if (
                content.get("request_type")
                != "public_transport_journeys"
            ):

                continue

            received_responses += 1

            journeys = content.get(
                "journeys",
                []
            )

            if not isinstance(
                journeys,
                list
            ):
                continue

            journey_candidates.extend(
                journeys
            )

        self.agent.set_journey_candidates(
            journey_candidates
        )

        selected_journey = (
            self.select_journey(
                journey_candidates
            )
        )

        if selected_journey is None:

            logger.warning(
                "Public transport customer {} "
                "could not find a feasible journey.".format(
                    self.agent.name
                )
            )

            self.agent.clear_journey()

            self.set_next_state(
                CUSTOMER_JOURNEY_FAILED
            )

            return

        #
        # From this point the Journey becomes the
        # Customer's runtime source of truth.
        #

        self.agent.set_journey(
            selected_journey
        )

        logger.info(
            "Public transport customer {} "
            "selected journey with {} transfers, "
            "{} walking distance and {} "
            "public transport stops.".format(
                self.agent.name,

                selected_journey.get(
                    "transfers",
                    0
                ),

                selected_journey.get(
                    "walking_distance",
                    0
                ),

                selected_journey.get(
                    "public_transport_stops",
                    0
                )
            )
        )

        self.set_next_state(
            CUSTOMER_WAITING_TO_MOVE
        )

class PublicTransportCustomerJourneyFailedState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Terminal unsuccessful state for PublicTransport journey planning.

    This state is entered when journey collection produces no feasible
    candidate.

    On entry, any pending customer movement is discarded and the complete
    logical service emits ``service_failed`` with
    ``failure_reason="no_feasible_journey"``.

    The current position and leg index are retained in the failure event for
    diagnostics.

    ``run()`` clears transient vehicle, Pattern, and pedestrian state and does
    not select another FSM state, allowing this PublicTransport strategy
    execution to terminate naturally.
    """

    async def on_start(self):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_JOURNEY_FAILED
        )

        logger.warning(
            "Public transport customer {} "
            "finished with an incomplete journey "
            "at position {}.".format(
                self.agent.name,
                self.agent.get_position()
            )
        )

        self.discard_pending_movement()

        self.fail_service(
            "no_feasible_journey",
            extra_details={
                "position": self.agent.get_position(),
                "leg_index": self.agent.current_leg_index,
            },
        )

    async def run(self):
        """
        Clear transient PublicTransport execution state and terminate this failed
        FSM path.

        No next state is selected.
        """
        self.agent.clear_current_vehicle()

        self.agent.clear_waiting_pattern_id()

        self.agent.pedestrian_dest = None

        #
        # Intentionally no set_next_state().
        #
        # CUSTOMER_JOURNEY_FAILED remains a real
        # terminal state.
        #

        return

class PublicTransportCustomerMovingToDestState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Execute and monitor one walking leg of the selected PublicTransport
    journey.

    The same state handles:

    - access walking from the customer origin to the first stop;
    - transfer walking between PublicTransport stops;
    - final walking from the last stop to the door-to-door destination.

    Walking before the first accepted boarding is recorded as
    ``phase="approach"``.

    Once the PublicTransport service has started, transfer and final walking
    are recorded as ``phase="service"``.

    Every walking movement uses ``movement_mode="walking"`` and explicitly
    clears ``transport_id`` because the customer is not inside a
    PublicTransport vehicle during that movement.

    Successful completion advances the current journey leg and returns to
    ``CUSTOMER_WAITING_TO_MOVE`` for dispatch of the next leg.
    """

    async def on_start(
        self
    ):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_MOVING_TO_DEST
        )

    def complete_walking_leg(
        self,
        leg
    ):
        """
        Finalize one logical walking leg after physical movement completes.

        When the leg ends at a PublicTransport stop, that stop becomes the
        customer's current logical stop. This applies to access and transfer
        walking.

        When no destination-stop descriptor exists, the leg is treated as final
        walking and the current stop is cleared.

        Temporary ``pedestrian_dest`` state is then cleared and
        ``current_leg_index`` advances exactly once.

        Args:
            leg (dict): Completed walking journey leg.
        """

        destination_stop = (
            leg.get(
                "destination_stop"
            )
        )

        #
        # Access or transfer walking:
        #
        # the walking leg finishes at a Public
        # Transport Stop.
        #

        if destination_stop is not None:

            destination_stop_id = (
                self.get_stop_id(
                    destination_stop
                )
            )

            if destination_stop_id is not None:

                self.agent.set_current_stop(
                    destination_stop_id
                )

        #
        # Final walking:
        #
        # the Customer is no longer located at a
        # Public Transport Stop.
        #

        else:

            self.agent.clear_current_stop()

        #
        # pedestrian_dest is only used while the
        # physical walking movement is active.
        #

        self.agent.pedestrian_dest = None

        #
        # The walking leg has been completed.
        #

        self.agent.advance_leg()

    async def run(
        self
    ):
        """
        Execute or continue the current PublicTransport walking leg.

        Missing or non-walking current legs return control to the central
        dispatcher without advancing the journey.

        A walking leg requires a physical destination and an active canonical
        service context.

        Its metric phase is selected dynamically:

        - ``approach`` before the service has started;
        - ``service`` after the first successful boarding.

        Physical movement is correlated through one pending movement carrying
        ``movement_mode="walking"``, ``leg_index``, and ``transport_id=None``.

        Already being at the target produces an explicit zero-distance movement
        when necessary, finalizes the walking leg, and returns to the dispatcher.

        Active movement remains in ``CUSTOMER_MOVING_TO_DEST``.

        PathRequestException is currently treated as retryable: the incomplete
        movement is discarded, the customer waits briefly, and the same walking
        leg is attempted again.
        """
        leg = (
            self.agent.get_current_leg()
        )

        if leg is None:

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if leg.get(
            "type"
        ) != "walking":

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        destination = (
            leg.get(
                "destination"
            )
        )

        if destination is None:

            logger.error(
                "Public transport customer {} "
                "has a walking leg without destination.".format(
                    self.agent.name
                )
            )

            await self.agent.sleep(
                5
            )

            self.set_next_state(
                CUSTOMER_MOVING_TO_DEST
            )

            return

        context = self.get_service_context()

        if context is None:
            logger.error(
                "Public transport customer {} has no metrics service "
                "context while executing a walking leg.".format(
                    self.agent.name
                )
            )
            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )
            return

        phase = (
            "service"
            if context.get("started")
            else "approach"
        )

        movement_details = {
            "transport_id": None,
            "leg_index": self.agent.current_leg_index,
            "movement_mode": "walking",
        }

        self.agent.pedestrian_dest = (
            destination
        )

        if self.agent.get_position() == (
            destination
        ):

            if not self.complete_pending_movement():
                self.set_pending_movement(
                    phase,
                    0,
                    extra_details=movement_details,
                )
                self.complete_pending_movement()

            self.complete_walking_leg(
                leg
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if self.agent.dest == (
            destination
        ):

            if self.agent.is_in_destination():

                if not self.complete_pending_movement():
                    self.set_pending_movement(
                        phase,
                        0,
                        extra_details=movement_details,
                    )
                    self.complete_pending_movement()

                self.complete_walking_leg(
                    leg
                )

                self.set_next_state(
                    CUSTOMER_WAITING_TO_MOVE
                )

                return

            await self.agent.sleep(
                1
            )

            self.set_next_state(
                CUSTOMER_MOVING_TO_DEST
            )

            return

        try:

            distance, _, _ = await self.agent.move_to(
                destination
            )

            self.set_pending_movement(
                phase,
                distance,
                extra_details=movement_details,
            )

        except AlreadyInDestination:

            self.agent.dest = (
                destination
            )

            if not self.complete_pending_movement():
                self.set_pending_movement(
                    phase,
                    0,
                    extra_details=movement_details,
                )
                self.complete_pending_movement()

            self.complete_walking_leg(
                leg
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        except PathRequestException:

            logger.error(
                "Public transport customer {} "
                "could not obtain a walking route to {}.".format(
                    self.agent.name,
                    destination
                )
            )

            self.discard_pending_movement()

            await self.agent.sleep(
                2
            )

            self.set_next_state(
                CUSTOMER_MOVING_TO_DEST
            )

            return

        await self.agent.sleep(
            1
        )

        self.set_next_state(
            CUSTOMER_MOVING_TO_DEST
        )


class PublicTransportCustomerInStopState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Request queue admission for the current PublicTransport leg.

    The current journey leg must define a directional Pattern, an origin stop,
    and a destination stop.

    ``current_stop`` may legitimately be unset when the journey begins directly
    at a stop. In that case the stop infrastructure performs the physical
    proximity validation. A different explicitly known current stop is treated
    as inconsistent journey state.

    Queue registration is operational stop infrastructure and is correlated by
    stop JID, Pattern, and destination stop rather than by canonical
    ``service_id``.

    After sending the queue request, the customer waits for one station reply.

    A valid ACCEPT establishes the logical current stop and waiting Pattern and
    advances to ``CUSTOMER_WAITING``.

    REFUSE, malformed responses, request failures, and queue-confirmation
    timeouts are retryable and remain in ``CUSTOMER_IN_STOP``.

    On timeout or an invalid response after a queue request, the customer
    cancels the queue entry before retrying because the stop queue does not
    guarantee duplicate suppression.
    """
    async def on_start(self):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_IN_STOP
        )

    def _bare_jid(self, jid):
        """
        Normalize a JID to its bare form for stop-response correlation.

        Args:
            jid: SPADE/Jabber identifier.

        Returns:
            str | None: Bare JID or None.
        """
        if jid is None:
            return None

        return str(jid).split("/")[0]

    async def run(self):
        """
        Validate the current PublicTransport leg and request queue admission.

        Missing or non-PublicTransport legs return control to the journey
        dispatcher.

        Invalid Pattern or stop metadata likewise returns to
        ``CUSTOMER_WAITING_TO_MOVE``.

        A queue request is sent to the exact origin-stop JID for the directional
        Pattern and intended destination stop.

        Failure to send the request is retryable.

        After a successful request, the customer waits up to the current queue
        confirmation window for the station response.

        Timeout, malformed payload, wrong sender, or unsupported response triggers
        queue cancellation before retry whenever an entry may already have been
        created.

        A valid ACCEPT records the logical current stop and waiting Pattern and
        enters ``CUSTOMER_WAITING``.

        A REFUSE leaves the journey and service open and retries stop admission.
        """
        leg = self.agent.get_current_leg()

        if leg is None:

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if (
            leg.get("type")
            != "public_transport"
        ):

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        pattern_id = (
            leg.get("pattern_id")
        )

        origin_stop = (
            leg.get("origin_stop")
        )

        destination_stop = (
            leg.get("destination_stop")
        )

        origin_stop_id = (
            self.get_stop_id(
                origin_stop
            )
        )

        origin_stop_jid = (
            self.get_stop_jid(
                origin_stop
            )
        )

        destination_stop_id = (
            self.get_stop_id(
                destination_stop
            )
        )

        if (
            pattern_id is None
            or origin_stop_id is None
            or origin_stop_jid is None
            or destination_stop_id is None
        ):

            logger.error(
                "Public transport customer {} "
                "has an invalid public transport leg "
                "while entering a stop.".format(
                    self.agent.name
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        #
        # current_stop may legitimately be None when
        # the Journey begins directly at a Stop.
        #
        # In that case QueueStationAgent performs the
        # physical proximity check before accepting
        # the Customer into the queue.
        #
        # We only reject the operation when the
        # Customer explicitly knows that it is at a
        # DIFFERENT logical Stop.
        #

        current_stop = (
            self.agent.get_current_stop()
        )

        if (
            current_stop is not None
            and current_stop
            != origin_stop_id
        ):

            logger.error(
                "Public transport customer {} "
                "cannot enter queue at stop {} "
                "because current logical stop "
                "is {}.".format(
                    self.agent.name,
                    origin_stop_id,
                    current_stop
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        sent = await self.request_stop_queue(
            origin_stop,
            pattern_id,
            destination_stop_id
        )

        if not sent:

            logger.warning(
                "Public transport customer {} "
                "could not request queue {} "
                "at stop {}.".format(
                    self.agent.name,
                    pattern_id,
                    origin_stop_id
                )
            )

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        msg = await self.receive(
            timeout=30
        )

        if not msg:

            #
            # QueueStationAgent does not deduplicate
            # queue entries.
            #
            # If ACCEPT was lost, retrying directly
            # could enqueue the same Customer twice.
            #
            # Therefore CANCEL before retrying.
            #

            await self.cancel_stop_queue(
                origin_stop,
                pattern_id
            )

            logger.warning(
                "Public transport customer {} "
                "did not receive queue confirmation "
                "from stop {} for pattern {}. "
                "Cancelling before retry.".format(
                    self.agent.name,
                    origin_stop_id,
                    pattern_id
                )
            )

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        protocol = msg.get_metadata(
            "protocol"
        )

        performative = msg.get_metadata(
            "performative"
        )

        sender = self._bare_jid(
            msg.sender
        )

        expected_sender = self._bare_jid(
            origin_stop_jid
        )

        if (
            protocol != REQUEST_PROTOCOL
            or sender != expected_sender
        ):

            await self.cancel_stop_queue(
                origin_stop,
                pattern_id
            )

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        try:

            content = (
                json.loads(msg.body)
                if msg.body
                else {}
            )

        except (
            json.JSONDecodeError,
            TypeError
        ):

            await self.cancel_stop_queue(
                origin_stop,
                pattern_id
            )

            logger.warning(
                "Public transport customer {} "
                "received invalid queue response "
                "from stop {}.".format(
                    self.agent.name,
                    origin_stop_id
                )
            )

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        if (
            performative
            == ACCEPT_PERFORMATIVE
        ):

            station_id = self._bare_jid(
                content.get(
                    "station_id"
                )
            )

            if (
                station_id
                != expected_sender
            ):

                await self.cancel_stop_queue(
                    origin_stop,
                    pattern_id
                )

                logger.warning(
                    "Public transport customer {} "
                    "received queue acceptance for "
                    "unexpected station {}.".format(
                        self.agent.name,
                        station_id
                    )
                )

                self.set_next_state(
                    CUSTOMER_IN_STOP
                )

                return

            #
            # Only now do we consider the Customer
            # logically located at the Stop.
            #

            self.agent.set_current_stop(
                origin_stop_id
            )

            self.agent.set_waiting_pattern_id(
                pattern_id
            )

            self.agent.clear_current_vehicle()

            logger.info(
                "Public transport customer {} "
                "registered at stop {} waiting "
                "for pattern {} to destination {}.".format(
                    self.agent.name,
                    origin_stop_id,
                    pattern_id,
                    destination_stop_id
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING
            )

            return

        if (
            performative
            == REFUSE_PERFORMATIVE
        ):

            self.agent.clear_waiting_pattern_id()

            logger.info(
                "Public transport customer {} "
                "was refused queue access at stop {} "
                "for pattern {}.".format(
                    self.agent.name,
                    origin_stop_id,
                    pattern_id
                )
            )

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        await self.cancel_stop_queue(
            origin_stop,
            pattern_id
        )

        self.set_next_state(
            CUSTOMER_IN_STOP
        )


class PublicTransportCustomerWaitingState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Wait reactively at a PublicTransport stop for a compatible vehicle.

    The customer has already been accepted into the queue associated with the
    current directional Pattern.

    No polling of FleetManagers, stops, or vehicles is performed.

    The state waits for REQUEST_PROTOCOL / INFORM_PERFORMATIVE
    ``public_transport_vehicle_available`` notifications sent by the exact
    origin stop.

    A notification is accepted only when stop and Pattern match the current
    journey leg and a concrete ``vehicle_id`` is supplied.

    The customer then sends a boarding request directly to that vehicle and
    stores it as the current provisional vehicle before entering
    ``CUSTOMER_WAITING_FOR_APPROVAL``.

    Unrelated, malformed, stale, or incompatible availability notifications are
    ignored while the customer remains reactively queued.
    """

    async def on_start(
        self
    ):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_WAITING
        )

    async def run(
        self
    ):
        """
        Resolve one direct PublicTransport boarding request.

        The state waits only for ACCEPT_PERFORMATIVE or REFUSE_PERFORMATIVE from the
        currently selected candidate vehicle.

        Boarding responses must match the active canonical service, vehicle,
        directional Pattern, origin stop, and destination stop.

        ACCEPT mirrors the vehicle-owned ``service_assigned`` milestone locally and
        calls ``start_service()``.

        The first successful boarding emits the unique customer-owned
        ``service_started`` event. Later transfers reuse the same ``service_id``;
        ``start_service()`` then remains idempotent and emits no second start.

        After acceptance, the customer clears stop-queue execution state and enters
        ``CUSTOMER_IN_TRANSPORT``.

        REFUSE clears only the provisional vehicle and returns to
        ``CUSTOMER_WAITING``. The customer remains in the same stop queue and may
        board a later compatible vehicle without creating a new service.
        """
        leg = (
            self.agent.get_current_leg()
        )

        #
        # Defensive validation.
        #

        if leg is None:

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if leg.get(
            "type"
        ) != (
            "public_transport"
        ):

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        pattern_id = leg.get(
            "pattern_id"
        )

        origin_stop = leg.get(
            "origin_stop"
        )

        destination_stop = leg.get(
            "destination_stop"
        )

        origin_stop_id = (
            self.get_stop_id(
                origin_stop
            )
        )

        origin_stop_jid = (
            self.get_stop_jid(
                origin_stop
            )
        )

        destination_stop_id = (
            self.get_stop_id(
                destination_stop
            )
        )

        if (
            pattern_id is None
            or origin_stop_id is None
            or origin_stop_jid is None
            or destination_stop_id is None
        ):

            logger.error(
                "Public transport customer {} "
                "has an invalid public transport leg "
                "while waiting at stop.".format(
                    self.agent.name
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if (
            self.agent.get_current_stop()
            != origin_stop_id
        ):

            logger.error(
                "Public transport customer {} "
                "is waiting at logical stop {}, "
                "but current leg starts at {}.".format(
                    self.agent.name,
                    self.agent.get_current_stop(),
                    origin_stop_id
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if (
            self.agent.get_waiting_pattern_id()
            != pattern_id
        ):

            logger.error(
                "Public transport customer {} "
                "is waiting for pattern {}, "
                "but current leg requires {}.".format(
                    self.agent.name,
                    self.agent.get_waiting_pattern_id(),
                    pattern_id
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        expected_sender = str(
            origin_stop_jid
        ).split(
            "/"
        )[0]

        #
        # Reactive wait.
        #
        # SPADE receive() without timeout is
        # non-blocking.
        #
        # A positive timeout is required to actually
        # suspend the behaviour while waiting.
        #

        while True:

            msg = await self.receive(
                timeout=3600
            )

            #
            # Defensive wake-up only.
            #
            # No polling or external query is performed.
            #

            if not msg:
                continue

            protocol = (
                msg.get_metadata(
                    "protocol"
                )
            )

            performative = (
                msg.get_metadata(
                    "performative"
                )
            )

            if (
                protocol != REQUEST_PROTOCOL
                or performative != INFORM_PERFORMATIVE
            ):
                continue

            try:

                content = (
                    json.loads(
                        msg.body
                    )
                    if msg.body
                    else {}
                )

            except (
                json.JSONDecodeError,
                TypeError,
            ):

                logger.warning(
                    "Public transport customer {} "
                    "received an invalid message while "
                    "waiting at stop {}.".format(
                        self.agent.name,
                        origin_stop_id
                    )
                )

                continue

            if content.get(
                "request_type"
            ) != (
                "public_transport_vehicle_available"
            ):

                continue

            vehicle_id = content.get(
                "vehicle_id"
            )

            informed_pattern_id = (
                content.get(
                    "pattern_id"
                )
            )

            informed_stop_id = (
                content.get(
                    "stop"
                )
            )

            sender = str(
                msg.sender
            ).split(
                "/"
            )[0]

            if sender != (
                expected_sender
            ):

                logger.warning(
                    "Public transport customer {} "
                    "ignored vehicle notification from "
                    "unexpected stop {}.".format(
                        self.agent.name,
                        sender
                    )
                )

                continue

            if informed_stop_id != (
                origin_stop_id
            ):

                logger.warning(
                    "Public transport customer {} "
                    "ignored vehicle notification for "
                    "stop {} while waiting at {}.".format(
                        self.agent.name,
                        informed_stop_id,
                        origin_stop_id
                    )
                )

                continue

            if informed_pattern_id != (
                pattern_id
            ):

                logger.debug(
                    "Public transport customer {} "
                    "ignored vehicle for pattern {} "
                    "while waiting for {}.".format(
                        self.agent.name,
                        informed_pattern_id,
                        pattern_id
                    )
                )

                continue

            if vehicle_id is None:

                logger.warning(
                    "Public transport customer {} "
                    "received vehicle availability "
                    "without vehicle_id at stop {}.".format(
                        self.agent.name,
                        origin_stop_id
                    )
                )

                continue

            sent = (
                await self.request_boarding(
                    vehicle_id,
                    leg
                )
            )

            if not sent:

                logger.warning(
                    "Public transport customer {} "
                    "could not request boarding to "
                    "vehicle {}.".format(
                        self.agent.name,
                        vehicle_id
                    )
                )

                continue

            self.agent.set_current_vehicle(
                vehicle_id
            )

            logger.info(
                "Public transport customer {} "
                "requesting boarding to vehicle {} "
                "at stop {} for pattern {} "
                "towards {}.".format(
                    self.agent.name,
                    vehicle_id,
                    origin_stop_id,
                    pattern_id,
                    destination_stop_id
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_FOR_APPROVAL
            )

            return


class PublicTransportCustomerWaitingForApprovalState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Resolve one direct PublicTransport boarding request.

    The state waits only for ACCEPT_PERFORMATIVE or REFUSE_PERFORMATIVE from the
    currently selected candidate vehicle.

    Boarding responses must match the active canonical service, vehicle,
    directional Pattern, origin stop, and destination stop.

    ACCEPT mirrors the vehicle-owned ``service_assigned`` milestone locally and
    calls ``start_service()``.

    The first successful boarding emits the unique customer-owned
    ``service_started`` event. Later transfers reuse the same ``service_id``;
    ``start_service()`` then remains idempotent and emits no second start.

    After acceptance, the customer clears stop-queue execution state and enters
    ``CUSTOMER_IN_TRANSPORT``.

    REFUSE clears only the provisional vehicle and returns to
    ``CUSTOMER_WAITING``. The customer remains in the same stop queue and may
    board a later compatible vehicle without creating a new service.
    """
    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            CUSTOMER_WAITING_FOR_APPROVAL
        )

    def _bare_jid(self, jid):
        """
        Normalize a JID for candidate-vehicle response correlation.

        Returns:
            str | None: Bare JID or None.
        """
        if jid is None:
            return None

        return str(jid).split("/")[0]

    async def run(self):
        """
        Process acceptance or refusal from the provisional PublicTransport vehicle.

        Missing or incompatible journey state abandons the provisional vehicle and
        returns to normal journey or stop waiting.

        The state reacts only to REQUEST_PROTOCOL messages from the exact candidate
        vehicle carrying ACCEPT_PERFORMATIVE or REFUSE_PERFORMATIVE.

        A valid payload must identify ``public_transport_board`` and match the
        canonical service, Pattern, origin stop, and destination stop.

        ACCEPT mirrors the boarding assignment using the returned
        ``boarding_key`` and invokes ``start_service()``.

        ``start_service()`` may legitimately return False on later transfers
        because the door-to-door service has already started; this does not make
        the new boarding invalid.

        The stop and waiting-Pattern markers are cleared after successful boarding
        and execution advances to ``CUSTOMER_IN_TRANSPORT``.

        REFUSE clears only candidate transport state and returns to
        ``CUSTOMER_WAITING`` for another vehicle.
        """
        leg = self.agent.get_current_leg()

        if leg is None:

            self.agent.clear_current_vehicle()

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if leg.get("type") != "public_transport":

            self.agent.clear_current_vehicle()

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        vehicle_id = (
            self.agent.get_current_vehicle()
        )

        if vehicle_id is None:

            self.set_next_state(
                CUSTOMER_WAITING
            )

            return

        pattern_id = (
            leg.get("pattern_id")
        )

        origin_stop_id = self.get_stop_id(
            leg.get("origin_stop")
        )

        destination_stop_id = self.get_stop_id(
            leg.get("destination_stop")
        )

        if (
            pattern_id is None
            or origin_stop_id is None
            or destination_stop_id is None
        ):

            logger.error(
                "Public transport customer {} "
                "has an invalid public transport leg "
                "while waiting for boarding approval.".format(
                    self.agent.name
                )
            )

            self.agent.clear_current_vehicle()
            self.clear_candidate_transport()

            self.set_next_state(
                CUSTOMER_WAITING
            )

            return

        expected_vehicle = self._bare_jid(
            vehicle_id
        )

        while True:

            msg = await self.receive(
                timeout=3600
            )

            if not msg:
                continue

            protocol = msg.get_metadata(
                "protocol"
            )

            performative = msg.get_metadata(
                "performative"
            )

            sender = self._bare_jid(
                msg.sender
            )

            if (
                protocol != REQUEST_PROTOCOL
                or sender != expected_vehicle
            ):
                continue

            if performative not in (
                ACCEPT_PERFORMATIVE,
                REFUSE_PERFORMATIVE,
            ):
                continue

            try:

                content = (
                    json.loads(msg.body)
                    if msg.body
                    else {}
                )

            except (
                json.JSONDecodeError,
                TypeError
            ):

                logger.error(
                    "Public transport customer {} "
                    "received an invalid boarding "
                    "response from vehicle {}.".format(
                        self.agent.name,
                        expected_vehicle
                    )
                )

                continue

            if (
                content.get("request_type")
                != "public_transport_board"
            ):
                continue

            if not self.message_matches_service(
                content
            ):
                logger.warning(
                    "Public transport customer {} ignored a stale or "
                    "uncorrelated boarding response from {}.".format(
                        self.agent.name,
                        expected_vehicle,
                    )
                )
                continue

            if (
                content.get("pattern_id")
                != pattern_id

                or content.get("origin_stop")
                != origin_stop_id

                or content.get("destination_stop")
                != destination_stop_id
            ):

                logger.warning(
                    "Public transport customer {} "
                    "received a boarding response from "
                    "{} that does not match the current "
                    "leg.".format(
                        self.agent.name,
                        expected_vehicle
                    )
                )

                continue

            if (
                performative
                == ACCEPT_PERFORMATIVE
            ):

                logger.info(
                    "Public transport customer {} "
                    "boarded vehicle {} at stop {} "
                    "on pattern {} towards {}.".format(
                        self.agent.name,
                        expected_vehicle,
                        origin_stop_id,
                        pattern_id,
                        destination_stop_id
                    )
                )

                boarding_key = content.get(
                    "boarding_key"
                )

                self.mark_service_assigned(
                    expected_vehicle,
                    boarding_key=boarding_key,
                )

                self.start_service(
                    expected_vehicle,
                    extra_details={
                        "pattern_id": pattern_id,
                        "route_id": leg.get("route_id"),
                        "mode": leg.get("mode"),
                        "origin_stop": origin_stop_id,
                        "destination_stop": destination_stop_id,
                        "leg_index": self.agent.current_leg_index,
                    },
                )

                self.agent.set_current_vehicle(
                    vehicle_id
                )

                self.agent.clear_waiting_pattern_id()

                self.agent.clear_current_stop()

                self.set_next_state(
                    CUSTOMER_IN_TRANSPORT
                )

                return

            if (
                performative
                == REFUSE_PERFORMATIVE
            ):

                logger.info(
                    "Public transport customer {} "
                    "was refused boarding by vehicle {} "
                    "at stop {} on pattern {}.".format(
                        self.agent.name,
                        expected_vehicle,
                        origin_stop_id,
                        pattern_id
                    )
                )

                self.clear_candidate_transport(
                    expected_vehicle
                )

                self.agent.clear_current_vehicle()

                self.set_next_state(
                    CUSTOMER_WAITING
                )

                return


class PublicTransportCustomerInTransportState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Track one accepted PublicTransport vehicle leg until the requested
    alighting stop is reached.

    The current vehicle, Pattern, origin stop, and destination stop are derived
    from the selected journey leg.

    The customer waits reactively for
    ``public_transport_arrival`` from the exact boarded vehicle.

    Arrival messages must match the active canonical service and additionally
    identify the expected vehicle, Pattern, and destination stop.

    On valid arrival, the customer's physical position is synchronized with
    the destination-stop position when available.

    The destination stop becomes the new logical current stop, provisional
    vehicle and waiting-Pattern state are cleared, and the completed
    PublicTransport leg advances exactly once.

    Execution then returns to ``CUSTOMER_WAITING_TO_MOVE`` so the next walking,
    transfer, PublicTransport, or terminal leg can be dispatched.

    Vehicle arrival does not emit ``service_completed`` because the complete
    door-to-door Journey may still contain additional legs.
    """
    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            CUSTOMER_IN_TRANSPORT
        )

    def _bare_jid(self, jid):
        """
        Normalize a JID for boarded-vehicle message correlation.

        Returns:
            str | None: Bare JID or None.
        """
        if jid is None:
            return None

        return str(jid).split("/")[0]

    async def run(self):
        """
        Wait for arrival at the destination stop of the current PublicTransport leg.

        Missing or inconsistent journey/vehicle state returns control to the
        central journey dispatcher.

        The state accepts only REQUEST_PROTOCOL / INFORM_PERFORMATIVE messages from
        the exact boarded vehicle.

        A valid message must contain ``public_transport_arrival``, match the
        canonical service, identify the expected vehicle and Pattern, and report
        the current leg's destination stop.

        On arrival, the customer position is synchronized with the destination-stop
        coordinates when available.

        The destination stop is stored as the new logical current stop, vehicle and
        waiting-Pattern execution state are cleared, ``current_leg_index`` advances
        once, and execution returns to ``CUSTOMER_WAITING_TO_MOVE``.
        """
        leg = self.agent.get_current_leg()

        if leg is None:

            logger.error(
                "Public transport customer {} "
                "has no current leg while travelling.".format(
                    self.agent.name
                )
            )

            self.agent.clear_current_vehicle()

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if leg.get("type") != "public_transport":

            logger.error(
                "Public transport customer {} "
                "is in transport but current leg "
                "type is {}.".format(
                    self.agent.name,
                    leg.get("type")
                )
            )

            self.agent.clear_current_vehicle()

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        vehicle_id = (
            self.agent.get_current_vehicle()
        )

        if vehicle_id is None:

            logger.error(
                "Public transport customer {} "
                "is in transport without "
                "current_vehicle.".format(
                    self.agent.name
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        pattern_id = (
            leg.get("pattern_id")
        )

        origin_stop = (
            leg.get("origin_stop")
        )

        destination_stop = (
            leg.get("destination_stop")
        )

        origin_stop_id = self.get_stop_id(
            origin_stop
        )

        destination_stop_id = self.get_stop_id(
            destination_stop
        )

        if (
            pattern_id is None
            or origin_stop_id is None
            or destination_stop_id is None
        ):

            logger.error(
                "Public transport customer {} "
                "has an invalid public transport leg "
                "while travelling.".format(
                    self.agent.name
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        expected_vehicle = self._bare_jid(
            vehicle_id
        )

        while True:

            msg = await self.receive(
                timeout=3600
            )

            if not msg:
                continue

            protocol = msg.get_metadata(
                "protocol"
            )

            performative = msg.get_metadata(
                "performative"
            )

            sender = self._bare_jid(
                msg.sender
            )

            if (
                protocol != REQUEST_PROTOCOL
                or performative
                != INFORM_PERFORMATIVE
            ):
                continue

            if sender != expected_vehicle:
                continue

            try:

                content = (
                    json.loads(msg.body)
                    if msg.body
                    else {}
                )

            except (
                json.JSONDecodeError,
                TypeError
            ):

                logger.warning(
                    "Public transport customer {} "
                    "received invalid transport message "
                    "from vehicle {}.".format(
                        self.agent.name,
                        expected_vehicle
                    )
                )

                continue

            if (
                content.get("request_type")
                != "public_transport_arrival"
            ):
                continue

            if not self.message_matches_service(
                content
            ):
                logger.warning(
                    "Public transport customer {} ignored a stale or "
                    "uncorrelated arrival from {}.".format(
                        self.agent.name,
                        expected_vehicle,
                    )
                )
                continue

            informed_vehicle = (
                self._bare_jid(
                    content.get("vehicle_id")
                )
            )

            informed_pattern = (
                content.get("pattern_id")
            )

            informed_stop = (
                content.get("stop")
            )

            if informed_vehicle != expected_vehicle:
                continue

            if informed_pattern != pattern_id:
                continue

            if informed_stop != destination_stop_id:
                continue

            destination_position = (
                destination_stop.get(
                    "position"
                )
            )

            if destination_position is not None:
                await self.agent.set_position(
                    destination_position
                )

            self.agent.set_current_stop(
                destination_stop_id
            )

            self.agent.clear_current_vehicle()

            self.agent.clear_waiting_pattern_id()

            self.agent.advance_leg()

            logger.info(
                "Public transport customer {} "
                "left vehicle {} at stop {} "
                "after completing pattern {}.".format(
                    self.agent.name,
                    expected_vehicle,
                    destination_stop_id,
                    pattern_id
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return


class PublicTransportCustomerInDestState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Terminal successful state of the complete PublicTransport door-to-door
    Journey.

    This state is reached only after the central dispatcher determines that all
    selected journey legs have been consumed.

    On entry, any residual customer pending movement is completed
    defensively.

    The customer then emits the unique public ``service_completed`` event with
    final journey summary metadata including transfers, walking distance,
    PublicTransport stops, and total leg count.

    Vehicle execution state, waiting Pattern, pedestrian destination, and the
    terminal service context are cleared in ``run()``.

    No next state is selected, allowing the PublicTransport FSM to terminate
    naturally and invoke modal-completion orchestration.
    """

    async def on_start(self):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_IN_DEST
        )

        journey = (
            self.agent.get_journey()
            or {}
        )

        logger.info(
            "Public transport customer {} "
            "completed its journey at {}.".format(
                self.agent.name,
                self.agent.get_position()
            )
        )

        self.complete_pending_movement()

        self.complete_service(
            extra_details={
                "final_position": self.agent.get_position(),
                "transfers": journey.get("transfers", 0),
                "walking_distance_m": journey.get("walking_distance", 0),
                "public_transport_stops": journey.get("public_transport_stops", 0),
                "legs": len(journey.get("legs", [])),
            },
        )

    async def run(self):
        """
        Clear residual PublicTransport execution state and terminate the successful
        FSM path.

        The canonical service context is cleared only after terminal processing has
        occurred.
        """
        self.agent.clear_current_vehicle()

        self.agent.clear_waiting_pattern_id()

        self.agent.pedestrian_dest = None

        self.clear_service_context()

        return


class FSMPublicTransportCustomerStrategyBehaviour(
    FSMSimfleetBehaviour
):
    """
    Finite-state customer strategy for door-to-door PublicTransport journeys.

    The FSM coordinates nine states covering:

    - FleetManager discovery and journey planning;
    - walking access, transfer, and final walking legs;
    - stop-queue admission;
    - reactive waiting for compatible vehicles;
    - boarding negotiation;
    - active PublicTransport travel;
    - successful and failed terminal outcomes.

    One persistent ``service_id`` spans the complete Journey even when several
    vehicles and transfers are used.

    Public ``service_assigned`` may occur once per accepted boarding and is
    emitted by the corresponding vehicle.

    The customer emits ``service_started`` only for the first successful
    boarding, and owns the unique ``service_completed`` or ``service_failed``
    terminal for the complete door-to-door Journey.

    Successful and failed terminal states have no active outgoing transition in
    the current implementation. Normal FSM termination invokes
    ``notify_modal_completion()`` after generic strategy-end instrumentation.

    MultiModalCustomerAgent uses that notification together with customer
    status to distinguish successful modal completion from fatal modal failure.

    Generic FSM lifecycle instrumentation is inherited from
    FSMSimfleetBehaviour.
    """

    async def on_end(self):
        """
        Finalize the PublicTransport customer FSM and notify modal orchestration.

        Generic FSM end instrumentation runs first.

        The modal-completion hook is then triggered so MultiModalCustomerAgent may
        inspect the terminal customer status and either continue the itinerary
        after success or stop it after modal failure.
        """
        await super().on_end()
        self.agent.notify_modal_completion()

    def setup(
        self
    ):
        """
        Register PublicTransport customer states and permitted transitions.
        """
        #
        # States
        #

        self.add_state(
            CUSTOMER_WAITING_TO_MOVE,
            PublicTransportCustomerWaitingToMoveState(),
            initial=True
        )

        self.add_state(
            CUSTOMER_WAITING_FOR_JOURNEY,
            PublicTransportCustomerWaitingForJourneyState()
        )

        self.add_state(
            CUSTOMER_MOVING_TO_DEST,
            PublicTransportCustomerMovingToDestState()
        )

        self.add_state(
            CUSTOMER_IN_STOP,
            PublicTransportCustomerInStopState()
        )

        self.add_state(
            CUSTOMER_WAITING,
            PublicTransportCustomerWaitingState()
        )

        self.add_state(
            CUSTOMER_WAITING_FOR_APPROVAL,
            PublicTransportCustomerWaitingForApprovalState()
        )

        self.add_state(
            CUSTOMER_IN_TRANSPORT,
            PublicTransportCustomerInTransportState()
        )

        self.add_state(
            CUSTOMER_IN_DEST,
            PublicTransportCustomerInDestState()
        )

        self.add_state(
            CUSTOMER_JOURNEY_FAILED,
            PublicTransportCustomerJourneyFailedState()
        )

        #
        # CUSTOMER_WAITING_TO_MOVE
        #

        self.add_transition(
            CUSTOMER_WAITING_TO_MOVE,
            CUSTOMER_WAITING_TO_MOVE
        )

        self.add_transition(
            CUSTOMER_WAITING_TO_MOVE,
            CUSTOMER_WAITING_FOR_JOURNEY
        )

        self.add_transition(
            CUSTOMER_WAITING_TO_MOVE,
            CUSTOMER_MOVING_TO_DEST
        )

        self.add_transition(
            CUSTOMER_WAITING_TO_MOVE,
            CUSTOMER_IN_STOP
        )

        self.add_transition(
            CUSTOMER_WAITING_TO_MOVE,
            CUSTOMER_IN_DEST
        )

        #
        # CUSTOMER_WAITING_FOR_JOURNEY
        #

        self.add_transition(
            CUSTOMER_WAITING_FOR_JOURNEY,
            CUSTOMER_WAITING_TO_MOVE
        )

        self.add_transition(
            CUSTOMER_WAITING_FOR_JOURNEY,
            CUSTOMER_JOURNEY_FAILED
        )

        #
        # CUSTOMER_MOVING_TO_DEST
        #

        self.add_transition(
            CUSTOMER_MOVING_TO_DEST,
            CUSTOMER_MOVING_TO_DEST
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_DEST,
            CUSTOMER_WAITING_TO_MOVE
        )

        #
        # CUSTOMER_IN_STOP
        #

        self.add_transition(
            CUSTOMER_IN_STOP,
            CUSTOMER_IN_STOP
        )

        self.add_transition(
            CUSTOMER_IN_STOP,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_IN_STOP,
            CUSTOMER_WAITING_TO_MOVE
        )

        #
        # CUSTOMER_WAITING
        #

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING_FOR_APPROVAL
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING_TO_MOVE
        )

        #
        # CUSTOMER_WAITING_FOR_APPROVAL
        #

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
            CUSTOMER_IN_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_WAITING_FOR_APPROVAL,
            CUSTOMER_WAITING_TO_MOVE
        )

        #
        # CUSTOMER_IN_TRANSPORT
        #

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_IN_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_WAITING_TO_MOVE
        )

        #
        # Successful terminal state
        #

        self.add_transition(
            CUSTOMER_IN_DEST,
            CUSTOMER_IN_DEST
        )

        #
        # Failed terminal state
        #

        self.add_transition(
            CUSTOMER_JOURNEY_FAILED,
            CUSTOMER_JOURNEY_FAILED
        )
