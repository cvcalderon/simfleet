import math
from uuid import uuid4
import json

from loguru import logger
from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from spade.behaviour import State
from spade.message import Message

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    PROPOSE_PERFORMATIVE
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination
)
from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_MOVING_TO_CUSTOMER, \
    TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_IN_CUSTOMER_PLACE, TRANSPORT_MOVING_TO_DESTINATION, \
    TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_MOVING_TO_STATION, TRANSPORT_IN_STATION_PLACE, \
    TRANSPORT_IN_WAITING_LIST, TRANSPORT_NEEDS_CHARGING, TRANSPORT_CHARGING, CUSTOMER_IN_TRANSPORT, CUSTOMER_IN_DEST, \
    TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_MOVING_TO_RETURN


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class ElectricTaxiStrategyBehaviour(State):
    """
    Base SPADE State shared by the ElectricTaxi transport FSM.

    The class combines two independent canonical lifecycles:

    Mobility service lifecycle
        Correlates customer negotiation and execution through ``service_id``
        and emits schema-1.0 service and movement events.

    Charging lifecycle
        Correlates one charging attempt through an independent ``charging_id``
        and emits ``charging_arrived``, ``charging_started``, and
        ``charging_completed``.

    It also provides common ElectricTaxi messaging helpers, charging-station
    selection context, autonomy-related helpers, and Taxi-style return-point
    requests.

    ``service_id`` and ``charging_id`` belong to different metric entities and
    must never be substituted for one another.

    Although its service contract closely mirrors TaxiStrategyBehaviour, this
    class currently inherits directly from SPADE State and maintains its own
    implementation rather than inheriting the Taxi strategy class.

    This class is an FSM state helper; state registration and transitions
    belong to FSMElectricTaxiBehaviour.
    """


    METRICS_MODALITY = "electric_taxi"
    _METRICS_SERVICE_CONTEXT_ATTR = "_metrics_service_context"
    _METRICS_PENDING_MOVEMENT_ATTR = "_metrics_pending_movement"
    _METRICS_MOVEMENT_PHASES = {"approach", "service", "auxiliary"}

    def _metrics_modality(self):
        """
        Return the canonical ElectricTaxi mobility modality.

        Returns:
            str: ``"electric_taxi"``.

        Raises:
            ValueError: If the concrete strategy does not define
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
        Return the active canonical mobility-service context.

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
        Create and store one canonical ElectricTaxi service context.

        Transport-side ElectricTaxi normally reuses the ``service_id`` created by
        the customer. A new identifier may be generated locally only when
        ``emit_requested`` is True.

        Existing unfinished or uncleared contexts are never silently replaced.

        Args:
            service_id: Existing logical customer-service identifier.
            user_id: Customer JID.
            transport_id: Optional ElectricTaxi JID.
            origin: Customer-service origin.
            destination: Customer-service destination.
            emit_requested (bool): Whether this agent owns and emits the initial
                ``service_requested`` event.

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
        Return the matching active service context or create a new one.

        An explicitly supplied service identifier must match any existing active
        context.

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
        Clear a terminal ElectricTaxi service context when no movement remains.

        Unfinished services and contexts that still own pending movement are
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
        Build canonical identifiers shared by mobility-service events.

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
        Add canonical service identifiers to a copied outgoing payload.

        Args:
            content (dict | None): Existing payload.
            context (dict | None): Service context.
            transport_id: Optional ElectricTaxi identifier to bind.

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
        Validate that a message belongs to the expected ElectricTaxi service.

        Service ID, modality, user identifier, and any established transport
        identifier must match the active service context.

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
        Mirror ElectricTaxi assignment state without emitting a canonical event.

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
        Mirror mobility-service start without emitting a canonical event.

        Returns:
            bool: True when the service context can be marked started.
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

    def mark_service_completed(self):
        """
        Mirror successful completion already emitted by the customer.

        No second ``service_completed`` event is emitted.

        Returns:
            bool: True when completion is valid or already mirrored.
        """
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
        """
        Mark the mobility service assigned and emit ``service_assigned`` once.

        Returns:
            bool: True when assignment was newly recorded and emitted.
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
        Mark customer transport as started and emit ``service_started``.

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
        Mark the service completed and emit ``service_completed``.

        The helper requires an already started service. The current ElectricTaxi
        FSM normally mirrors customer-owned completion through
        ``mark_service_completed()``.

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
        Mark the active mobility service failed and emit ``service_failed``.

        Failure may occur before or after service start, but only one terminal
        status may be recorded.

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
        Register one planned ElectricTaxi movement for deferred metric emission.

        ``movement_completed`` is emitted only after physical completion is
        confirmed.

        Canonical phases are ``approach``, ``service``, and ``auxiliary``.
        ``require_service=False`` allows operational movements such as travel to a
        charging station or Taxi return movement without requiring an active
        customer service.

        If a service context does exist, its identifiers are still propagated even
        when ``require_service`` is False.

        Returns:
            bool: True when the pending movement was registered.

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
        Emit the pending movement as canonical ``movement_completed``.

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
        Discard incomplete planned movement without emitting a movement metric.

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


    _METRICS_CHARGING_CONTEXT_ATTR = "_metrics_charging_context"

    def get_charging_context(self):
        """
        Return the active ElectricTaxi charging-session context.

        Returns:
            dict | None: Current charging context.
        """
        return getattr(
            self.agent,
            self._METRICS_CHARGING_CONTEXT_ATTR,
            None,
        )

    def create_charging_context(self, station_id, charging_id=None):
        """
        Create one independent ElectricTaxi charging-session context.

        Charging sessions use ``charging_id`` rather than the customer
        ``service_id``. When no identifier is supplied, a new UUID is generated.

        The context binds one ElectricTaxi transport to one charging station and
        tracks three ordered milestones:

        - arrived;
        - started;
        - completed.

        An unfinished charging context is never silently replaced.

        Args:
            station_id: Charging-station JID.
            charging_id (str | None): Optional existing charging-session ID.

        Returns:
            dict | None: Created or reusable charging context.
        """
        current = self.get_charging_context()
        if current is not None:
            requested_id = str(charging_id) if charging_id is not None else None
            if (
                not current.get("completed")
                and (
                    requested_id is None
                    or current.get("charging_id") == requested_id
                )
            ):
                return current
            logger.warning(
                "Agent[{}]: Refusing to replace charging context [{}] before "
                "it is cleared.".format(
                    self.agent.name,
                    current.get("charging_id"),
                )
            )
            return None
        if station_id is None:
            return None
        context = {
            "charging_id": str(charging_id) if charging_id is not None else str(uuid4()),
            "modality": self.METRICS_MODALITY,
            "transport_id": self.agent.bare_jid(self.agent.jid),
            "station_id": self.agent.bare_jid(station_id),
            "arrived": False,
            "started": False,
            "completed": False,
        }
        setattr(
            self.agent,
            self._METRICS_CHARGING_CONTEXT_ATTR,
            context,
        )
        return context

    def clear_charging_context(self):
        """
        Clear a completed charging-session context.

        Unfinished sessions cannot be cleared through this normal completion path.

        Returns:
            bool: True when no charging context remains.
        """
        context = self.get_charging_context()
        if context is None:
            return True
        if not context.get("completed"):
            logger.warning(
                "Agent[{}]: Refusing to clear unfinished charging session [{}].".format(
                    self.agent.name,
                    context.get("charging_id"),
                )
            )
            return False
        setattr(
            self.agent,
            self._METRICS_CHARGING_CONTEXT_ATTR,
            None,
        )
        return True

    def abandon_charging_context(self):
        """
        Abandon an unfinished internal charging context without emitting failure.

        Any charging milestones already emitted remain in the event log and are
        therefore reconstructed by MobilityStatisticsClass as an ``unfinished``
        charging session.

        A later charging attempt receives a new ``charging_id``.

        Returns:
            bool: True after the internal context has been abandoned.
        """
        context = self.get_charging_context()
        if context is None:
            return True
        setattr(self.agent, self._METRICS_CHARGING_CONTEXT_ATTR, None)
        return True

    def _charging_event_details(self, context=None):
        """
        Build canonical identifiers shared by charging lifecycle events.

        Args:
            context (dict | None): Charging-session context.

        Returns:
            dict | None: Modality, charging ID, transport ID, and station ID.
        """
        context = context or self.get_charging_context()
        if context is None:
            return None
        return {
            "modality": context["modality"],
            "charging_id": context["charging_id"],
            "transport_id": context["transport_id"],
            "station_id": context["station_id"],
        }

    def charging_message_matches_context(
        self,
        content,
        sender=None
    ):
        """
        Validate that a station message belongs to the active charging session.

        When an XMPP sender is available, its bare JID is authoritative and must
        match the station stored in the charging context.

        When sender information is unavailable, ``station_id`` in the decoded
        payload is used as a backward-compatible fallback.

        If neither sender nor payload station identifier is available, the current
        compatibility behaviour accepts the message.

        Args:
            content: Decoded station payload.
            sender: Optional XMPP sender JID.

        Returns:
            bool: True when station identity is compatible with the active
            charging context.
        """

        context = self.get_charging_context()

        if context is None or not isinstance(content, dict):
            return False

        expected_station = context.get("station_id")

        #
        # The XMPP sender is the authoritative station identity.
        #
        if sender is not None:
            return (
                self.agent.bare_jid(sender)
                == expected_station
            )

        #
        # Backward-compatible fallback for messages where
        # sender is not available to the caller.
        #
        station_id = content.get("station_id")

        if station_id is None:
            return True

        return (
            self.agent.bare_jid(station_id)
            == expected_station
        )

    def charging_arrived(self):
        """
        Record physical station arrival and emit ``charging_arrived`` once.

        Returns:
            bool: True when the arrival milestone was newly emitted.
        """
        context = self.get_charging_context()
        if context is None or context.get("arrived"):
            return False
        context["arrived"] = True
        self.agent.events_store.emit(
            event_type="charging_arrived",
            details=self._charging_event_details(context),
        )
        return True

    def charging_started(self):
        """
        Record charging-service start and emit ``charging_started`` once.

        Charging can start only after ``charging_arrived`` has been recorded.

        Returns:
            bool: True when the start milestone was newly emitted.
        """
        context = self.get_charging_context()
        if context is None or context.get("started") or not context.get("arrived"):
            return False
        context["started"] = True
        self.agent.events_store.emit(
            event_type="charging_started",
            details=self._charging_event_details(context),
        )
        return True

    def charging_completed(self):
        """
        Record charging completion and emit ``charging_completed`` once.

        Completion is accepted only after ``charging_started``.

        Returns:
            bool: True when the completion milestone was newly emitted.
        """
        context = self.get_charging_context()
        if context is None or context.get("completed") or not context.get("started"):
            return False
        context["completed"] = True
        self.agent.events_store.emit(
            event_type="charging_completed",
            details=self._charging_event_details(context),
        )
        return True


    _METRICS_PENDING_OFFER_ATTR = "_metrics_pending_offer"
    _METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR = "_metrics_active_message_context"

    def _validate_metrics_service_request(self, content):
        """
        Validate a schema-1.0 ElectricTaxi request before proposing service.

        Required fields are ``service_id``, ``modality``, ``user_id``,
        ``customer_id``, ``origin``, and ``dest``.

        User and customer identifiers must represent the same bare JID, modality
        must be ``electric_taxi``, and the initial request must not already contain
        a transport assignment.

        Returns:
            bool: True when the request may enter proposal negotiation.
        """
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
        """
        Store one validated customer request while the Taxi proposal is pending.

        The pending context preserves service correlation and binds this Taxi as
        the proposed ``transport_id`` without yet creating the active service
        lifecycle context.

        Args:
            content (dict): Validated customer request.

        Returns:
            dict | None: Stored pending-offer context, or None when validation
            fails.
        """
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
        """
        Return the Taxi proposal currently awaiting customer resolution.

        Returns:
            dict | None: Pending-offer context.
        """
        return getattr(self.agent, self._METRICS_PENDING_OFFER_ATTR, None)

    def clear_pending_offer(self):
        """
        Clear the currently stored pending Taxi proposal.

        Returns:
            bool: True after the pending-offer context has been cleared.
        """
        setattr(self.agent, self._METRICS_PENDING_OFFER_ATTR, None)
        return True

    def pending_offer_message_details(self):
        """
        Build canonical identifiers propagated with a Taxi proposal.

        Returns:
            dict | None: Service, modality, user, and proposed transport
            identifiers.
        """
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
        """
        Validate a customer response against the currently pending Taxi proposal.

        Service, modality, user, transport, and customer identifiers must all
        match the stored pending context.

        Args:
            content: Decoded customer response.

        Returns:
            bool: True when the response resolves the current proposal.
        """
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
        """
        Promote a matching pending proposal into active message context.

        The pending context is copied into the active context and then removed from
        pending storage.

        Args:
            content: Customer response expected to match the pending proposal.

        Returns:
            dict | None: Newly active message context.
        """
        if not self.message_matches_pending_offer(content):
            return None
        pending = dict(self.get_pending_offer())
        setattr(self.agent, self._METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR, pending)
        self.clear_pending_offer()
        return pending

    def get_active_message_context(self):
        """
        Return canonical message-correlation context for the accepted service.

        Returns:
            dict | None: Active message context.
        """
        return getattr(
            self.agent,
            self._METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR,
            None,
        )

    def active_service_message_details(self):
        """
        Build canonical identifiers for messages belonging to the active service.

        Returns:
            dict | None: Service, modality, user, and transport identifiers.
        """
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
        """
        Add active service-correlation identifiers to an outgoing payload.

        When no active message context exists, a copy of the supplied payload is
        returned unchanged.

        Args:
            content (dict | None): Existing outgoing payload.

        Returns:
            dict: Copied payload with active identifiers when available.
        """
        details = self.active_service_message_details()
        if details is None:
            return dict(content or {})
        result = dict(content or {})
        result.update(details)
        return result

    def message_matches_active_service(self, content):
        """
        Validate that a message belongs to the currently accepted Taxi service.

        The message must match service ID, modality, user bare JID, and transport
        bare JID.

        Args:
            content: Decoded message payload.

        Returns:
            bool: True when the message belongs to the active service.
        """
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
        """
        Clear message-correlation state for the accepted Taxi service.

        Returns:
            bool: True after the active context has been cleared.
        """
        setattr(self.agent, self._METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR, None)
        return True

    async def on_start(self):
        """
        Log entry into one concrete ElectricTaxi FSM state.

        Generic FSM lifecycle instrumentation belongs to
        FSMElectricTaxiBehaviour rather than to individual State transitions.
        """
        # await super().on_start()
        logger.debug(
            "Agent[{}]: Strategy {} started.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def on_end(self):
        """
        Log exit from one concrete ElectricTaxi FSM state.
        """
        # await super().on_start()
        logger.debug(
            "Agent[{}]: Strategy {} finished.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def go_to_the_station(self, station_id, dest):
        """
        Bind the selected charging station as the ElectricTaxi current station.

        This helper does not perform physical movement and does not modify
        autonomy. Route execution is started separately by the charging FSM state
        through ``move_to()``.

        Args:
            station_id: Selected charging-station JID.
            dest: Station coordinates retained by the caller for route planning.
        """
        logger.info(
            "Agent[{}]: On route to station [{}]".format(
                self.agent.name,
                station_id
            )
        )

        self.agent.set_current_station(
            station_id
        )

    def check_and_decrease_autonomy(
        self,
        customer_orig,
        customer_dest
    ):
        """
        Validate and reserve autonomy for one complete customer service.

        Required distance is estimated as current position to customer origin plus
        customer origin to destination using ChargeableMixin's straight-line
        distance model.

        When sufficient autonomy remains above the configured reserve, the full
        estimated service distance is immediately deducted.

        Args:
            customer_orig: Customer pickup coordinates.
            customer_dest: Customer destination coordinates.

        Returns:
            bool: True when sufficient autonomy existed and was deducted.
        """
        travel_km = self.agent.calculate_service_km(
            customer_orig,
            customer_dest
        )

        if not self.agent.has_enough_autonomy_km(
            travel_km
        ):
            return False

        self.agent.decrease_autonomy_km(
            travel_km
        )

        return True

    async def drop_station(self):
        """
        Clear ElectricTaxi charging-station assignment state.

        Both the current station and cached nearby-station selection are removed.
        """

        logger.debug(
            "Agent[{}]: The agent has dropped the station [{}].".format(
                self.agent.agent_id,
                self.agent.get_current_station()
            )
        )

        self.agent.clear_current_station()
        self.agent.clear_nearby_station()

    async def request_access_station(self, station_id, content):
        """
        Request access to one charging-station service.

        A REQUEST_PROTOCOL / REQUEST_PERFORMATIVE message containing the supplied
        service payload is sent directly to the selected station.

        Args:
            station_id: Charging-station JID.
            content (dict | None): Requested service data.
        """

        if content is None:
            content = {}
        reply = Message()
        reply.to = station_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", REQUEST_PERFORMATIVE)
        reply.body = json.dumps(content)
        logger.debug(
            "Agent[{}]: The agent requesting access to [{}]".format(
                self.agent.name,
                station_id,
                reply.body
            )
        )
        await self.send(reply)

    async def send_proposal(self, customer_id, content=None):
        """
        Send an ElectricTaxi service proposal to a customer.

        The message uses REQUEST_PROTOCOL / PROPOSE_PERFORMATIVE.

        Args:
            customer_id: Customer JID.
            content (dict | None): Proposal payload.
        """
        if content is None:
            content = {}
        logger.info(
            "Agent[{}]: The agent sent proposal to agent [{}]".format(self.agent.name, customer_id)
        )
        reply = Message()
        reply.to = customer_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", PROPOSE_PERFORMATIVE)
        reply.body = json.dumps(content)
        await self.send(reply)

    async def cancel_proposal(self, agent_id, content=None):
        """
        Cancels a previously sent proposal.

        Args:
            agent_id (str): The ID of the customer.
            content (dict, optional): Additional content for the cancellation. Defaults to None.
        """
        if content is None:
            content = {}
        content = self.add_active_service_identifiers(content)
        logger.info(
            "Agent[{}]: The agent sent cancel proposal to agent [{}]".format(
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
        """
        Request an operational ElectricTaxi return point from its FleetManager.

        A ``taxi_return`` REQUEST_PROTOCOL message containing the current position
        is sent to the registered FleetManager.

        If no FleetManager is configured, the request is skipped.
        """
        fleetmanager = self.agent.get_registration_fleet()

        if not fleetmanager:
            logger.warning(
                "Agent[{}]: No fleet manager configured for electric taxi return.".format(
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
        """
        Execute the concrete ElectricTaxi FSM state.

        Raises:
            NotImplementedError: When a concrete state does not provide an
                implementation.
        """
        raise NotImplementedError

# ==================================================================
# -------------------------End Behaviour----------------------------
# ==================================================================


################################################################
#                                                              #
#              Point Of Return Electric Taxi Strategy          #
#                                                              #
################################################################

class ElectricTaxiWaitingState(ElectricTaxiStrategyBehaviour):
    """
    Idle request-screening state of the standard ElectricTaxi FSM.

    The transport waits for canonical ElectricTaxi service requests.

    A valid REQUEST_PERFORMATIVE is stored as a pending offer. Before sending
    a proposal, the ElectricTaxi estimates whether its current autonomy is
    sufficient for travel from its current position to the customer origin and
    then to the requested destination.

    When autonomy is sufficient, a PROPOSE_PERFORMATIVE is sent and execution
    advances to ``TRANSPORT_WAITING_FOR_APPROVAL``.

    When autonomy is insufficient, the customer receives a cancellation of the
    unresolved proposal, the pending offer is cleared, and the FSM enters
    ``TRANSPORT_NEEDS_CHARGING``. No mobility service context has been created
    at this point, so no ``service_failed`` event is emitted.

    Missing, invalid, or unsupported messages remain in
    ``TRANSPORT_WAITING``.
    """
    async def on_start(self):
        """
        Enter ElectricTaxi request waiting and mark the operational status as
        ``TRANSPORT_WAITING``.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING

    async def run(self):
        """
        Validate one ElectricTaxi request and screen it against available autonomy.

        Valid requests are stored as pending offers before autonomy is evaluated.

        Sufficient autonomy sends the normal service proposal and advances to
        approval waiting. Insufficient autonomy cancels the unresolved proposal,
        clears pending negotiation state, and enters the charging circuit.

        No customer-service lifecycle is created until a later valid
        ACCEPT_PERFORMATIVE.
        """
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


            if not self.agent.has_enough_autonomy_for_service(
                content["origin"],
                content["dest"]
            ):


                await self.cancel_proposal(
                    content["customer_id"],
                    self.pending_offer_message_details(),
                )
                self.clear_pending_offer()
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return
            else:


                await self.send_proposal(
                    content["customer_id"],
                    self.pending_offer_message_details(),
                )
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
                return
        else:
            self.set_next_state(TRANSPORT_WAITING)
            return

class ElectricTaxiNeedsChargingState(ElectricTaxiStrategyBehaviour):
    """
    Discover a charging station and start travel toward it.

    Entering the state marks the ElectricTaxi busy so it cannot accept a new
    customer service while charging is required.

    When no usable station list is available, station positions are requested
    for the configured charging ``service_type`` and the state retries.

    From the available stations, one nearby station is selected and stored.
    A new independent charging context is created before physical travel so the
    same ``charging_id`` can later correlate arrival, charging start, and
    charging completion.

    Route-based travel to the station is registered as canonical
    ``phase="auxiliary"`` movement. Physical autonomy is reduced using
    ChargeableMixin's straight-line distance estimate to the station.

    Successful route planning advances to
    ``TRANSPORT_MOVING_TO_STATION``.

    If the ElectricTaxi is already at the station, a zero-distance auxiliary
    movement and ``charging_arrived`` are emitted immediately, station access
    is requested, and execution advances directly to
    ``TRANSPORT_IN_STATION_PLACE``.

    Route or unexpected setup failure discards any incomplete movement,
    abandons the current charging attempt, clears station selection, and
    retries from ``TRANSPORT_NEEDS_CHARGING``.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_NEEDS_CHARGING
        self.agent.set_busy()

    async def run(self):
        """
        Resolve one charging station and initiate travel toward it.

        Missing station candidates keep the FSM in
        ``TRANSPORT_NEEDS_CHARGING``.

        Once a station is selected, a charging context is established before
        movement begins. The OSRM-resolved route distance is stored for canonical
        movement metrics, while autonomy consumption uses the ChargeableMixin
        geographic-distance estimate.

        Successful travel setup enters ``TRANSPORT_MOVING_TO_STATION``.
        Immediate physical coincidence with the station records arrival and
        requests station access directly.

        Failed route setup abandons this charging attempt and retries station
        selection.
        """
        if self.agent.get_stations() is None or self.agent.get_number_stations() < 1:
            logger.info(
                "Agent[{}]: The agent looking for a station.".format(
                    self.agent.name
                )
            )
            stations = await self.agent.get_list_agent_position(
                self.agent.service_type,
                self.agent.get_stations()
            )
            self.agent.set_stations(stations)

            if not stations:
                await self.agent.sleep(1)

            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

        nearby_station_dest = self.agent.nearst_agent(
            self.agent.get_stations(),
            self.agent.get_position()
        )

        if nearby_station_dest is None:
            logger.warning(
                "Agent[{}]: No charging station available.".format(
                    self.agent.name
                )
            )

            await self.agent.sleep(1)

            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

        self.agent.set_nearby_station(nearby_station_dest)
        station_id = self.agent.get_nearby_station_id()
        station_position = self.agent.get_nearby_station_position()
        logger.info("Agent[{}]: The agent selected station [{}].".format(self.agent.name, station_id))

        # A charging_id is independent from service_id. It is created before travel
        # so the same session is used for physical arrival/start/completion.
        if self.get_charging_context() is None:
            if self.create_charging_context(station_id) is None:
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return

        try:
            await self.go_to_the_station(station_id, station_position)
            travel_km = self.agent.calculate_distance_km(
                self.agent.get_position(), station_position
            )
            try:
                distance, _osrm_duration, _speed_based_duration = await self.agent.move_to(
                    station_position
                )
                if not self.set_pending_movement(
                    "auxiliary", distance, require_service=False
                ):
                    raise RuntimeError("Unable to register charging-station movement.")
                self.agent.decrease_autonomy_km(travel_km)
                self.agent.status = TRANSPORT_MOVING_TO_STATION
                self.set_next_state(TRANSPORT_MOVING_TO_STATION)
                return
            except AlreadyInDestination:
                self.set_pending_movement("auxiliary", 0, require_service=False)
                self.complete_pending_movement()
                self.charging_arrived()
                arguments = {
                    "transport_need": self.agent.max_autonomy_km - self.agent.current_autonomy_km
                }
                await self.request_access_station(
                    station_id,
                    {"service_name": self.agent.service_type, "object_type": "transport", "args": arguments},
                )
                self.agent.status = TRANSPORT_IN_STATION_PLACE
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
        except PathRequestException:
            logger.error("Agent[{}]: The agent could not get a path to station [{}].".format(self.agent.name, station_id))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name, e))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

class ElectricTaxiMovingToStationState(ElectricTaxiStrategyBehaviour):
    """
    Monitor physical ElectricTaxi movement toward the selected charging
    station.

    MovableMixin performs the actual movement. This state remains active until
    the selected station is reached.

    Confirmed arrival completes the pending auxiliary movement, ensures the
    charging context exists, emits ``charging_arrived``, and requests access
    to the charging service. The FSM then enters
    ``TRANSPORT_IN_STATION_PLACE``.

    Route or unexpected movement failure discards the incomplete movement,
    abandons the charging attempt, clears station state, and returns to
    ``TRANSPORT_NEEDS_CHARGING``.
    """
    async def on_start(self):
        """
        Enter charging-station movement monitoring.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_STATION

    async def _arrive_and_request(self):
        """
        Finalize physical station arrival and request charging-service access.

        The pending auxiliary movement is completed first. A charging context is
        created if necessary, ``charging_arrived`` is emitted exactly once, and a
        station request is sent containing the ElectricTaxi's remaining charging
        need.

        Successful setup advances to ``TRANSPORT_IN_STATION_PLACE``.

        Returns:
            bool: True when the arrival/access sequence was prepared successfully.
        """
        station_id = self.agent.get_current_station()
        self.complete_pending_movement()
        if self.get_charging_context() is None:
            if self.create_charging_context(station_id) is None:
                return False
        self.charging_arrived()
        arguments = {
            "transport_need": self.agent.max_autonomy_km - self.agent.current_autonomy_km
        }
        await self.request_access_station(
            station_id,
            {"service_name": self.agent.service_type, "object_type": "transport", "args": arguments},
        )
        self.agent.status = TRANSPORT_IN_STATION_PLACE
        self.set_next_state(TRANSPORT_IN_STATION_PLACE)
        return True

    async def run(self):
        """
        Monitor movement to the charging station until arrival or recovery.

        Incomplete movement remains in ``TRANSPORT_MOVING_TO_STATION`` after a
        one-second asynchronous wait.

        Arrival delegates to ``_arrive_and_request()``.

        An AlreadyInDestination path guarantees an explicit auxiliary movement,
        including zero distance when necessary, before the charging-arrival
        milestone is emitted.

        Route and unexpected failures abandon the current charging attempt and
        return to station selection.
        """
        try:
            if not self.agent.is_in_destination():
                await self.agent.sleep(1)
                self.set_next_state(TRANSPORT_MOVING_TO_STATION)
                return
            await self._arrive_and_request()
            return
        except AlreadyInDestination:
            if not self.complete_pending_movement():
                self.set_pending_movement("auxiliary", 0, require_service=False)
            await self._arrive_and_request()
            return
        except PathRequestException:
            logger.error("Agent[{}]: The agent could not complete the path to charging station.".format(self.agent.name))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        except Exception as e:
            logger.error("Unexpected charging-route error in [{}]: {}".format(self.agent.name, e))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

class ElectricTaxiInStationState(ElectricTaxiStrategyBehaviour):
    """
    Wait for charging-station admission after physical arrival.

    The ElectricTaxi has already emitted ``charging_arrived`` and requested
    station service before entering this state.

    A matching ACCEPT_PERFORMATIVE represents admission to the station service
    queue and advances to ``TRANSPORT_IN_WAITING_LIST``. It does not yet mean
    that charging has started.

    A matching REFUSE_PERFORMATIVE abandons the internal charging context,
    clears station selection, and returns to ``TRANSPORT_NEEDS_CHARGING``.

    Any already emitted ``charging_arrived`` milestone remains in the event log
    and is therefore reconstructed as an unfinished charging session.

    Timeouts, malformed payloads, unrelated messages, and mismatched station
    identities keep the ElectricTaxi in this state.
    """
    async def on_start(self):
        """
        Enter charging-station admission waiting.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_IN_STATION_PLACE

    async def run(self):
        """
        Process charging-station admission or refusal.

        ACCEPT from the station associated with the active charging context moves
        the ElectricTaxi into the station waiting list.

        REFUSE from that station abandons the current charging attempt and retries
        station discovery.

        Station identity is validated through
        ``charging_message_matches_context()`` before either transition.
        """
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_IN_STATION_PLACE)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_IN_STATION_PLACE)
            return
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                logger.warning("Agent[{}]: Ignoring charging-station ACCEPT with mismatched station.".format(self.agent.name))
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
            self.agent.status = TRANSPORT_IN_WAITING_LIST
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        if performative == REFUSE_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
            # The already-emitted arrival remains an unfinished charging session.
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        self.set_next_state(TRANSPORT_IN_STATION_PLACE)

class ElectricTaxiInWaitingListState(ElectricTaxiStrategyBehaviour):
    """
    Wait in the charging-station queue until service begins.

    A matching INFORM_PERFORMATIVE with ``serving`` records
    ``charging_started`` and advances to ``TRANSPORT_CHARGING``.

    A matching station refusal abandons the current charging attempt, clears
    station state, and returns to ``TRANSPORT_NEEDS_CHARGING``.

    Timeouts, malformed messages, mismatched stations, and non-serving informs
    keep the ElectricTaxi in the waiting list.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_IN_WAITING_LIST

    async def run(self):
        """
        Wait for station confirmation that charging service is starting.

        Matching ``INFORM_PERFORMATIVE`` with a truthy ``serving`` field must
        successfully emit ``charging_started`` before the FSM may enter
        ``TRANSPORT_CHARGING``.

        A matching refusal abandons the unfinished charging session and starts a
        new station-selection attempt.
        """
        msg = await self.receive(timeout=5)
        if not msg:
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        performative = msg.get_metadata("performative")
        if performative == INFORM_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                return
            if content.get("serving"):
                if not self.charging_started():
                    logger.warning("Agent[{}]: Charging start milestone could not be emitted.".format(self.agent.name))
                    self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                    return
                self.agent.status = TRANSPORT_CHARGING
                self.set_next_state(TRANSPORT_CHARGING)
                return
        elif performative == REFUSE_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                return
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        self.set_next_state(TRANSPORT_IN_WAITING_LIST)

class ElectricTaxiChargingState(ElectricTaxiStrategyBehaviour):
    """
    Wait for completion of the active ElectricTaxi charging service.

    A charging session completes only when the active station sends
    REQUEST_PROTOCOL / INFORM_PERFORMATIVE with a truthy ``charged`` field.

    The station identity must match the current charging context and
    ``charging_completed`` must be emitted successfully before operational
    cleanup occurs.

    Successful completion restores autonomy to its configured maximum, clears
    station state, and removes the completed charging context.

    If a Taxi return position was preserved before charging, the ElectricTaxi
    remains busy and resumes ``TRANSPORT_WAITING_FOR_RETURN``. Otherwise it
    becomes available and returns to ``TRANSPORT_WAITING``.

    Other messages and timeouts keep the vehicle in
    ``TRANSPORT_CHARGING``.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_CHARGING

    async def run(self):
        """
        Process completion of the active charging session.

        Valid station completion first emits ``charging_completed``. Full autonomy
        is then restored, station state is cleared, and the completed charging
        context is removed.

        A preserved return position resumes the interrupted Taxi return lifecycle;
        otherwise the ElectricTaxi becomes available for new customer requests.
        """
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_CHARGING)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_CHARGING)
            return
        protocol = msg.get_metadata("protocol")
        performative = msg.get_metadata("performative")
        if protocol == REQUEST_PROTOCOL and performative == INFORM_PERFORMATIVE and content.get("charged"):
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_CHARGING)
                return
            if not self.charging_completed():
                self.set_next_state(TRANSPORT_CHARGING)
                return
            self.agent.increase_full_autonomy_km()
            await self.drop_station()
            self.clear_charging_context()
            if self.agent.has_return_position():
                logger.info("Agent[{}]: Charging completed. Continuing pending return to {}.".format(self.agent.name, self.agent.get_return_position()))
                self.agent.status = TRANSPORT_WAITING_FOR_RETURN
                self.agent.set_busy()
                self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
                return
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return
        self.set_next_state(TRANSPORT_CHARGING)

class ElectricTaxiWaitingForApprovalState(ElectricTaxiStrategyBehaviour):
    """
    Resolve the customer response to a pending ElectricTaxi proposal.

    A matching ACCEPT_PERFORMATIVE promotes the pending offer to active
    message context and recalculates the complete estimated service distance
    from the ElectricTaxi's current position to pickup and then destination.

    If autonomy is no longer sufficient, the accepted proposal is cancelled,
    active message correlation is cleared, and the ElectricTaxi enters the
    charging circuit without creating a canonical mobility-service lifecycle.

    With sufficient autonomy, the state creates the transport-side service
    context, emits ``service_assigned``, binds the customer, starts approach
    movement, and reserves autonomy for the complete estimated customer trip.

    A matching REFUSE_PERFORMATIVE clears the pending proposal and returns to
    normal waiting.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING_FOR_APPROVAL

    async def run(self):
        """
        Process acceptance or refusal of the current ElectricTaxi proposal.

        Valid acceptance first recalculates the full estimated customer-service
        distance. Insufficient autonomy cancels the unresolved service and enters
        ``TRANSPORT_NEEDS_CHARGING`` without emitting ``service_assigned`` or
        ``service_failed``.

        When autonomy is sufficient:

        1. the canonical service context is created;
        2. ``service_assigned`` is emitted;
        3. the customer is bound to the ElectricTaxi;
        4. approach movement to the customer is requested;
        5. the complete estimated trip autonomy is deducted;
        6. the route is registered as pending ``phase="approach"`` movement.

        Successful setup advances to ``TRANSPORT_MOVING_TO_CUSTOMER``.

        If the ElectricTaxi is already at pickup, the same complete trip autonomy
        is deducted, an explicit zero-distance approach movement is emitted, and
        execution advances directly to ``TRANSPORT_ARRIVED_AT_CUSTOMER``.

        Approach-route or unexpected setup failure terminates the canonical
        service as failed and restores normal waiting according to the existing
        recovery path.
        """
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            if not self.message_matches_pending_offer(content):
                logger.warning("Agent[{}]: Ignoring stale or mismatched acceptance.".format(self.agent.name))
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
                return
            active = self.activate_pending_offer(content)
            customer_id = content["customer_id"]

            travel_km = self.agent.calculate_service_km(
                content["origin"],
                content["dest"],
            )

            if not self.agent.has_enough_autonomy_km(travel_km):
                await self.cancel_proposal(customer_id)
                self.clear_active_message_context()
                self.agent.set_busy()
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return
            try:
                context = self.create_service_context(
                    service_id=active["service_id"], user_id=active["user_id"],
                    transport_id=active["transport_id"], origin=active["origin"],
                    destination=active["destination"], emit_requested=False,
                )
                if context is None or not self.assign_service(self.agent.jid):
                    raise RuntimeError("Unable to establish Electric Taxi metrics service context.")
                self.agent.add_assigned_customer(
                    customer_id=customer_id, origin=content["origin"], dest=content["dest"]
                )
                self.agent.set_busy()
                await self.inform_customer(customer_id=customer_id, status=TRANSPORT_MOVING_TO_CUSTOMER)
                distance, _osrm_duration, _speed_based_duration = (
                    await self.agent.move_to(content["origin"])
                )

                self.agent.decrease_autonomy_km(travel_km)

                if not self.set_pending_movement("approach", distance):
                    raise RuntimeError(
                        "Unable to register Electric Taxi approach movement."
                    )

                self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER
                self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
                return
            except AlreadyInDestination:
                self.agent.decrease_autonomy_km(travel_km)

                self.set_pending_movement("approach", 0)
                self.complete_pending_movement()

                await self.inform_customer(
                    customer_id=customer_id,
                    status=TRANSPORT_IN_CUSTOMER_PLACE,
                )

                self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
                self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
                return
            except PathRequestException:
                self.fail_service("approach_route_failed")
                self.discard_pending_movement()
                await self.cancel_proposal(customer_id, {"terminal_status":"failed","failure_reason":"approach_route_failed"})
                self.clear_service_context(); self.clear_active_message_context()
                if self.agent.get("assigned_customer"):
                    self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING; self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING); return
            except Exception as e:
                logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name, e))
                self.fail_service("approach_unexpected_error")
                self.discard_pending_movement()
                await self.cancel_proposal(customer_id, {"terminal_status":"failed","failure_reason":"approach_unexpected_error"})
                self.clear_service_context(); self.clear_active_message_context()
                if self.agent.get("assigned_customer"):
                    self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING; self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING); return
        elif performative == REFUSE_PERFORMATIVE:
            if not self.message_matches_pending_offer(content):
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL); return
            self.clear_pending_offer(); self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)

class ElectricTaxiMovingToCustomerState(ElectricTaxiStrategyBehaviour):
    """
    Monitor ElectricTaxi approach movement toward the assigned customer.

    MovableMixin performs physical movement while this state observes arrival
    and customer cancellation.

    Matching customer refusal before pickup emits ``service_failed`` with
    ``customer_cancelled_before_pickup``, discards the incomplete approach
    movement, clears service and assignment state, and restores availability.

    Confirmed arrival emits the pending ``phase="approach"``
    ``movement_completed`` event and informs the customer that the
    ElectricTaxi is at the pickup location.
    """
    async def on_start(self):
        await super().on_start(); self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER

    async def run(self):
        """
        Monitor approach movement until pickup arrival or cancellation.

        While movement remains incomplete, the state briefly waits for customer
        messages and otherwise remains in ``TRANSPORT_MOVING_TO_CUSTOMER``.

        Matching refusal fails the service and discards the pending approach
        movement.

        Physical arrival completes that movement, informs the customer with
        ``TRANSPORT_IN_CUSTOMER_PLACE``, and advances to
        ``TRANSPORT_ARRIVED_AT_CUSTOMER``.

        Existing route and unexpected-error recovery terminates the active service
        and returns the ElectricTaxi to normal waiting.
        """
        customers = self.get("assigned_customer")
        if not customers:
            self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        customer_id = next(iter(customers.items()))[0]
        try:
            if not self.agent.is_in_destination():
                msg = await self.receive(timeout=2)
                if msg and msg.get_metadata("performative") == REFUSE_PERFORMATIVE:
                    try: content=json.loads(msg.body)
                    except (json.JSONDecodeError,TypeError):
                        self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
                    if not self.message_matches_active_service(content):
                        self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
                    self.fail_service("customer_cancelled_before_pickup")
                    self.discard_pending_movement()
                    await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"customer_cancelled_before_pickup"})
                    self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
                    self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
                self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
            self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=TRANSPORT_IN_CUSTOMER_PLACE)
            self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER; self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        except AlreadyInDestination:
            if not self.complete_pending_movement():
                self.set_pending_movement("approach",0); self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=TRANSPORT_IN_CUSTOMER_PLACE)
            self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER; self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        except PathRequestException:
            self.fail_service("approach_route_failed"); self.discard_pending_movement()
            await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"approach_route_failed"})
            self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
            self.fail_service("approach_unexpected_error"); self.discard_pending_movement()
            await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"approach_unexpected_error"})
            self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return

class ElectricTaxiArrivedAtCustomerState(ElectricTaxiStrategyBehaviour):
    """
    Wait for the customer to board after ElectricTaxi pickup arrival.

    Matching INFORM_PERFORMATIVE with ``CUSTOMER_IN_TRANSPORT`` transfers the
    customer into onboard state, emits ``service_started``, and starts physical
    movement toward the customer destination.

    The destination leg is not charged again against autonomy because the
    complete estimated customer trip was already deducted when the proposal
    was accepted.

    A matching cancellation fails the service at pickup and restores normal
    ElectricTaxi availability.
    """
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER

    async def run(self):
        """
        Process customer boarding or cancellation at the pickup point.

        CUSTOMER_IN_TRANSPORT moves the customer into onboard state and emits
        ``service_started`` before destination route resolution.

        Successful route planning registers pending ``phase="service"`` movement
        and advances to ``TRANSPORT_MOVING_TO_DESTINATION``.

        If pickup and destination already coincide, an explicit zero-distance
        service movement is emitted and the FSM advances directly to
        ``TRANSPORT_ARRIVED_AT_DESTINATION``.

        A destination PathRequestException restores the straight-line
        ``service_km`` reserved for the unexecuted destination leg, emits
        ``service_failed``, clears the active lifecycle, and returns to waiting.

        Other unexpected service-setup failures follow the existing failure path.
        """
        msg=await self.receive(timeout=60)
        if not msg: self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        try: content=json.loads(msg.body)
        except (json.JSONDecodeError,TypeError): self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        performative=msg.get_metadata("performative")
        if performative in (INFORM_PERFORMATIVE,CANCEL_PERFORMATIVE) and not self.message_matches_active_service(content):
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        if performative==INFORM_PERFORMATIVE and content.get("status")==CUSTOMER_IN_TRANSPORT:
            customers=self.get("assigned_customer")
            if not customers: self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
            customer_id = next(iter(customers.items()))[0]
            dest = next(iter(customers.items()))[1]["destination"]

            service_km = self.agent.calculate_distance_km(
                self.agent.get_position(),
                dest,
            )

            try:
                self.agent.add_customer_in_transport(
                    customer_id=customer_id,
                    dest=dest,
                )

                if not self.start_service(self.agent.jid):
                    raise RuntimeError(
                        "Unable to emit Electric Taxi service start."
                    )

                self.agent.remove_assigned_customer()

                distance, _osrm_duration, _speed_based_duration = (
                    await self.agent.move_to(dest)
                )

                if not self.set_pending_movement("service", distance):
                    raise RuntimeError(
                        "Unable to register Electric Taxi service movement."
                    )

                self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
                self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)
                return

            except AlreadyInDestination:
                self.set_pending_movement("service", 0)
                self.complete_pending_movement()

                await self.inform_customer(
                    customer_id=customer_id,
                    status=CUSTOMER_IN_DEST,
                )

                self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION
                self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
                return

            except PathRequestException:
                # The service leg never started physically. Restore the
                # autonomy that had been reserved for that unexecuted leg.
                self.agent.increase_autonomy_km(service_km)

                self.fail_service("service_route_failed")
                self.discard_pending_movement()

                await self.cancel_customer(
                    customer_id,
                    {
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
            except Exception as e:
                logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
                self.fail_service("service_unexpected_error"); self.discard_pending_movement()
                await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_unexpected_error"})
                self.clear_service_context(); self.clear_active_message_context()
                if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
                self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        elif performative==CANCEL_PERFORMATIVE:
            self.fail_service("customer_cancelled_at_pickup"); self.discard_pending_movement()
            self.clear_service_context(); self.clear_active_message_context()
            if self.agent.get("assigned_customer"): self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)

class ElectricTaxiMovingToCustomerDestState(ElectricTaxiStrategyBehaviour):
    """
    Monitor active ElectricTaxi passenger movement to destination.

    Physical movement is performed by MovableMixin. This state waits for
    destination completion while preserving the already reserved autonomy
    accounting for the service.

    Confirmed arrival emits the pending ``phase="service"``
    ``movement_completed`` event and informs the customer with
    ``CUSTOMER_IN_DEST``.

    The FSM then enters ``TRANSPORT_ARRIVED_AT_DESTINATION`` and waits for
    explicit customer-side lifecycle completion.
    """
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_MOVING_TO_DESTINATION

    async def run(self):
        """
        Monitor customer-service movement until destination or failure.

        Incomplete physical movement keeps the state active after a one-second
        asynchronous wait.

        Arrival completes the pending service movement and informs the customer
        that the destination has been reached.

        Route or unexpected movement failures terminate the canonical service,
        clear onboard customer state, restore availability according to the
        existing recovery path, and return to ``TRANSPORT_WAITING``.

        AlreadyInDestination completes an existing pending movement or emits an
        explicit zero-distance service movement.
        """
        customers=self.get("current_customer")
        if not customers: self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        customer_id=next(iter(customers.items()))[0]
        try:
            if not self.agent.is_in_destination():
                await self.agent.sleep(1); self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION); return
            self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=CUSTOMER_IN_DEST)
            self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION; self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        except AlreadyInDestination:
            if not self.complete_pending_movement(): self.set_pending_movement("service",0); self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=CUSTOMER_IN_DEST)
            self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION; self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        except PathRequestException:
            self.fail_service("service_route_failed"); self.discard_pending_movement()
            await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_route_failed"})
            self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
            self.fail_service("service_unexpected_error"); self.discard_pending_movement()
            await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_unexpected_error"})
            self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return

class ElectricTaxiArrivedAtCustomerDestState(ElectricTaxiStrategyBehaviour):
    """
    Wait for explicit customer confirmation after physical destination arrival.

    Physical arrival alone does not emit ``service_completed``. The customer
    side owns canonical completion.

    Matching INFORM_PERFORMATIVE with ``CUSTOMER_IN_DEST`` mirrors the
    already-completed lifecycle locally, removes the onboard customer,
    increments completed assignments, and keeps the ElectricTaxi busy while it
    enters the Taxi return phase.

    A matching terminal cancellation instead emits ``service_failed``, clears
    the service context, restores availability, and returns directly to
    ``TRANSPORT_WAITING``.
    """
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION

    async def run(self):
        """
        Resolve the customer-side terminal service message.

        CUSTOMER_IN_DEST mirrors successful customer-owned completion through
        ``mark_service_completed()`` without emitting a duplicate
        ``service_completed`` event.

        The onboard customer and active message context are cleared, completed
        assignments are incremented, and the ElectricTaxi remains busy while
        entering ``TRANSPORT_WAITING_FOR_RETURN``.

        Matching cancellation emits ``service_failed`` with
        ``customer_cancelled_at_destination`` and performs terminal cleanup.
        """
        customers=self.get("current_customer")
        if not customers: self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        customer_id=next(iter(customers.items()))[0]
        msg=await self.receive(timeout=60)
        if not msg: self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        try: content=json.loads(msg.body)
        except (json.JSONDecodeError,TypeError): self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        performative=msg.get_metadata("performative")
        if performative in (INFORM_PERFORMATIVE,CANCEL_PERFORMATIVE) and not self.message_matches_active_service(content):
            self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        if performative==INFORM_PERFORMATIVE and content.get("status")==CUSTOMER_IN_DEST:
            if not self.mark_service_completed(): logger.warning("Agent[{}]: Could not mirror completed Electric Taxi service.".format(self.agent.name))
            self.agent.remove_customer_in_transport(customer_id); self.clear_active_message_context(); self.agent.increment_completed_assignments()
            self.agent.status=TRANSPORT_WAITING_FOR_RETURN; self.agent.set_busy(); self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
        if performative==CANCEL_PERFORMATIVE:
            self.fail_service("customer_cancelled_at_destination"); self.discard_pending_movement(); self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)

class ElectricTaxiWaitingForReturnState(ElectricTaxiStrategyBehaviour):
    """
    Resolve and execute the post-service ElectricTaxi return operation.

    The completed customer service context is retained while this state is
    active and the ElectricTaxi remains unavailable for new assignments.

    If no return position is stored, the registered FleetManager is queried
    using ``request_type="taxi_return"``.

    Once a return point is known, the ElectricTaxi calculates the straight-line
    autonomy required to reach it.

    Sufficient autonomy starts route-based return movement and registers
    pending ``phase="auxiliary"`` movement.

    Insufficient autonomy preserves the return point, keeps the ElectricTaxi
    busy, and enters ``TRANSPORT_NEEDS_CHARGING``. After successful charging,
    the charging FSM returns here to continue the same return operation.

    Reaching an already-current return point emits zero-distance auxiliary
    movement, clears the retained completed service context, removes the return
    point, restores availability, and returns to normal waiting.
    """
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_WAITING_FOR_RETURN; self.return_requested=False

    async def run(self):
        """
        Resolve a return point, verify autonomy, and initiate ElectricTaxi return.

        Missing return-point data triggers FleetManager resolution.

        When the return point is known, straight-line distance is used for the
        autonomy check while ``move_to()`` provides the routed distance recorded in
        the pending auxiliary movement.

        Insufficient autonomy diverts to ``TRANSPORT_NEEDS_CHARGING`` without
        clearing the return point or completed service context.

        Successful route setup deducts the straight-line return estimate and
        enters ``TRANSPORT_MOVING_TO_RETURN``.

        Already being at the return point completes an explicit zero-distance
        auxiliary movement and ends the retained service cycle.

        Route-resolution failures discard any pending return movement and retry
        this state without changing the already completed customer lifecycle.
        """
        if not self.agent.has_return_position():
            if not self.return_requested:
                await self.request_return_position(); self.return_requested=True
            msg=await self.receive(timeout=5)
            if not msg: self.return_requested=False; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
            if msg.get_metadata("performative")!=INFORM_PERFORMATIVE: self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
            try: content=json.loads(msg.body)
            except (json.JSONDecodeError,TypeError): self.return_requested=False; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
            if content.get("request_type")!="taxi_return": self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
            return_position=content.get("return_position")
            if return_position is None: self.return_requested=False; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
            self.agent.set_return_position(return_position)
        return_position=self.agent.get_return_position()
        if return_position is None: self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
        return_km=self.agent.calculate_distance_km(self.agent.get_position(),return_position)
        if not self.agent.has_enough_autonomy_km(return_km):
            self.agent.status=TRANSPORT_NEEDS_CHARGING; self.agent.set_busy(); self.set_next_state(TRANSPORT_NEEDS_CHARGING); return
        try:
            distance,_osrm_duration,_speed_based_duration=await self.agent.move_to(return_position)
            if not self.set_pending_movement("auxiliary",distance,require_service=False): raise RuntimeError("Unable to register Electric Taxi return movement.")
            self.agent.decrease_autonomy_km(return_km)
            self.agent.status=TRANSPORT_MOVING_TO_RETURN; self.set_next_state(TRANSPORT_MOVING_TO_RETURN); return
        except AlreadyInDestination:
            self.set_pending_movement("auxiliary",0,require_service=False); self.complete_pending_movement(); self.clear_service_context()
            self.agent.clear_return_position(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        except PathRequestException:
            self.discard_pending_movement(); await self.agent.sleep(1); self.agent.status=TRANSPORT_WAITING_FOR_RETURN; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
        except Exception as e:
            logger.error("Unexpected error returning electric taxi [{}]: {}".format(self.agent.name,e)); self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING_FOR_RETURN; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return

class ElectricTaxiMovingToReturnState(ElectricTaxiStrategyBehaviour):
    """
    Monitor auxiliary movement toward the ElectricTaxi return point.

    MovableMixin performs physical return movement while the ElectricTaxi
    remains unavailable for new services.

    Confirmed arrival completes the pending auxiliary movement, clears the
    retained completed mobility-service context and return position, restores
    availability, and transitions to ``TRANSPORT_WAITING``.

    If the return position disappears before arrival, the pending movement is
    discarded and execution returns to
    ``TRANSPORT_WAITING_FOR_RETURN``.
    """
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_MOVING_TO_RETURN

    async def run(self):
        """
        Monitor ElectricTaxi return movement until completion.

        Missing return-point state discards pending movement and restarts
        return-point resolution.

        While physical movement remains incomplete, the state waits one second and
        remains active.

        Arrival emits the pending auxiliary movement, clears retained service and
        return contexts, restores availability, and returns to
        ``TRANSPORT_WAITING``.
        """
        return_position=self.agent.get_return_position()
        if return_position is None:
            self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING_FOR_RETURN; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
        if not self.agent.is_in_destination():
            await self.agent.sleep(1); self.set_next_state(TRANSPORT_MOVING_TO_RETURN); return
        self.complete_pending_movement(); self.clear_service_context(); self.agent.clear_return_position(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return

class FSMElectricTaxiBehaviour(FSMSimfleetBehaviour):
    """
    Finite-state operational strategy for the standard ElectricTaxi agent.

    The FSM combines three coordinated operational flows:

    Customer service
        Request negotiation, approach, pickup, passenger movement, and
        customer-confirmed completion.

    Charging
        Charging-station discovery, auxiliary station travel, admission,
        waiting-list service, and charging completion through an independent
        ``charging_id``.

    Post-service Taxi return
        FleetManager return-point resolution and auxiliary return movement
        before the ElectricTaxi becomes available again.

    Charging may be entered before accepting a customer when autonomy is
    insufficient, or during the post-service return when the retained return
    point cannot be reached safely.

    When charging interrupts a return operation, successful charging resumes
    ``TRANSPORT_WAITING_FOR_RETURN`` instead of returning directly to normal
    waiting.

    Generic FSM lifecycle instrumentation is inherited from
    FSMSimfleetBehaviour.
    """

    def setup(self):
        """
        Register standard ElectricTaxi states and permitted transitions.
        """

        # Add states to the FSM
        self.add_state(TRANSPORT_WAITING, ElectricTaxiWaitingState(), initial=True)
        self.add_state(TRANSPORT_NEEDS_CHARGING, ElectricTaxiNeedsChargingState())
        self.add_state(TRANSPORT_WAITING_FOR_APPROVAL, ElectricTaxiWaitingForApprovalState())
        self.add_state(TRANSPORT_MOVING_TO_CUSTOMER, ElectricTaxiMovingToCustomerState())
        self.add_state(TRANSPORT_ARRIVED_AT_CUSTOMER, ElectricTaxiArrivedAtCustomerState())
        self.add_state(TRANSPORT_MOVING_TO_DESTINATION, ElectricTaxiMovingToCustomerDestState())
        self.add_state(TRANSPORT_ARRIVED_AT_DESTINATION, ElectricTaxiArrivedAtCustomerDestState())
        self.add_state(TRANSPORT_MOVING_TO_STATION, ElectricTaxiMovingToStationState())
        self.add_state(TRANSPORT_IN_STATION_PLACE, ElectricTaxiInStationState())
        self.add_state(TRANSPORT_IN_WAITING_LIST, ElectricTaxiInWaitingListState())
        self.add_state(TRANSPORT_CHARGING, ElectricTaxiChargingState())
        self.add_state(TRANSPORT_WAITING_FOR_RETURN, ElectricTaxiWaitingForReturnState())
        self.add_state(TRANSPORT_MOVING_TO_RETURN,ElectricTaxiMovingToReturnState())

        # Define transitions between states

        # Transitions related to the 'Waiting' state
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING)  # Remains in waiting if no new action
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING_FOR_APPROVAL)  # When a customer accepts a proposal
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_NEEDS_CHARGING)  # If the taxi needs charging

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

        # Transitions related to the 'Needs Charging' state
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_NEEDS_CHARGING)  # Continue searching for a station
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_WAITING)  # Issue finding station, return to waiting
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_MOVING_TO_STATION)  # Successfully heading to station
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_IN_STATION_PLACE)  # Arrives at the station

        # Transitions from 'Moving To Station' state
        self.add_transition(TRANSPORT_MOVING_TO_STATION, TRANSPORT_MOVING_TO_STATION)  # Still heading to the station
        self.add_transition(TRANSPORT_MOVING_TO_STATION, TRANSPORT_IN_STATION_PLACE)  # Arrives at station
        self.add_transition(TRANSPORT_MOVING_TO_STATION, TRANSPORT_NEEDS_CHARGING)

        # Transitions from 'In Station Place' state
        self.add_transition(TRANSPORT_IN_STATION_PLACE, TRANSPORT_IN_STATION_PLACE)  # Waiting in station queue
        self.add_transition(TRANSPORT_IN_STATION_PLACE, TRANSPORT_NEEDS_CHARGING)  # Transition if refused service
        self.add_transition(TRANSPORT_IN_STATION_PLACE, TRANSPORT_IN_WAITING_LIST)  # Moved to waiting list for service

        # Transitions from 'In Waiting List' state
        self.add_transition(TRANSPORT_IN_WAITING_LIST, TRANSPORT_IN_WAITING_LIST)  # Remain in queue
        self.add_transition(TRANSPORT_IN_WAITING_LIST, TRANSPORT_CHARGING)  # Begin charging process
        self.add_transition(TRANSPORT_IN_WAITING_LIST, TRANSPORT_NEEDS_CHARGING)

        # Transitions from 'Charging' state
        self.add_transition(TRANSPORT_CHARGING, TRANSPORT_CHARGING)  # Continue charging
        self.add_transition(TRANSPORT_CHARGING, TRANSPORT_WAITING)  # Finish charging and return to waiting

        # Additional transitions for customer movement and destination states
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_MOVING_TO_CUSTOMER)  # Still en route to customer
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_WAITING)  # Return to waiting if issue arises

        self.add_transition(TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_WAITING_FOR_RETURN)

        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_MOVING_TO_RETURN)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_NEEDS_CHARGING)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_WAITING)

        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_MOVING_TO_RETURN)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_WAITING)

        self.add_transition(TRANSPORT_CHARGING, TRANSPORT_WAITING_FOR_RETURN)


################################################################
#                                                              #
#           NO Point Of Return Electric Taxi Strategy          #
#                                                              #
################################################################

class NRPElectricTaxiWaitingState(ElectricTaxiStrategyBehaviour):
    """
    Idle request-screening state of the NRP ElectricTaxi FSM.

    The transport waits for canonical ElectricTaxi customer requests and uses
    the same schema-1.0 negotiation contract as the standard ElectricTaxi.

    A valid REQUEST_PERFORMATIVE is stored as a pending offer before autonomy
    is evaluated.

    Sufficient autonomy sends a PROPOSE_PERFORMATIVE and advances to
    ``TRANSPORT_WAITING_FOR_APPROVAL``.

    Insufficient autonomy cancels the unresolved proposal, clears the pending
    offer, and enters ``TRANSPORT_NEEDS_CHARGING``. No mobility-service
    context has been created at that point, so no ``service_failed`` event is
    emitted.

    Missing or unsupported messages remain in ``TRANSPORT_WAITING``.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING

    async def run(self):
        """
        Receive one NRP ElectricTaxi request and screen it against autonomy.

        A valid schema-1.0 request becomes a pending proposal.

        Sufficient autonomy sends the proposal and advances to customer approval.
        Insufficient autonomy cancels the unresolved proposal and diverts the
        ElectricTaxi into the charging circuit.

        Customer-service lifecycle creation remains deferred until a later valid
        ACCEPT_PERFORMATIVE.
        """
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


            if not self.agent.has_enough_autonomy_for_service(
                content["origin"],
                content["dest"]
            ):


                await self.cancel_proposal(
                    content["customer_id"],
                    self.pending_offer_message_details(),
                )
                self.clear_pending_offer()
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return
            else:


                await self.send_proposal(
                    content["customer_id"],
                    self.pending_offer_message_details(),
                )
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
                return
        else:
            self.set_next_state(TRANSPORT_WAITING)
            return


class NPRElectricTaxiWaitingForApprovalState(ElectricTaxiStrategyBehaviour):
    """
    Resolve the customer response to a pending NRP ElectricTaxi proposal.

    Matching acceptance activates message correlation and performs a second
    autonomy validation for the complete estimated customer trip.

    If autonomy is no longer sufficient, the accepted proposal is cancelled,
    active message context is cleared, the ElectricTaxi remains unavailable,
    and execution enters ``TRANSPORT_NEEDS_CHARGING`` without creating a
    canonical mobility-service lifecycle.

    With sufficient autonomy, the service context is created,
    ``service_assigned`` is emitted, the customer is bound to the transport,
    and approach movement begins.

    Matching refusal clears the pending offer and returns directly to normal
    waiting.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING_FOR_APPROVAL

    async def run(self):
        """
        Process acceptance or refusal of the pending NRP ElectricTaxi proposal.

        Valid acceptance first recalculates the complete service distance.

        Insufficient autonomy cancels the accepted proposal and enters the
        charging circuit before ``service_assigned`` is emitted.

        When autonomy is sufficient:

        1. the canonical service context is created;
        2. ``service_assigned`` is emitted;
        3. the customer is assigned;
        4. the ElectricTaxi is marked busy;
        5. approach movement to the customer is requested;
        6. autonomy for the complete estimated trip is deducted;
        7. the route is registered as pending ``phase="approach"`` movement.

        Successful route setup enters ``TRANSPORT_MOVING_TO_CUSTOMER``.

        Already being at pickup produces an explicit zero-distance approach
        movement and advances directly to
        ``TRANSPORT_ARRIVED_AT_CUSTOMER``.

        Approach-route and unexpected setup failures terminate the service and
        recover to ``TRANSPORT_WAITING`` according to the current error path.
        """
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            if not self.message_matches_pending_offer(content):
                logger.warning("Agent[{}]: Ignoring stale or mismatched acceptance.".format(self.agent.name))
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
                return
            active = self.activate_pending_offer(content)
            customer_id = content["customer_id"]
            if not self.agent.has_enough_autonomy_km(self.agent.calculate_service_km(content["origin"], content["dest"])):
                await self.cancel_proposal(customer_id)
                self.clear_active_message_context()
                self.agent.set_busy()
                self.agent.status = TRANSPORT_NEEDS_CHARGING
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return
            try:
                context = self.create_service_context(
                    service_id=active["service_id"], user_id=active["user_id"],
                    transport_id=active["transport_id"], origin=active["origin"],
                    destination=active["destination"], emit_requested=False,
                )
                if context is None or not self.assign_service(self.agent.jid):
                    raise RuntimeError("Unable to establish Electric Taxi metrics service context.")
                self.agent.add_assigned_customer(
                    customer_id=customer_id, origin=content["origin"], dest=content["dest"]
                )
                self.agent.set_busy()
                await self.inform_customer(customer_id=customer_id, status=TRANSPORT_MOVING_TO_CUSTOMER)
                distance, _osrm_duration, _speed_based_duration = await self.agent.move_to(content["origin"])
                self.agent.decrease_autonomy_km(self.agent.calculate_service_km(content["origin"], content["dest"]))
                if not self.set_pending_movement("approach", distance):
                    raise RuntimeError("Unable to register Electric Taxi approach movement.")
                self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER
                self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
                return
            except AlreadyInDestination:
                self.agent.decrease_autonomy_km(
                    self.agent.calculate_service_km(
                        content["origin"],
                        content["dest"]
                    )
                )

                self.set_pending_movement("approach", 0)
                self.complete_pending_movement()

                await self.inform_customer(
                    customer_id=customer_id,
                    status=TRANSPORT_IN_CUSTOMER_PLACE
                )

                self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
                self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
                return
            except PathRequestException:
                self.fail_service("approach_route_failed")
                self.discard_pending_movement()
                await self.cancel_proposal(customer_id, {"terminal_status":"failed","failure_reason":"approach_route_failed"})
                self.clear_service_context(); self.clear_active_message_context()
                if self.agent.get("assigned_customer"):
                    self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING; self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING); return
            except Exception as e:
                logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name, e))
                self.fail_service("approach_unexpected_error")
                self.discard_pending_movement()
                await self.cancel_proposal(customer_id, {"terminal_status":"failed","failure_reason":"approach_unexpected_error"})
                self.clear_service_context(); self.clear_active_message_context()
                if self.agent.get("assigned_customer"):
                    self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING; self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING); return
        elif performative == REFUSE_PERFORMATIVE:
            if not self.message_matches_pending_offer(content):
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL); return
            self.clear_pending_offer(); self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)

class NPRElectricTaxiMovingToCustomerState(ElectricTaxiStrategyBehaviour):
    """
    Monitor NRP ElectricTaxi approach movement toward the assigned customer.

    MovableMixin performs physical movement while this state observes arrival
    and possible customer cancellation.

    A matching REFUSE_PERFORMATIVE before pickup emits ``service_failed`` with
    ``customer_cancelled_before_pickup``, discards the incomplete approach
    movement, clears active service state, and restores transport
    availability.

    Confirmed arrival emits the pending ``phase="approach"``
    ``movement_completed`` event and informs the customer that the vehicle is
    at the pickup location.
    """
    async def on_start(self):
        await super().on_start(); self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER

    async def run(self):
        """
        Monitor approach movement until pickup arrival or cancellation.

        Incomplete movement briefly waits for customer messages and otherwise
        remains in ``TRANSPORT_MOVING_TO_CUSTOMER``.

        Matching customer refusal fails the service and discards pending movement.

        Physical arrival completes the approach metric, informs the customer with
        ``TRANSPORT_IN_CUSTOMER_PLACE``, and advances to
        ``TRANSPORT_ARRIVED_AT_CUSTOMER``.

        Route and unexpected movement errors terminate the active service and
        recover to normal waiting.
        """
        customers = self.get("assigned_customer")
        if not customers:
            self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        customer_id = next(iter(customers.items()))[0]
        try:
            if not self.agent.is_in_destination():
                msg = await self.receive(timeout=2)
                if msg and msg.get_metadata("performative") == REFUSE_PERFORMATIVE:
                    try: content=json.loads(msg.body)
                    except (json.JSONDecodeError,TypeError):
                        self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
                    if not self.message_matches_active_service(content):
                        self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
                    self.fail_service("customer_cancelled_before_pickup")
                    self.discard_pending_movement()
                    await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"customer_cancelled_before_pickup"})
                    self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
                    self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
                self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
            self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=TRANSPORT_IN_CUSTOMER_PLACE)
            self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER; self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        except AlreadyInDestination:
            if not self.complete_pending_movement():
                self.set_pending_movement("approach",0); self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=TRANSPORT_IN_CUSTOMER_PLACE)
            self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER; self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        except PathRequestException:
            self.fail_service("approach_route_failed"); self.discard_pending_movement()
            await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"approach_route_failed"})
            self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
            self.fail_service("approach_unexpected_error"); self.discard_pending_movement()
            await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"approach_unexpected_error"})
            self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return

class NPRElectricTaxiArrivedAtCustomerState(ElectricTaxiStrategyBehaviour):
    """
    Wait for customer boarding at the NRP ElectricTaxi pickup location.

    Matching INFORM_PERFORMATIVE with ``CUSTOMER_IN_TRANSPORT`` transfers the
    customer into onboard state and emits ``service_started``.

    Movement toward the customer destination is then resolved and registered
    as pending ``phase="service"`` movement.

    The destination leg is not deducted from autonomy a second time because the
    complete estimated customer trip was reserved during proposal acceptance.

    Matching cancellation fails the service at pickup and restores the
    ElectricTaxi to normal waiting.
    """
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER

    async def run(self):
        """
        Process customer boarding or cancellation at the pickup location.

        CUSTOMER_IN_TRANSPORT:

        - moves the customer into onboard state;
        - emits ``service_started``;
        - clears pre-pickup assignment;
        - resolves movement to the customer destination;
        - registers pending ``phase="service"`` movement.

        Successful route setup enters
        ``TRANSPORT_MOVING_TO_DESTINATION``.

        If the destination already equals the current position, a zero-distance
        service movement is emitted and execution advances directly to
        ``TRANSPORT_ARRIVED_AT_DESTINATION``.

        A destination PathRequestException restores the straight-line autonomy
        reserved for the unexecuted service leg before failing the service.

        Other unexpected setup errors follow the existing failure path without
        changing the current autonomy-recovery policy.
        """
        msg=await self.receive(timeout=60)
        if not msg: self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        try: content=json.loads(msg.body)
        except (json.JSONDecodeError,TypeError): self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        performative=msg.get_metadata("performative")
        if performative in (INFORM_PERFORMATIVE,CANCEL_PERFORMATIVE) and not self.message_matches_active_service(content):
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        if performative==INFORM_PERFORMATIVE and content.get("status")==CUSTOMER_IN_TRANSPORT:
            customers=self.get("assigned_customer")
            if not customers: self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
            customer_id = next(iter(customers.items()))[0]
            dest = next(iter(customers.items()))[1]["destination"]

            service_km = self.agent.calculate_distance_km(
                self.agent.get_position(),
                dest,
            )

            try:
                self.agent.add_customer_in_transport(
                    customer_id=customer_id,
                    dest=dest,
                )

                if not self.start_service(self.agent.jid):
                    raise RuntimeError(
                        "Unable to emit Electric Taxi service start."
                    )

                self.agent.remove_assigned_customer()

                distance, _osrm_duration, _speed_based_duration = (
                    await self.agent.move_to(dest)
                )

                if not self.set_pending_movement("service", distance):
                    raise RuntimeError(
                        "Unable to register Electric Taxi service movement."
                    )

                self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
                self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)
                return

            except AlreadyInDestination:
                self.set_pending_movement("service", 0)
                self.complete_pending_movement()

                await self.inform_customer(
                    customer_id=customer_id,
                    status=CUSTOMER_IN_DEST,
                )

                self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION
                self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
                return

            except PathRequestException:
                # The service leg never started physically. Restore the
                # autonomy that had been reserved for that unexecuted leg.
                self.agent.increase_autonomy_km(service_km)

                self.fail_service("service_route_failed")
                self.discard_pending_movement()

                await self.cancel_customer(
                    customer_id,
                    {
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
            except Exception as e:
                logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
                self.fail_service("service_unexpected_error"); self.discard_pending_movement()
                await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_unexpected_error"})
                self.clear_service_context(); self.clear_active_message_context()
                if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
                self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        elif performative==CANCEL_PERFORMATIVE:
            self.fail_service("customer_cancelled_at_pickup"); self.discard_pending_movement()
            self.clear_service_context(); self.clear_active_message_context()
            if self.agent.get("assigned_customer"): self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)

class NPRElectricTaxiMovingToCustomerDestState(ElectricTaxiStrategyBehaviour):
    """
    Monitor active NRP ElectricTaxi passenger movement to destination.

    MovableMixin performs physical movement while this state waits for arrival.

    Confirmed arrival emits the pending ``phase="service"``
    ``movement_completed`` event and informs the customer with
    ``CUSTOMER_IN_DEST``.

    The FSM then waits in ``TRANSPORT_ARRIVED_AT_DESTINATION`` for explicit
    customer-side lifecycle completion.

    Route or unexpected movement failure terminates the active service and
    restores normal transport availability.
    """
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_MOVING_TO_DESTINATION

    async def run(self):
        """
        Monitor NRP customer-service movement until destination or failure.

        Incomplete movement remains active after a one-second asynchronous wait.

        Arrival completes the pending service movement and informs the customer
        that the destination has been reached.

        AlreadyInDestination completes an existing pending movement or emits an
        explicit zero-distance service movement.

        Route and unexpected errors fail the canonical service, clear onboard
        customer state, restore availability, and return to waiting.
        """
        customers=self.get("current_customer")
        if not customers: self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        customer_id=next(iter(customers.items()))[0]
        try:
            if not self.agent.is_in_destination():
                await self.agent.sleep(1); self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION); return
            self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=CUSTOMER_IN_DEST)
            self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION; self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        except AlreadyInDestination:
            if not self.complete_pending_movement(): self.set_pending_movement("service",0); self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=CUSTOMER_IN_DEST)
            self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION; self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        except PathRequestException:
            self.fail_service("service_route_failed"); self.discard_pending_movement()
            await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_route_failed"})
            self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
            self.fail_service("service_unexpected_error"); self.discard_pending_movement()
            await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_unexpected_error"})
            self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return

class NPRElectricTaxiArrivedAtCustomerDestState(ElectricTaxiStrategyBehaviour):
    """
    Resolve customer-side completion after NRP ElectricTaxi destination
    arrival.

    Physical arrival alone does not emit ``service_completed``. Canonical
    completion remains owned by the customer strategy.

    Matching INFORM_PERFORMATIVE with ``CUSTOMER_IN_DEST`` mirrors that
    completion locally through ``mark_service_completed()``, removes the
    onboard customer, clears active message and service contexts, increments
    completed assignments, immediately restores ElectricTaxi availability, and
    returns to ``TRANSPORT_WAITING``.

    Unlike the standard ElectricTaxi FSM, NRP has no FleetManager return-point
    phase after successful customer service.

    Matching cancellation emits ``service_failed`` and performs terminal
    cleanup before returning to waiting.
    """
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION

    async def run(self):
        """
        Process the terminal customer message for an NRP ElectricTaxi service.

        CUSTOMER_IN_DEST mirrors already emitted customer-side completion without
        producing a duplicate ``service_completed`` event.

        The customer, active message context, and canonical service context are
        then cleared. Completed assignments are incremented and the ElectricTaxi
        becomes immediately available for another service.

        CANCEL_PERFORMATIVE records
        ``customer_cancelled_at_destination`` as ``service_failed`` and performs
        terminal cleanup.

        Timeouts, malformed payloads, and stale messages remain in
        ``TRANSPORT_ARRIVED_AT_DESTINATION``.
        """
        customers=self.get("current_customer")
        if not customers: self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        customer_id=next(iter(customers.items()))[0]
        msg=await self.receive(timeout=60)
        if not msg: self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        try: content=json.loads(msg.body)
        except (json.JSONDecodeError,TypeError): self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        performative=msg.get_metadata("performative")
        if performative in (INFORM_PERFORMATIVE,CANCEL_PERFORMATIVE) and not self.message_matches_active_service(content):
            self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        if performative==INFORM_PERFORMATIVE and content.get("status")==CUSTOMER_IN_DEST:
            if not self.mark_service_completed(): logger.warning("Agent[{}]: Could not mirror completed NRP Electric Taxi service.".format(self.agent.name))
            self.agent.remove_customer_in_transport(customer_id); self.clear_active_message_context(); self.agent.increment_completed_assignments(); self.clear_service_context()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        if performative==CANCEL_PERFORMATIVE:
            self.fail_service("customer_cancelled_at_destination"); self.discard_pending_movement(); self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)

class NPRElectricTaxiNeedsChargingState(ElectricTaxiStrategyBehaviour):
    """
    Discover a charging station for the NRP ElectricTaxi and start travel
    toward it.

    The ElectricTaxi remains busy while charging is required and therefore
    cannot accept another customer service.

    When no usable station list is available, charging-station positions are
    requested for the configured service type and the state retries.

    A nearby station is selected, stored as the current charging target, and
    associated with a new independent ``charging_id`` before physical travel
    begins.

    Route-based travel to the station is registered as canonical
    ``phase="auxiliary"`` movement. Autonomy consumption uses
    ChargeableMixin's straight-line geographic estimate, while movement
    metrics retain the routed distance returned by ``move_to()``.

    Successful travel setup advances to
    ``TRANSPORT_MOVING_TO_STATION``.

    If the ElectricTaxi is already at the station, an explicit zero-distance
    auxiliary movement and ``charging_arrived`` are emitted immediately before
    station access is requested.

    Charging-route failures abandon the unfinished charging attempt, clear the
    selected station, and retry from ``TRANSPORT_NEEDS_CHARGING``.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_NEEDS_CHARGING
        self.agent.set_busy()

    async def run(self):
        """
        Resolve a charging station and initiate NRP ElectricTaxi travel toward it.

        Missing station candidates keep the FSM in
        ``TRANSPORT_NEEDS_CHARGING``.

        Once a station is selected, a charging context is created before movement
        starts so arrival, service start, and completion share one
        ``charging_id``.

        Straight-line station distance is used for autonomy consumption, whereas
        ``move_to()`` supplies the distance stored in the pending auxiliary
        movement.

        Successful route setup enters ``TRANSPORT_MOVING_TO_STATION``.

        Already being at the station completes a zero-distance auxiliary movement,
        emits ``charging_arrived``, requests charging service, and enters
        ``TRANSPORT_IN_STATION_PLACE``.

        Route and unexpected setup errors discard incomplete movement, abandon the
        charging context, clear station selection, and retry.
        """
        if self.agent.get_stations() is None or self.agent.get_number_stations() < 1:
            logger.info(
                "Agent[{}]: The agent looking for a station.".format(
                    self.agent.name
                )
            )
            stations = await self.agent.get_list_agent_position(
                self.agent.service_type,
                self.agent.get_stations()
            )
            self.agent.set_stations(stations)

            if not stations:
                await self.agent.sleep(1)

            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

        nearby_station_dest = self.agent.nearst_agent(
            self.agent.get_stations(),
            self.agent.get_position()
        )

        if nearby_station_dest is None:
            logger.warning(
                "Agent[{}]: No charging station available.".format(
                    self.agent.name
                )
            )

            await self.agent.sleep(1)

            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

        self.agent.set_nearby_station(nearby_station_dest)
        station_id = self.agent.get_nearby_station_id()
        station_position = self.agent.get_nearby_station_position()
        logger.info("Agent[{}]: The agent selected station [{}].".format(self.agent.name, station_id))

        # A charging_id is independent from service_id. It is created before travel
        # so the same session is used for physical arrival/start/completion.
        if self.get_charging_context() is None:
            if self.create_charging_context(station_id) is None:
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return

        try:
            await self.go_to_the_station(station_id, station_position)
            travel_km = self.agent.calculate_distance_km(
                self.agent.get_position(), station_position
            )
            try:
                distance, _osrm_duration, _speed_based_duration = await self.agent.move_to(
                    station_position
                )
                if not self.set_pending_movement(
                    "auxiliary", distance, require_service=False
                ):
                    raise RuntimeError("Unable to register charging-station movement.")
                self.agent.decrease_autonomy_km(travel_km)
                self.agent.status = TRANSPORT_MOVING_TO_STATION
                self.set_next_state(TRANSPORT_MOVING_TO_STATION)
                return
            except AlreadyInDestination:
                self.set_pending_movement("auxiliary", 0, require_service=False)
                self.complete_pending_movement()
                self.charging_arrived()
                arguments = {
                    "transport_need": self.agent.max_autonomy_km - self.agent.current_autonomy_km
                }
                await self.request_access_station(
                    station_id,
                    {"service_name": self.agent.service_type, "object_type": "transport", "args": arguments},
                )
                self.agent.status = TRANSPORT_IN_STATION_PLACE
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
        except PathRequestException:
            logger.error("Agent[{}]: The agent could not get a path to station [{}].".format(self.agent.name, station_id))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name, e))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

class NPRElectricTaxiMovingToStationState(ElectricTaxiStrategyBehaviour):
    """
    Monitor NRP ElectricTaxi movement toward the selected charging station.

    MovableMixin performs physical movement while this state observes station
    arrival.

    Confirmed arrival completes the pending ``phase="auxiliary"`` movement,
    ensures that a charging context exists, emits ``charging_arrived``, and
    requests access to the station service.

    Successful arrival advances to ``TRANSPORT_IN_STATION_PLACE``.

    Route or unexpected movement failure discards the incomplete movement,
    abandons the charging attempt, clears station state, and returns to
    ``TRANSPORT_NEEDS_CHARGING``.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_STATION

    async def _arrive_and_request(self):
        """
        Finalize station arrival and request NRP ElectricTaxi charging access.

        The pending auxiliary movement is completed first. A charging context is
        created when necessary, ``charging_arrived`` is emitted exactly once, and
        the remaining charging need is sent to the selected station.

        Successful preparation advances to
        ``TRANSPORT_IN_STATION_PLACE``.

        Returns:
            bool: True when station-arrival processing was prepared successfully.
        """
        station_id = self.agent.get_current_station()
        self.complete_pending_movement()
        if self.get_charging_context() is None:
            if self.create_charging_context(station_id) is None:
                return False
        self.charging_arrived()
        arguments = {
            "transport_need": self.agent.max_autonomy_km - self.agent.current_autonomy_km
        }
        await self.request_access_station(
            station_id,
            {"service_name": self.agent.service_type, "object_type": "transport", "args": arguments},
        )
        self.agent.status = TRANSPORT_IN_STATION_PLACE
        self.set_next_state(TRANSPORT_IN_STATION_PLACE)
        return True

    async def run(self):
        """
        Monitor NRP movement to the charging station until arrival or recovery.

        While physical movement is incomplete, the state sleeps asynchronously for
        one second and remains in ``TRANSPORT_MOVING_TO_STATION``.

        Confirmed arrival delegates to ``_arrive_and_request()``.

        AlreadyInDestination ensures an explicit auxiliary movement exists,
        including zero distance when necessary, before the station-arrival
        lifecycle is processed.

        Charging-route failures abandon the current attempt and return to station
        discovery.
        """
        try:
            if not self.agent.is_in_destination():
                await self.agent.sleep(1)
                self.set_next_state(TRANSPORT_MOVING_TO_STATION)
                return
            await self._arrive_and_request()
            return
        except AlreadyInDestination:
            if not self.complete_pending_movement():
                self.set_pending_movement("auxiliary", 0, require_service=False)
            await self._arrive_and_request()
            return
        except PathRequestException:
            logger.error("Agent[{}]: The agent could not complete the path to charging station.".format(self.agent.name))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        except Exception as e:
            logger.error("Unexpected charging-route error in [{}]: {}".format(self.agent.name, e))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

class NPRElectricTaxiInStationState(ElectricTaxiStrategyBehaviour):
    """
    Wait for charging-station admission after NRP ElectricTaxi arrival.

    ``charging_arrived`` has already been emitted before this state begins.

    A matching ACCEPT_PERFORMATIVE admits the ElectricTaxi to the station
    waiting list and advances to ``TRANSPORT_IN_WAITING_LIST``. Admission does
    not yet mean that charging has started.

    A matching REFUSE_PERFORMATIVE abandons the current internal charging
    context, clears station state, and returns to
    ``TRANSPORT_NEEDS_CHARGING``.

    Previously emitted charging milestones remain in the event log, so a
    refused session after physical arrival is reconstructed as unfinished.

    Timeouts, malformed messages, unrelated performatives, and mismatched
    station identities keep the ElectricTaxi in this state.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_IN_STATION_PLACE

    async def run(self):
        """
        Resolve NRP charging-station admission or refusal.

        ACCEPT from the station associated with the active charging context moves
        the ElectricTaxi into the station waiting list.

        REFUSE from that station abandons the unfinished charging attempt and
        restarts station selection.

        Station identity is validated before either transition.
        """
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_IN_STATION_PLACE)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_IN_STATION_PLACE)
            return
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                logger.warning("Agent[{}]: Ignoring charging-station ACCEPT with mismatched station.".format(self.agent.name))
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
            self.agent.status = TRANSPORT_IN_WAITING_LIST
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        if performative == REFUSE_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
            # The already-emitted arrival remains an unfinished charging session.
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        self.set_next_state(TRANSPORT_IN_STATION_PLACE)

class NPRElectricTaxiInWaitingListState(ElectricTaxiStrategyBehaviour):
    """
    Wait in the charging-station queue until NRP charging service begins.

    Matching INFORM_PERFORMATIVE with a truthy ``serving`` value emits
    ``charging_started`` and advances to ``TRANSPORT_CHARGING``.

    A matching station refusal abandons the unfinished charging context, clears
    station state, and returns to ``TRANSPORT_NEEDS_CHARGING``.

    Timeouts, malformed messages, mismatched stations, and non-serving informs
    keep the ElectricTaxi in the waiting list.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_IN_WAITING_LIST

    async def run(self):
        """
        Wait for confirmation that NRP ElectricTaxi charging is starting.

        A matching ``serving`` notification must successfully emit
        ``charging_started`` before the FSM enters ``TRANSPORT_CHARGING``.

        A station refusal abandons the current unfinished charging attempt and
        restarts station selection.
        """
        msg = await self.receive(timeout=5)
        if not msg:
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        performative = msg.get_metadata("performative")
        if performative == INFORM_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                return
            if content.get("serving"):
                if not self.charging_started():
                    logger.warning("Agent[{}]: Charging start milestone could not be emitted.".format(self.agent.name))
                    self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                    return
                self.agent.status = TRANSPORT_CHARGING
                self.set_next_state(TRANSPORT_CHARGING)
                return
        elif performative == REFUSE_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                return
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        self.set_next_state(TRANSPORT_IN_WAITING_LIST)

class NPRElectricTaxiChargingState(ElectricTaxiStrategyBehaviour):
    """
    Wait for completion of the active NRP ElectricTaxi charging session.

    A session completes only when the station associated with the current
    charging context sends REQUEST_PROTOCOL / INFORM_PERFORMATIVE with a
    truthy ``charged`` field.

    Successful completion emits ``charging_completed``, restores autonomy to
    its configured maximum, clears station state, and removes the completed
    charging context.

    Because the NRP strategy has no post-service return-point lifecycle,
    successful charging always makes the ElectricTaxi available and returns
    directly to ``TRANSPORT_WAITING``.

    Other messages, malformed payloads, mismatched stations, and timeouts keep
    the vehicle in ``TRANSPORT_CHARGING``.
    """
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_CHARGING

    async def run(self):
        """
        Process completion of the active NRP charging session.

        Matching station completion must first emit ``charging_completed``.
        Full autonomy is then restored, station state is cleared, and the completed
        charging context is removed.

        The NRP ElectricTaxi subsequently becomes available and returns directly
        to ``TRANSPORT_WAITING``.
        """
        msg=await self.receive(timeout=60)
        if not msg: self.set_next_state(TRANSPORT_CHARGING); return
        try: content=json.loads(msg.body)
        except (json.JSONDecodeError,TypeError): self.set_next_state(TRANSPORT_CHARGING); return
        if msg.get_metadata("protocol")==REQUEST_PROTOCOL and msg.get_metadata("performative")==INFORM_PERFORMATIVE and content.get("charged"):
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ): self.set_next_state(TRANSPORT_CHARGING); return
            if not self.charging_completed(): self.set_next_state(TRANSPORT_CHARGING); return
            self.agent.increase_full_autonomy_km(); await self.drop_station(); self.clear_charging_context()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_CHARGING)

class FSMNPRElectricTaxiBehaviour(FSMSimfleetBehaviour):
    """
    Finite-state operational strategy for the No-Return-Point ElectricTaxi.

    The FSM combines two operational flows:

    Customer service
        Request negotiation, autonomy screening, approach, pickup, passenger
        movement, and customer-confirmed completion.

    Charging
        Charging-station discovery, auxiliary station travel, admission,
        waiting-list service, and charging completion through an independent
        ``charging_id``.

    The strategy deliberately omits the standard ElectricTaxi post-service
    return-point lifecycle. Successful customer completion clears the mobility
    service context and returns directly to ``TRANSPORT_WAITING``.

    Likewise, successful charging always restores availability and returns to
    ``TRANSPORT_WAITING``; there is no
    ``TRANSPORT_WAITING_FOR_RETURN`` recovery branch.

    NRP remains part of the canonical ``electric_taxi`` modality rather than
    defining a separate public metrics modality.

    Generic FSM lifecycle instrumentation is inherited from
    FSMSimfleetBehaviour.
    """
    def setup(self):
        """
        Register NRP ElectricTaxi states and permitted transitions.
        """
        # ============================================================
        # States
        # ============================================================

        self.add_state(
            TRANSPORT_WAITING,
            NRPElectricTaxiWaitingState(),
            initial=True
        )

        self.add_state(
            TRANSPORT_WAITING_FOR_APPROVAL,
            NPRElectricTaxiWaitingForApprovalState()
        )

        self.add_state(
            TRANSPORT_MOVING_TO_CUSTOMER,
            NPRElectricTaxiMovingToCustomerState()
        )

        self.add_state(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            NPRElectricTaxiArrivedAtCustomerState()
        )

        self.add_state(
            TRANSPORT_MOVING_TO_DESTINATION,
            NPRElectricTaxiMovingToCustomerDestState()
        )

        self.add_state(
            TRANSPORT_ARRIVED_AT_DESTINATION,
            NPRElectricTaxiArrivedAtCustomerDestState()
        )

        self.add_state(
            TRANSPORT_NEEDS_CHARGING,
            NPRElectricTaxiNeedsChargingState()
        )

        self.add_state(
            TRANSPORT_MOVING_TO_STATION,
            NPRElectricTaxiMovingToStationState()
        )

        self.add_state(
            TRANSPORT_IN_STATION_PLACE,
            NPRElectricTaxiInStationState()
        )

        self.add_state(
            TRANSPORT_IN_WAITING_LIST,
            NPRElectricTaxiInWaitingListState()
        )

        self.add_state(
            TRANSPORT_CHARGING,
            NPRElectricTaxiChargingState()
        )

        # ============================================================
        # WAITING
        # ============================================================

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING_FOR_APPROVAL
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # WAITING FOR APPROVAL
        # ============================================================

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_WAITING_FOR_APPROVAL
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_NEEDS_CHARGING
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_MOVING_TO_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_ARRIVED_AT_CUSTOMER
        )

        # ============================================================
        # MOVING TO CUSTOMER
        # ============================================================

        self.add_transition(
            TRANSPORT_MOVING_TO_CUSTOMER,
            TRANSPORT_MOVING_TO_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_CUSTOMER,
            TRANSPORT_ARRIVED_AT_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_CUSTOMER,
            TRANSPORT_WAITING
        )

        # ============================================================
        # ARRIVED AT CUSTOMER
        # ============================================================

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_ARRIVED_AT_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_ARRIVED_AT_DESTINATION
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_WAITING
        )

        # ============================================================
        # MOVING TO DESTINATION
        # ============================================================

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_ARRIVED_AT_DESTINATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_WAITING
        )

        # ============================================================
        # ARRIVED AT DESTINATION
        # ============================================================

        self.add_transition(
            TRANSPORT_ARRIVED_AT_DESTINATION,
            TRANSPORT_ARRIVED_AT_DESTINATION
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_DESTINATION,
            TRANSPORT_WAITING
        )

        # ============================================================
        # NEEDS CHARGING
        # ============================================================

        self.add_transition(
            TRANSPORT_NEEDS_CHARGING,
            TRANSPORT_NEEDS_CHARGING
        )

        self.add_transition(
            TRANSPORT_NEEDS_CHARGING,
            TRANSPORT_MOVING_TO_STATION
        )

        self.add_transition(
            TRANSPORT_NEEDS_CHARGING,
            TRANSPORT_IN_STATION_PLACE
        )

        # ============================================================
        # MOVING TO STATION
        # ============================================================

        self.add_transition(
            TRANSPORT_MOVING_TO_STATION,
            TRANSPORT_MOVING_TO_STATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_STATION,
            TRANSPORT_IN_STATION_PLACE
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_STATION,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # IN STATION PLACE
        # ============================================================

        self.add_transition(
            TRANSPORT_IN_STATION_PLACE,
            TRANSPORT_IN_STATION_PLACE
        )

        self.add_transition(
            TRANSPORT_IN_STATION_PLACE,
            TRANSPORT_IN_WAITING_LIST
        )

        self.add_transition(
            TRANSPORT_IN_STATION_PLACE,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # IN WAITING LIST
        # ============================================================

        self.add_transition(
            TRANSPORT_IN_WAITING_LIST,
            TRANSPORT_IN_WAITING_LIST
        )

        self.add_transition(
            TRANSPORT_IN_WAITING_LIST,
            TRANSPORT_CHARGING
        )

        self.add_transition(
            TRANSPORT_IN_WAITING_LIST,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # CHARGING
        # ============================================================

        self.add_transition(
            TRANSPORT_CHARGING,
            TRANSPORT_CHARGING
        )

        self.add_transition(
            TRANSPORT_CHARGING,
            TRANSPORT_WAITING
        )
