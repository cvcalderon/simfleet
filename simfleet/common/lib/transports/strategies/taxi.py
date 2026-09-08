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
    Base SPADE State shared by the Taxi transport FSM.

    The class provides the common contracts used by all Taxi states:

    - schema-1.0 service lifecycle correlation;
    - canonical service and movement event emission;
    - pending-offer validation before customer acceptance;
    - active-service message correlation after acceptance;
    - transport/customer assignment helpers;
    - REQUEST_PROTOCOL Taxi messaging primitives;
    - optional Taxi return-point requests.

    A customer creates the logical ``service_id``. The Taxi preserves that
    identifier throughout negotiation, approach, customer service, failure,
    and completion so events emitted by different agents can be reconstructed
    as one service by MobilityStatisticsClass.

    This class represents an FSM state helper, not the complete Taxi FSM.
    State registration, transitions, and generic FSM lifecycle events belong
    to FSMTaxiBehaviour and FSMSimfleetBehaviour.
    """


    METRICS_MODALITY = "taxi"
    _METRICS_SERVICE_CONTEXT_ATTR = "_metrics_service_context"
    _METRICS_PENDING_MOVEMENT_ATTR = "_metrics_pending_movement"
    _METRICS_MOVEMENT_PHASES = {"approach", "service", "auxiliary"}

    def _metrics_modality(self):
        """
        Return the canonical modality associated with this strategy family.

        Returns:
            str: Canonical mobility modality.

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
        Return the active canonical service context stored on the Taxi agent.

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
        Create and store one canonical service lifecycle context.

        A transport-side Taxi service normally reuses the ``service_id`` created
        by the customer. Creating a new identifier locally is allowed only when
        ``emit_requested`` is True.

        An existing context is reused only when it still represents the same
        unfinished service. A different or uncleared context is never silently
        overwritten.

        The stored context tracks:

        - service, modality, user, and transport identifiers;
        - origin and destination;
        - requested, assigned, and started lifecycle flags;
        - terminal status;
        - one pending movement.

        When ``emit_requested`` is True, a canonical ``service_requested`` event
        is emitted with origin and destination.

        Args:
            service_id: Existing logical service identifier.
            user_id: Customer/user JID.
            transport_id: Optional Taxi JID.
            origin: Service origin coordinates.
            destination: Service destination coordinates.
            emit_requested (bool): Whether this agent owns and emits the initial
                ``service_requested`` event.

        Returns:
            dict | None: Created or reusable service context, or None when the
            context cannot be established safely.
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
        Return the current matching service context or create a new one.

        When an active context exists, an explicitly supplied ``service_id`` must
        match it. Mismatched service identifiers are rejected rather than
        replacing the active lifecycle.

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
        Clear a finished canonical service context when it is safe to do so.

        An unfinished service is never silently discarded. A context containing
        a pending movement is also retained until that movement is either
        completed or explicitly discarded.

        Returns:
            bool: True when no context remains; False when cleanup is refused
            because the service is unfinished or still owns pending movement.
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
        Build the canonical identifiers shared by service lifecycle events.

        Args:
            context (dict | None): Service context. The active context is used
                when omitted.

        Returns:
            dict | None: Mapping containing modality, service_id, user_id, and
            transport_id.
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
        Add canonical service identifiers to an outgoing payload.

        A new payload mapping is returned. When ``transport_id`` is explicitly
        supplied, the active context is updated with its bare-JID representation
        before identifiers are copied.

        Args:
            content (dict | None): Existing payload fields.
            context (dict | None): Service context to use.
            transport_id: Optional transport identifier to bind to the context.

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
        Validate that a message belongs to the expected canonical service.

        The message must match service ID, modality, and user bare JID. When the
        context already contains a transport identifier, the message must also
        contain and match that transport.

        Args:
            content: Decoded message payload.
            context (dict | None): Expected service context.

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
        Mirror assignment state in the local service context without emitting an
        event.

        The method is idempotent when the same transport is already assigned and
        rejects attempts to rebind the service to another transport.

        Args:
            transport_id: Assigned transport JID.

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
        Mirror service-start state without emitting a canonical event.

        An optional transport identifier must be compatible with any transport
        already stored in the context. Starting a service requires a known
        transport.

        Args:
            transport_id: Optional Taxi JID.

        Returns:
            bool: True when the local context can be marked started.
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

        Taxi uses this method after receiving the customer's final
        ``CUSTOMER_IN_DEST`` confirmation. It changes only local lifecycle state
        and deliberately does not emit another ``service_completed`` event.

        Returns:
            bool: True when completion is valid or was already mirrored.
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
        Mark the service assigned and emit ``service_assigned`` exactly once.

        Args:
            transport_id: Taxi JID assigned to the service.
            extra_details (dict | None): Additional canonical event fields.

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
        Mark passenger service as started and emit ``service_started``.

        The service cannot start after reaching a terminal state and requires a
        known transport identifier.

        Args:
            transport_id: Optional Taxi JID.
            extra_details (dict | None): Additional canonical event fields.

        Returns:
            bool: True when the service-start event was newly emitted.
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
        Mark the active service completed and emit ``service_completed``.

        Completion is accepted only after the service has started and only when no
        terminal status was previously recorded.

        Args:
            extra_details (dict | None): Additional canonical event fields.

        Returns:
            bool: True when completion was newly recorded and emitted.
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
        Mark the active service failed and emit ``service_failed``.

        Failure may occur before or after ``service_started``. Only one terminal
        state may be recorded for a service.

        Args:
            failure_reason (str | None): Optional machine-readable failure reason.
            extra_details (dict | None): Additional canonical event fields.

        Returns:
            bool: True when failure was newly recorded and emitted.
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
        Register one planned movement for deferred canonical emission.

        Movement distance is validated immediately but
        ``movement_completed`` is not emitted until
        ``complete_pending_movement()`` confirms physical completion.

        Supported phases are ``approach``, ``service``, and ``auxiliary``.
        At most one pending movement may exist on the transport.

        By default a movement requires an active service context. Auxiliary Taxi
        return movement may explicitly set ``require_service=False`` and is then
        correlated only with the transport and modality.

        Args:
            phase (str): Canonical movement phase.
            distance_m: Finite non-negative planned distance in metres.
            extra_details (dict | None): Additional movement fields.
            require_service (bool): Whether an active service context is required.

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
        Emit the current pending movement as ``movement_completed``.

        After emission, both the transport-level pending movement and any
        reference held by the active service context are cleared.

        Returns:
            bool: True when a pending movement existed and was emitted.
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
        Clear a pending movement without emitting ``movement_completed``.

        This is used when a planned movement fails or is cancelled before physical
        completion.

        Returns:
            bool: True when a pending movement existed and was discarded.
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


    _METRICS_PENDING_OFFER_ATTR = "_metrics_pending_offer"
    _METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR = "_metrics_active_message_context"

    def _validate_metrics_service_request(self, content):
        """
        Validate a Taxi-like schema-1.0 request before offering service.

        A valid request must provide:

        - service_id;
        - canonical modality;
        - user_id;
        - customer_id;
        - origin;
        - destination in ``dest``.

        ``user_id`` and ``customer_id`` must represent the same bare JID, and the
        request must not already contain a transport assignment.

        Args:
            content: Decoded request payload.

        Returns:
            bool: True when the request can enter Taxi proposal negotiation.
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
        Log entry into one concrete Taxi FSM state.

        Generic ``initial_event`` emission belongs to the surrounding
        FSMTaxiBehaviour through FSMSimfleetBehaviour, not to each individual
        Taxi State.
        """
        logger.debug(
            "Agent[{}]: Strategy {} started.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def on_end(self):
        """
        Log exit from one concrete Taxi FSM state.

        Generic ``final_event`` emission belongs to the complete Taxi FSM rather
        than to individual state transitions.
        """
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
        """
        Bind a customer to the Taxi and mark the transport unavailable.

        Customer assignment data are delegated to TransportAgent's generic
        assigned-customer state. The Taxi is then marked busy.

        Args:
            customer_id: Assigned customer JID.
            origin: Customer pickup coordinates.
            dest: Customer destination coordinates.
        """
        self.agent.add_assigned_customer(
            customer_id,
            origin,
            dest
        )

        self.agent.set_busy()

    async def unassigned_taxicustomer(self):
        """
        Remove the current assigned-customer context from the Taxi.

        Availability is managed separately by the FSM state that performs the
        unassignment.
        """
        self.agent.remove_assigned_customer()


    async def send_proposal(self, customer_id, content=None):
        """
        Send a Taxi service proposal to a customer.

        The message uses REQUEST_PROTOCOL / PROPOSE_PERFORMATIVE. Canonical
        service identifiers are expected to be supplied by the caller, normally
        through ``pending_offer_message_details()``.

        Args:
            customer_id: Customer JID receiving the proposal.
            content (dict | None): Proposal payload.
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
        Cancel an accepted or active Taxi proposal.

        Active service identifiers are added to the payload before a
        REQUEST_PROTOCOL / CANCEL_PERFORMATIVE message is sent.

        Args:
            agent_id: Customer JID receiving the cancellation.
            content (dict | None): Additional cancellation fields.
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
        Inform the active customer of a Taxi service status change.

        Active canonical identifiers are included together with ``status`` in a
        REQUEST_PROTOCOL / INFORM_PERFORMATIVE message.

        Args:
            customer_id: Customer JID receiving the update.
            status: Operational Taxi/customer status.
            data (dict | None): Additional status fields.
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
        Inform the active customer that the current Taxi service is cancelled.

        Active canonical identifiers are propagated through
        REQUEST_PROTOCOL / CANCEL_PERFORMATIVE.

        Args:
            customer_id: Customer JID receiving the cancellation.
            data (dict | None): Additional terminal or failure information.
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
        Request an operational Taxi return point from the registered FleetManager.

        A REQUEST_PROTOCOL / REQUEST_PERFORMATIVE message with
        ``request_type="taxi_return"`` and the Taxi's current position is sent to
        its registration FleetManager.

        If no FleetManager is configured, the request is skipped.

        The response is processed by the Taxi return FSM states; this helper only
        sends the request.
        """
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
        """
        Execute the concrete Taxi FSM state.

        State subclasses implement negotiation, movement monitoring, pickup,
        service progression, return movement, and transitions.

        Raises:
            NotImplementedError: When a concrete Taxi state does not implement
                execution.
        """
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
    Idle and proposal-entry state of the Taxi FSM.

    The Taxi remains available for new service requests and waits for incoming
    REQUEST_PROTOCOL messages.

    A REQUEST_PERFORMATIVE enters negotiation only when its payload satisfies
    the canonical Taxi service-request contract. The validated request is
    stored as a pending offer, a PROPOSE_PERFORMATIVE response is sent to the
    customer, and the FSM advances to
    ``TRANSPORT_WAITING_FOR_APPROVAL``.

    Invalid, unsupported, or absent messages keep the Taxi in
    ``TRANSPORT_WAITING``.
    """
    async def on_start(self):
        """
        Enter the idle Taxi state and mark the transport as waiting.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING

    async def run(self):
        """
        Wait for and validate one new Taxi service request.

        A valid REQUEST_PERFORMATIVE is stored as a pending proposal and receives a
        Taxi PROPOSE_PERFORMATIVE containing canonical service identifiers.

        The Taxi then waits for customer approval. Missing messages, invalid
        schema-1.0 requests, and unsupported performatives remain in the waiting
        state.
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
    Resolve the customer response to a pending Taxi proposal.

    A matching ACCEPT_PERFORMATIVE promotes the pending offer to active
    message context, creates the transport-side canonical service context,
    emits ``service_assigned``, binds the customer to the Taxi, and starts the
    approach movement toward the pickup origin.

    A matching REFUSE_PERFORMATIVE clears the pending offer and returns the
    Taxi to waiting without creating a service lifecycle.

    Stale or mismatched responses are ignored and keep the Taxi waiting for
    resolution of the current proposal.
    """
    async def on_start(self):
        """
        Enter proposal resolution and mark the Taxi as waiting for approval.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING_FOR_APPROVAL

    async def run(self):
        """
        Process acceptance or refusal of the current Taxi proposal.

        On valid acceptance the state:

        1. activates pending message correlation;
        2. creates the Taxi service context from the customer-created service ID;
        3. emits ``service_assigned``;
        4. informs the customer that approach movement is starting;
        5. binds the customer and marks the Taxi busy;
        6. requests a route to the customer origin;
        7. registers that route as pending ``phase="approach"`` movement.

        Successful route planning advances to
        ``TRANSPORT_MOVING_TO_CUSTOMER``.

        If the Taxi is already at the pickup origin, a zero-distance approach
        ``movement_completed`` is emitted and execution advances directly to
        ``TRANSPORT_ARRIVED_AT_CUSTOMER``.

        Route failure or an unexpected approach error emits ``service_failed``,
        cancels the customer interaction, clears service state, restores
        availability, and returns to ``TRANSPORT_WAITING``.

        A valid customer refusal clears only the pending offer and returns to
        waiting.
        """
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
    Monitor Taxi approach movement toward the assigned customer.

    Physical movement is executed independently by MovableMixin's
    MovingBehaviour. This state monitors arrival and listens briefly for
    customer messages while the approach is in progress.

    A matching REFUSE_PERFORMATIVE represents cancellation before pickup. The
    service is failed, the pending approach movement is discarded, assignment
    state is cleared, and the Taxi returns to waiting.

    When physical arrival is confirmed, the pending approach movement is
    emitted as ``movement_completed`` and the customer is informed that the
    Taxi is at the pickup location.
    """
    async def on_start(self):
        """
        Enter approach monitoring and mark the Taxi as moving to the customer.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER
        logger.debug("{} in Transport Moving To Customer State".format(self.agent.jid))

    async def run(self):
        """
        Monitor approach movement until arrival or cancellation.

        While the Taxi has not reached the pickup origin, the state receives
        messages with a short timeout and otherwise remains in
        ``TRANSPORT_MOVING_TO_CUSTOMER``.

        Matching customer refusal emits ``service_failed`` with
        ``customer_cancelled_before_pickup`` and discards the incomplete approach
        movement.

        Physical arrival completes the pending ``approach`` movement, informs the
        customer with ``TRANSPORT_IN_CUSTOMER_PLACE``, and advances to
        ``TRANSPORT_ARRIVED_AT_CUSTOMER``.

        Existing route and unexpected-error recovery returns the Taxi to waiting.
        """
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
    Wait for the customer to board after Taxi pickup arrival.

    The Taxi remains at the pickup location until it receives a message that
    matches the active service.

    ``CUSTOMER_IN_TRANSPORT`` starts passenger service: the customer is moved
    from assigned-customer state into current-customer state,
    ``service_started`` is emitted, and route-based movement to the customer's
    destination begins.

    A matching CANCEL_PERFORMATIVE fails the service at pickup and restores the
    Taxi to waiting.
    """

    async def on_start(self):
        """
        Enter pickup waiting and mark the Taxi as arrived at the customer.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER

    async def run(self):
        """
        Process customer boarding or cancellation at the pickup point.

        INFORM_PERFORMATIVE with ``CUSTOMER_IN_TRANSPORT``:

        - transfers the customer into onboard state;
        - emits ``service_started``;
        - clears the pre-pickup assignment;
        - plans movement to the service destination;
        - registers the route as pending ``phase="service"`` movement.

        Successful planning advances to
        ``TRANSPORT_MOVING_TO_DESTINATION``.

        If the Taxi is already at the customer destination, a zero-distance
        service movement is completed immediately and the FSM advances directly to
        ``TRANSPORT_ARRIVED_AT_DESTINATION``.

        Route failure or an unexpected error emits ``service_failed`` and returns
        the Taxi to waiting.

        A matching CANCEL_PERFORMATIVE fails the service with
        ``customer_cancelled_at_pickup`` and clears customer state.
        """
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
    Monitor passenger transport from pickup origin to service destination.

    MovableMixin performs physical position updates. This FSM state polls
    destination completion while the customer remains onboard.

    Confirmed arrival emits the pending ``phase="service"``
    ``movement_completed`` event and informs the customer with
    ``CUSTOMER_IN_DEST``.

    The Taxi then waits in ``TRANSPORT_ARRIVED_AT_DESTINATION`` for the
    customer's explicit completion confirmation.
    """
    async def on_start(self):
        """
        Enter passenger-trip monitoring and mark the Taxi as moving to destination.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION


    async def run(self):
        """
        Monitor service movement until destination or failure.

        While physical movement remains incomplete, the state sleeps for one
        second and remains active.

        Arrival completes the pending ``service`` movement and informs the
        customer that the destination has been reached.

        Route or unexpected service errors emit ``service_failed``, cancel the
        customer interaction, remove onboard state, restore Taxi availability, and
        return to waiting.

        An AlreadyInDestination path completes an existing pending movement or,
        when none exists, records an explicit zero-distance service movement.
        """
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
    Wait for customer confirmation after physical arrival at destination.

    Taxi arrival alone does not complete the canonical service lifecycle.
    Completion is owned by the customer side.

    When a matching INFORM_PERFORMATIVE confirms ``CUSTOMER_IN_DEST``, the
    Taxi mirrors that already emitted terminal state with
    ``mark_service_completed()``, removes the onboard customer, increments its
    completed-assignment counter, and remains busy while entering the return
    phase.

    A matching cancellation before completion instead emits
    ``service_failed`` and returns directly to normal waiting.
    """
    async def on_start(self):
        """
        Enter destination-confirmation waiting and mark physical Taxi arrival.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION

    async def run(self):
        """
        Resolve the customer-side terminal service message.

        Matching INFORM_PERFORMATIVE with ``CUSTOMER_IN_DEST`` mirrors successful
        completion locally without emitting a second ``service_completed`` event.

        The onboard customer is removed, active message correlation is cleared,
        the completed-assignment counter is incremented, and the Taxi enters
        ``TRANSPORT_WAITING_FOR_RETURN`` while remaining unavailable.

        Matching cancellation emits ``service_failed`` with
        ``customer_cancelled_at_destination`` and restores normal Taxi
        availability.

        Stale service messages and timeouts keep the Taxi in the destination state.
        """
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
    Obtain and start movement toward a FleetManager-assigned Taxi return point.

    The Taxi remains busy and unavailable for customer assignment while this
    state is active.

    If no return point is already stored, a ``taxi_return`` request is sent to
    the Taxi's registered FleetManager. INFORM_PERFORMATIVE responses are
    accepted only when they contain the matching ``request_type`` and a
    ``return_position``.

    Once a return point is available, route movement is started and registered
    as canonical ``phase="auxiliary"`` movement.

    Because the completed service context is currently retained until return
    finishes, this auxiliary movement may inherit that service's canonical
    identifiers.
    """

    async def on_start(self):
        """
        Enter return-point resolution and reset the local request-attempt flag.
        """
        await super().on_start()

        self.agent.status = TRANSPORT_WAITING_FOR_RETURN
        self.return_requested = False

    async def run(self):
        """
        Resolve a Taxi return point and start auxiliary return movement.

        A previously stored return position advances immediately to
        ``TRANSPORT_MOVING_TO_RETURN``.

        Otherwise, the FleetManager is queried at most once per receive cycle.
        Timeout or invalid responses reset the request flag and keep the Taxi in
        return-point waiting.

        A valid point is stored and passed to ``move_to()``. Successful route
        planning registers pending ``phase="auxiliary"`` movement and advances to
        movement monitoring.

        When the Taxi is already at the assigned return point, an explicit
        zero-distance auxiliary movement is emitted, the completed service context
        is cleared, the return point is removed, and the Taxi becomes available.

        Route-resolution errors clear the unusable return point and retry
        FleetManager resolution without failing the already completed customer
        service.
        """
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
    Monitor auxiliary movement toward the FleetManager-assigned return point.

    The Taxi remains unavailable while MovableMixin performs physical return
    movement.

    Arrival completes the pending auxiliary movement, clears the retained
    completed service context and return position, restores normal Taxi
    availability, and transitions to ``TRANSPORT_WAITING``.

    If the return position disappears before arrival, the pending movement is
    discarded and the FSM returns to return-point resolution.
    """

    async def on_start(self):
        """
        Enter return movement monitoring and mark the Taxi as moving to return.
        """
        await super().on_start()

        self.agent.status = TRANSPORT_MOVING_TO_RETURN

    async def run(self):
        """
        Monitor auxiliary Taxi return movement until completion.

        Missing return-point state discards the pending movement and returns to
        ``TRANSPORT_WAITING_FOR_RETURN``.

        While physical movement continues, the state sleeps for one second and
        remains active.

        Confirmed arrival emits the pending ``movement_completed``, clears the
        retained service and return contexts, marks the Taxi available, and returns
        the FSM to ``TRANSPORT_WAITING``.
        """
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
    Finite-state operational strategy for TaxiAgent.

    The FSM implements one complete Taxi service cycle:

    ``TRANSPORT_WAITING``
        Receive and validate customer requests.

    ``TRANSPORT_WAITING_FOR_APPROVAL``
        Resolve a pending Taxi proposal.

    ``TRANSPORT_MOVING_TO_CUSTOMER``
        Monitor approach movement to pickup.

    ``TRANSPORT_ARRIVED_AT_CUSTOMER``
        Wait for customer boarding.

    ``TRANSPORT_MOVING_TO_DESTINATION``
        Monitor passenger-service movement.

    ``TRANSPORT_ARRIVED_AT_DESTINATION``
        Wait for explicit customer completion confirmation.

    ``TRANSPORT_WAITING_FOR_RETURN``
        Resolve a FleetManager-assigned return point.

    ``TRANSPORT_MOVING_TO_RETURN``
        Complete auxiliary return movement before becoming available again.

    Service failures before completion recover directly to
    ``TRANSPORT_WAITING``. Successful services pass through the return cycle
    before the Taxi becomes available for a new assignment.

    Generic FSM lifecycle instrumentation is inherited from
    FSMSimfleetBehaviour.
    """

    def setup(self):
        """
        Register Taxi FSM states and all permitted transitions.
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
