import math
from uuid import uuid4
import asyncio
import json

from loguru import logger
from spade.behaviour import State
from spade.message import Message

from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    PROPOSE_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination,
)

from simfleet.utils.status import (
    TRANSPORT_WAITING,
    TRANSPORT_WAITING_FOR_APPROVAL,
    TRANSPORT_MOVING_TO_CUSTOMER,
    TRANSPORT_ARRIVED_AT_CUSTOMER,
    TRANSPORT_IN_CUSTOMER_PLACE,
    TRANSPORT_MOVING_TO_DESTINATION,
    TRANSPORT_ARRIVED_AT_DESTINATION,
    CUSTOMER_IN_TRANSPORT,
    CUSTOMER_IN_DEST,
)


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class DeliveryStrategyBehaviour(State):
    """
    Base SPADE State shared by the Delivery transport FSM.

    The class provides the common contracts used by Delivery states:

    - schema-1.0 service lifecycle correlation;
    - canonical service and movement event emission;
    - pending-offer validation before requester acceptance;
    - active-service message correlation after acceptance;
    - requester assignment helpers;
    - REQUEST_PROTOCOL messaging primitives.

    Delivery follows the same canonical service lifecycle structure used by
    Taxi, but declares ``modality="delivery"`` and has no post-service Taxi
    return phase.

    The service requester creates the logical ``service_id``. Delivery
    preserves that identifier throughout negotiation, approach, service
    execution, failure, and completion so MobilityStatisticsClass can
    reconstruct the complete lifecycle.

    This class is an FSM state helper, not the complete Delivery FSM.
    """

    METRICS_MODALITY = "delivery"
    _METRICS_SERVICE_CONTEXT_ATTR = "_metrics_service_context"
    _METRICS_PENDING_MOVEMENT_ATTR = "_metrics_pending_movement"
    _METRICS_MOVEMENT_PHASES = {"approach", "service", "auxiliary"}

    def _metrics_modality(self):
        """
        Return the canonical modality associated with Delivery metrics.

        Returns:
            str: ``"delivery"``.

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
        Return the active canonical Delivery service context.

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
        Create and store one canonical Delivery service context.

        Transport-side Delivery normally reuses the ``service_id`` created by the
        requester. A new identifier is generated locally only when
        ``emit_requested`` is True.

        An unfinished or uncleared context is never silently replaced.

        The context tracks service identifiers, origin and destination, lifecycle
        flags, terminal status, and any pending canonical movement.

        Args:
            service_id: Existing logical service identifier.
            user_id: Service-requester JID.
            transport_id: Optional Delivery transport JID.
            origin: Collection or service origin.
            destination: Final service destination.
            emit_requested (bool): Whether this agent emits the initial
                ``service_requested`` event.

        Returns:
            dict | None: Created or reusable service context, or None when a safe
            context cannot be established.
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

        An explicitly supplied ``service_id`` must match an already active
        context; mismatched services are rejected instead of replacing it.

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
        Clear a terminal Delivery service context when no movement remains.

        An unfinished lifecycle or a service that still owns a pending movement
        is deliberately retained.

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
        Build canonical identifiers shared by Delivery lifecycle events.

        Args:
            context (dict | None): Service context. The active context is used
                when omitted.

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
            context (dict | None): Service context to use.
            transport_id: Optional transport identifier to bind.

        Returns:
            dict: Payload extended with canonical identifiers.

        Raises:
            ValueError: If no service context exists.
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
        Validate that a message belongs to the expected Delivery service.

        Service ID, modality, and requester JID must match. When the context
        already contains a transport identifier, that identifier must also be
        supplied and match.

        Args:
            content: Decoded message payload.
            context (dict | None): Expected service context.

        Returns:
            bool: True when the message belongs to the service.
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
        Mirror Delivery assignment state without emitting an event.

        Args:
            transport_id: Assigned Delivery transport JID.

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
        Mirror Delivery service-start state without emitting an event.

        Args:
            transport_id: Optional Delivery transport JID.

        Returns:
            bool: True when the context can be marked started.
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
        Mirror successful completion already emitted by the service requester.

        Delivery uses this after receiving the final ``CUSTOMER_IN_DEST``
        confirmation. No second ``service_completed`` event is emitted.

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
            transport_id: Delivery transport JID.
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
        Mark Delivery service execution as started and emit ``service_started``.

        Args:
            transport_id: Optional Delivery transport JID.
            extra_details (dict | None): Additional canonical fields.

        Returns:
            bool: True when the event was newly emitted.
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

        The helper requires an already started service and is retained as part of
        the generic Delivery metrics contract, even though the current FSM
        normally mirrors requester-owned completion through
        ``mark_service_completed()``.

        Args:
            extra_details (dict | None): Additional canonical fields.

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
        Mark the active Delivery service failed and emit ``service_failed``.

        Failure may occur before or after service start, but only one terminal
        state may be stored.

        Args:
            failure_reason (str | None): Machine-readable failure reason.
            extra_details (dict | None): Additional canonical fields.

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
        Register one planned Delivery movement for deferred metric emission.

        ``movement_completed`` is emitted only after physical movement is
        confirmed by ``complete_pending_movement()``.

        Supported canonical phases are ``approach``, ``service``, and
        ``auxiliary``. At most one pending movement is allowed.

        Args:
            phase (str): Canonical movement phase.
            distance_m: Finite non-negative planned distance in metres.
            extra_details (dict | None): Additional movement fields.
            require_service (bool): Whether an active service context is required.

        Returns:
            bool: True when the movement was registered.

        Raises:
            ValueError: If phase or distance violates the movement contract.
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
        Emit the pending Delivery movement as ``movement_completed``.

        The pending movement is removed from both transport and service context
        after emission.

        Returns:
            bool: True when one movement existed and was emitted.
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
        Discard an incomplete planned movement without emitting a metric.

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


    _METRICS_PENDING_OFFER_ATTR = "_metrics_pending_offer"
    _METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR = "_metrics_active_message_context"

    def _validate_metrics_service_request(self, content):
        """
        Validate a schema-1.0 Delivery request before sending a proposal.

        Required fields are ``service_id``, ``modality``, ``user_id``,
        ``customer_id``, ``origin``, and ``dest``.

        User and customer identifiers must represent the same bare JID, the
        modality must be ``delivery``, and the initial request must not already
        contain a transport assignment.

        Args:
            content: Decoded service request.

        Returns:
            bool: True when the request may enter Delivery negotiation.
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
        Store one validated Delivery request while its proposal is unresolved.

        The pending context binds the current Delivery transport as proposed
        ``transport_id`` without yet creating the active lifecycle context.

        Args:
            content (dict): Validated request payload.

        Returns:
            dict | None: Pending proposal context.
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
        Return the Delivery proposal currently awaiting resolution.

        Returns:
            dict | None: Pending proposal context.
        """
        return getattr(self.agent, self._METRICS_PENDING_OFFER_ATTR, None)

    def clear_pending_offer(self):
        """
        Clear the currently pending Delivery proposal.

        Returns:
            bool: True after cleanup.
        """
        setattr(self.agent, self._METRICS_PENDING_OFFER_ATTR, None)
        return True

    def pending_offer_message_details(self):
        """
        Build canonical identifiers propagated with a Delivery proposal.

        Returns:
            dict | None: Service, modality, requester, and transport identifiers.
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
        Validate a requester response against the pending Delivery proposal.

        Service, modality, user, transport, and customer identifiers must match
        the stored proposal.

        Args:
            content: Decoded response payload.

        Returns:
            bool: True when the response belongs to the pending proposal.
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

        Args:
            content: Matching requester response.

        Returns:
            dict | None: Newly activated message context.
        """
        if not self.message_matches_pending_offer(content):
            return None
        pending = dict(self.get_pending_offer())
        setattr(self.agent, self._METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR, pending)
        self.clear_pending_offer()
        return pending

    def get_active_message_context(self):
        """
        Return message-correlation state for the accepted Delivery service.

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
        Add active Delivery service identifiers to a copied payload.

        Args:
            content (dict | None): Existing payload.

        Returns:
            dict: Extended or unchanged copied payload.
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
        Add active Delivery service identifiers to a copied payload.

        Args:
            content (dict | None): Existing payload.

        Returns:
            dict: Extended or unchanged copied payload.
        """
        details = self.active_service_message_details()
        if details is None:
            return dict(content or {})
        result = dict(content or {})
        result.update(details)
        return result

    def message_matches_active_service(self, content):
        """
        Validate that a message belongs to the accepted Delivery service.

        Args:
            content: Decoded message payload.

        Returns:
            bool: True when service, modality, user, and transport identifiers
            match the active context.
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
        Clear message-correlation state for the accepted Delivery service.

        Returns:
            bool: True after cleanup.
        """
        setattr(self.agent, self._METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR, None)
        return True

    async def on_start(self):
        """
        Log entry into one concrete Delivery FSM state.

        Generic FSM lifecycle instrumentation belongs to FSMDeliveryBehaviour
        rather than to individual State transitions.
        """
        logger.debug(
            "Agent[{}]: Strategy {} started.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def on_end(self):
        """
        Log exit from one concrete Delivery FSM state.
        """
        logger.debug(
            "Agent[{}]: Strategy {} finished.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def assigned_customer(self, customer_id, origin=None, dest=None):
        """
        Bind one requester/customer to the Delivery transport and mark it busy.

        Args:
            customer_id: Assigned requester JID.
            origin: Service collection origin.
            dest: Service destination.
        """
        self.agent.add_assigned_customer(
            customer_id,
            origin,
            dest
        )

        self.agent.set_busy()

    async def unassigned_customer(self):
        """
        Remove the current pre-service assigned-customer context.

        Transport availability is managed separately by the concrete FSM state.
        """
        self.agent.remove_assigned_customer()

    # ======== Comunications ========

    async def send_proposal(self, customer_id, content=None):
        """
        Send a Delivery proposal through REQUEST_PROTOCOL / PROPOSE_PERFORMATIVE.

        Args:
            customer_id: Requester JID.
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
        Cancel an accepted Delivery proposal.

        Active canonical service identifiers are added before sending
        REQUEST_PROTOCOL / CANCEL_PERFORMATIVE.

        Args:
            agent_id: Requester JID.
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
        Inform the requester of a Delivery service-status change.

        Active service identifiers and ``status`` are sent through
        REQUEST_PROTOCOL / INFORM_PERFORMATIVE.

        Args:
            customer_id: Requester JID.
            status: Operational service status.
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
        Inform the active requester that Delivery service execution is cancelled.

        Active canonical identifiers are propagated with
        REQUEST_PROTOCOL / CANCEL_PERFORMATIVE.

        Args:
            customer_id: Requester JID.
            data (dict | None): Failure or cancellation information.
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

    async def run(self):
        """
        Execute the concrete Delivery FSM state.

        Raises:
            NotImplementedError: When a concrete Delivery state does not provide
                an implementation.
        """
        raise NotImplementedError

# ==================================================================
# -------------------------End Behaviour----------------------------
# ==================================================================


################################################################
#                                                              #
#                      Delivery Strategy                       #
#                                                              #
################################################################

class DeliveryWaitingState(DeliveryStrategyBehaviour):
    """
    Idle and proposal-entry state of the Delivery FSM.

    The transport waits for REQUEST_PROTOCOL messages while no Delivery
    service is active.

    A REQUEST_PERFORMATIVE enters negotiation only when its payload satisfies
    the canonical Delivery service-request contract. The validated request is
    stored as a pending offer, a PROPOSE_PERFORMATIVE is sent to the requester,
    and execution advances to ``TRANSPORT_WAITING_FOR_APPROVAL``.

    Missing, invalid, or unsupported messages keep the Delivery transport in
    ``TRANSPORT_WAITING``.
    """
    async def on_start(self):
        """
        Enter the idle Delivery state and mark the transport as waiting.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING

    async def run(self):
        """
        Wait for and validate one new Delivery service request.

        A valid REQUEST_PERFORMATIVE is stored as a pending proposal and receives
        a PROPOSE_PERFORMATIVE containing canonical service identifiers.

        The FSM then advances to ``TRANSPORT_WAITING_FOR_APPROVAL``. Missing
        messages, invalid schema-1.0 requests, and unsupported performatives remain
        in the waiting state.
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


class DeliveryWaitingForApprovalState(DeliveryStrategyBehaviour):
    """
    Resolve the requester response to a pending Delivery proposal.

    A matching ACCEPT_PERFORMATIVE promotes the pending offer to active
    message context, creates the transport-side canonical service context,
    emits ``service_assigned``, binds the requester to the transport, and
    starts approach movement toward the service origin.

    A matching REFUSE_PERFORMATIVE clears the pending proposal and returns to
    waiting without creating an active service lifecycle.

    Stale or mismatched responses are ignored while the current proposal
    remains unresolved.
    """
    async def on_start(self):
        """
        Enter proposal resolution and mark Delivery as waiting for approval.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING_FOR_APPROVAL

    async def run(self):
        """
        Process acceptance or refusal of the current Delivery proposal.

        On valid acceptance the state:

        1. activates pending message correlation;
        2. creates the Delivery service context using the requester-created
           service ID;
        3. emits ``service_assigned``;
        4. informs the requester that approach movement is starting;
        5. stores requester assignment and marks the transport busy;
        6. requests a route to the service origin;
        7. registers that route as pending ``phase="approach"`` movement.

        Successful route planning advances to
        ``TRANSPORT_MOVING_TO_CUSTOMER``.

        If the transport is already at the service origin, an explicit
        zero-distance approach movement is completed and execution advances
        directly to ``TRANSPORT_ARRIVED_AT_CUSTOMER``.

        Route failure or an unexpected approach error emits ``service_failed``,
        cancels the requester interaction, clears transient service state,
        restores availability, and returns to ``TRANSPORT_WAITING``.

        A matching refusal clears the pending proposal and returns to waiting.
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
                    raise RuntimeError("Unable to create Delivery metrics service context.")
                if not self.assign_service(self.agent.jid):
                    raise RuntimeError("Unable to emit Delivery service assignment.")
                logger.debug(
                    "Agent[{}]: The agent got accept from [{}]".format(
                        self.agent.name, content["customer_id"]
                    )
                )


                await self.inform_customer(
                    customer_id=content["customer_id"], status=TRANSPORT_MOVING_TO_CUSTOMER
                )


                await self.assigned_customer(
                    customer_id=content["customer_id"],
                    origin=content["origin"],
                    dest=content["dest"]
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
                        "Agent[{}]: Could not register Delivery approach movement.".format(
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

class DeliveryMovingToCustomerState(DeliveryStrategyBehaviour):
    """
    Monitor Delivery approach movement toward the service origin.

    Physical movement is executed by MovableMixin's MovingBehaviour. This
    state monitors arrival and briefly processes messages while approach
    movement remains active.

    A matching REFUSE_PERFORMATIVE represents cancellation before service
    collection. The lifecycle is failed, the incomplete approach movement is
    discarded, requester assignment is removed, and the transport returns to
    waiting.

    Confirmed physical arrival emits the pending ``phase="approach"``
    ``movement_completed`` event and informs the requester that the transport
    has reached the service origin.
    """
    async def on_start(self):
        """
        Enter approach monitoring and mark Delivery as moving to the service
        origin.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER
        logger.debug("{} in Transport Moving To Customer State".format(self.agent.jid))

    async def run(self):
        """
        Monitor approach movement until arrival or cancellation.

        While the transport has not reached the service origin, messages are
        received with a short timeout and execution otherwise remains in
        ``TRANSPORT_MOVING_TO_CUSTOMER``.

        Matching requester refusal emits ``service_failed`` with
        ``customer_cancelled_before_pickup`` and discards the incomplete approach
        movement.

        Confirmed arrival completes the pending approach movement, informs the
        requester with ``TRANSPORT_IN_CUSTOMER_PLACE``, and advances to
        ``TRANSPORT_ARRIVED_AT_CUSTOMER``.

        Existing route and unexpected-error recovery returns the transport to
        normal waiting.
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


class DeliveryArrivedAtCustomerState(DeliveryStrategyBehaviour):
    """
    Wait for service-start confirmation at the Delivery origin.

    A matching INFORM_PERFORMATIVE carrying ``CUSTOMER_IN_TRANSPORT`` begins
    the actual Delivery service. The requester is transferred from assigned
    state into current-service state, ``service_started`` is emitted, and
    movement toward the configured service destination begins.

    A matching CANCEL_PERFORMATIVE fails the service at its origin, clears
    requester state, restores transport availability, and returns to waiting.
    """

    async def on_start(self):
        """
        Enter service-origin waiting and mark Delivery as arrived at the requester.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER


    async def run(self):
        """
        Process Delivery service start or cancellation at the origin.

        INFORM_PERFORMATIVE with ``CUSTOMER_IN_TRANSPORT``:

        - moves the requester into current-service state;
        - emits ``service_started``;
        - removes the pre-service assignment;
        - resolves a route to the Delivery destination;
        - registers that route as pending ``phase="service"`` movement.

        Successful route planning advances to
        ``TRANSPORT_MOVING_TO_DESTINATION``.

        If origin and destination coincide, a zero-distance service movement is
        completed immediately and execution advances directly to
        ``TRANSPORT_ARRIVED_AT_DESTINATION``.

        Route failure or unexpected error emits ``service_failed``, clears the
        active service, restores availability, and returns to waiting.

        A matching cancellation fails the service with
        ``customer_cancelled_at_pickup``.
        """
        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
            return
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")

        if performative in (INFORM_PERFORMATIVE, CANCEL_PERFORMATIVE) and not self.message_matches_active_service(content):
            logger.warning(
                "Agent[{}]: Ignoring stale message at delivery collection.".format(
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
                            raise RuntimeError("Unable to emit Delivery service start.")


                        await self.unassigned_customer()

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
                                "Agent[{}]: Could not register Delivery service movement.".format(
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


class DeliveryMovingToCustomerDestState(DeliveryStrategyBehaviour):
    """
    Monitor active Delivery service movement toward its destination.

    MovableMixin performs the physical position updates while this state polls
    for destination completion.

    Confirmed arrival emits the pending ``phase="service"``
    ``movement_completed`` event and informs the requester with
    ``CUSTOMER_IN_DEST``.

    The FSM then enters ``TRANSPORT_ARRIVED_AT_DESTINATION`` and waits for
    explicit requester-side lifecycle completion confirmation.
    """
    async def on_start(self):
        """
        Enter service-movement monitoring and mark Delivery as moving to
        destination.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION


    async def run(self):
        """
        Monitor Delivery service movement until destination or failure.

        Incomplete movement remains in
        ``TRANSPORT_MOVING_TO_DESTINATION`` after a one-second asynchronous wait.

        Confirmed physical arrival completes the pending service movement and
        informs the requester that the destination has been reached.

        Route failure or an unexpected service error emits ``service_failed``,
        cancels the requester interaction, removes current-service state, restores
        transport availability, and returns to waiting.

        An AlreadyInDestination path completes an existing pending movement or
        emits an explicit zero-distance service movement when none remains.
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

class DeliveryArrivedAtCustomerDestState(DeliveryStrategyBehaviour):
    """
    Wait for requester confirmation after physical Delivery arrival.

    Physical arrival alone does not complete the canonical service lifecycle.
    Completion is owned by the requester-side strategy.

    A matching INFORM_PERFORMATIVE with ``CUSTOMER_IN_DEST`` mirrors that
    completed lifecycle locally, removes current-service state, clears service
    and message-correlation contexts, increments completed assignments,
    restores transport availability, and returns directly to
    ``TRANSPORT_WAITING``.

    A matching cancellation instead emits ``service_failed`` and performs the
    same operational cleanup without incrementing successful assignments.
    """
    async def on_start(self):
        """
        Enter destination-confirmation waiting and mark physical Delivery arrival.
        """
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION


    async def run(self):
        """
        Resolve the requester-side terminal Delivery message.

        Matching INFORM_PERFORMATIVE with ``CUSTOMER_IN_DEST`` mirrors successful
        completion through ``mark_service_completed()`` without emitting a second
        ``service_completed`` event.

        The current requester, canonical service context, and active message
        context are then cleared. The completed-assignment counter is incremented,
        the transport becomes available immediately, and the FSM returns to
        ``TRANSPORT_WAITING``.

        Matching CANCEL_PERFORMATIVE emits ``service_failed`` with
        ``customer_cancelled_at_destination`` and performs terminal cleanup.

        Timeouts, unsupported messages, and stale terminal messages remain in
        ``TRANSPORT_ARRIVED_AT_DESTINATION``.
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
                    "Agent[{}]: Ignoring stale message at delivery completion.".format(
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
                                "Agent[{}]: Could not mirror completed Delivery service.".format(
                                    self.agent.name
                                )
                            )
                        self.agent.remove_customer_in_transport(customer_id)
                        self.clear_service_context()
                        self.clear_active_message_context()

                        self.agent.increment_completed_assignments()
                        self.agent.set_available()

                        logger.debug(
                            "Agent[{}]: The agent has dropped the customer [{}] in destination.".format(
                                self.agent.agent_id, customer_id
                            )
                        )
                        self.agent.status = TRANSPORT_WAITING
                        self.set_next_state(TRANSPORT_WAITING)
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


class FSMDeliveryBehaviour(FSMSimfleetBehaviour):
    """
    Finite-state operational strategy for Delivery transport agents.

    The FSM implements one complete Delivery service cycle:

    ``TRANSPORT_WAITING``
        Receive and validate new service requests.

    ``TRANSPORT_WAITING_FOR_APPROVAL``
        Resolve a pending proposal.

    ``TRANSPORT_MOVING_TO_CUSTOMER``
        Monitor approach movement to the service origin.

    ``TRANSPORT_ARRIVED_AT_CUSTOMER``
        Wait for service-start confirmation.

    ``TRANSPORT_MOVING_TO_DESTINATION``
        Monitor Delivery service movement.

    ``TRANSPORT_ARRIVED_AT_DESTINATION``
        Wait for explicit requester completion confirmation.

    Successful completion returns directly to ``TRANSPORT_WAITING`` and makes
    the Delivery transport available again. There is no Taxi-style return
    phase.

    Service failures similarly recover to ``TRANSPORT_WAITING`` after
    lifecycle and requester-state cleanup.

    Generic FSM lifecycle instrumentation is inherited from
    FSMSimfleetBehaviour.
    """

    def setup(self):
        """
        Register Delivery FSM states and permitted transitions.
        """

        # Add states to the FSM
        self.add_state(TRANSPORT_WAITING, DeliveryWaitingState(), initial=True)
        self.add_state(TRANSPORT_WAITING_FOR_APPROVAL, DeliveryWaitingForApprovalState())
        self.add_state(TRANSPORT_MOVING_TO_CUSTOMER, DeliveryMovingToCustomerState())
        self.add_state(TRANSPORT_ARRIVED_AT_CUSTOMER, DeliveryArrivedAtCustomerState())
        self.add_state(TRANSPORT_MOVING_TO_DESTINATION, DeliveryMovingToCustomerDestState())
        self.add_state(TRANSPORT_ARRIVED_AT_DESTINATION, DeliveryArrivedAtCustomerDestState())

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

        # Additional transitions for customer movement and destination states
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_MOVING_TO_CUSTOMER)  # Still en route to customer
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_WAITING)  # Return to waiting if issue arises
