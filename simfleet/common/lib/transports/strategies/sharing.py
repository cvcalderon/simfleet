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
    """
    Base SPADE State shared by the free-floating Sharing transport FSM.

    The transport participates in a customer-owned Sharing service lifecycle
    identified by the customer's ``service_id``.

    Its responsibilities include:

    - validating candidate-specific booking requests;
    - maintaining a transport-side mirror of the canonical service context;
    - accepting or refusing reservations;
    - emitting the public ``service_started`` milestone when the customer
      physically reaches the reserved vehicle;
    - emitting vehicle ``movement_completed`` events for the service leg;
    - mirroring customer-owned terminal completion or failure;
    - propagating canonical service identifiers through transport messages.

    Public assignment is owned by the customer after validating the vehicle's
    ACCEPT response. Public completion is likewise owned by the customer after
    receiving the destination notification.

    This class is an FSM State helper. State registration and transitions
    belong to FSMSharingStrategyBehaviour.
    """

    METRICS_MODALITY = "sharing"
    _METRICS_SERVICE_CONTEXT_ATTR = "_metrics_service_context"
    _METRICS_PENDING_MOVEMENT_ATTR = "_metrics_pending_movement"
    _METRICS_MOVEMENT_PHASES = {"approach", "service", "auxiliary"}

    def _metrics_modality(self):
        """
        Return the canonical free-floating Sharing modality.

        Returns:
            str: ``"sharing"``.

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
        Return the active transport-side Sharing service context.

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
        Create and store one canonical Sharing service context.

        A Sharing transport normally receives the ``service_id`` already created
        by the customer and therefore does not emit ``service_requested``.

        The context correlates the customer, selected transport, origin,
        destination, lifecycle state, and any pending vehicle movement.

        An unfinished or uncleared context is never silently replaced.

        Args:
            service_id: Customer-created logical service identifier.
            user_id: Sharing customer JID.
            transport_id: Sharing transport JID.
            origin: Customer trip origin.
            destination: Final service destination.
            emit_requested (bool): Whether this agent owns the initial request
                event; normally False for Sharing transports.

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
        Return the matching Sharing service context or create one.

        A different ``service_id`` cannot replace an already active context.

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
        Clear a terminal Sharing service context when no movement remains.

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
        Build canonical identifiers shared by Sharing transport events.

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
        Add canonical Sharing identifiers to a copied outgoing payload.

        Args:
            content (dict | None): Existing payload.
            context (dict | None): Service context.
            transport_id: Optional Sharing transport identifier.

        Returns:
            dict: Extended payload.

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
        Validate that a message belongs to the active Sharing service.

        Service ID, modality, customer identity, and any established transport
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
        Mirror customer-owned Sharing assignment without emitting an event.

        The vehicle uses this after accepting a valid booking request. The public
        ``service_assigned`` event is emitted later by the customer after it
        validates the ACCEPT response.

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
        Mirror Sharing service-start state without emitting a public event.

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
        Mirror the customer-owned successful terminal outcome locally.

        The Sharing transport uses this after reaching the service destination and
        notifying the customer with ``CUSTOMER_IN_DEST``.

        No public ``service_completed`` event is emitted here. The customer owns
        that event when it processes the destination notification.

        Returns:
            bool: True when the local terminal state is completed.
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

    def mark_service_failed(self, failure_reason=None):
        """
        Mirror a customer-owned terminal Sharing failure without public emission.

        The transport may establish this local terminal state before sending a
        cancellation message that causes the customer to emit the canonical
        ``service_failed`` event.

        Args:
            failure_reason (str | None): Optional failure reason.

        Returns:
            bool: True when the local terminal state is failed.
        """
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
        """
        Mark a Sharing service assigned and emit ``service_assigned``.

        This generic helper remains available to custom strategies, although the
        current free-floating Sharing transport FSM mirrors customer-owned
        assignment through ``mark_service_assigned()`` instead.

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
        Mark Sharing vehicle use as started and emit ``service_started``.

        The current Sharing transport FSM owns this public milestone. It is emitted
        when the customer physically reaches the booked vehicle and not when the
        vehicle initially accepts the reservation.

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
        Mark the Sharing service completed and emit ``service_completed``.

        The helper is retained for generic or custom strategies. The current
        free-floating Sharing transport FSM does not normally own this public
        milestone; it mirrors customer-owned completion instead.

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
        Mark the Sharing service failed and emit ``service_failed``.

        This public-emission helper remains available to custom strategies. The
        current transport FSM normally mirrors failure locally and propagates a
        cancellation so the customer owns the terminal public event.

        Returns:
            bool: True when failure was newly emitted.
        """
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
        """
        Register one planned Sharing-transport movement for deferred emission.

        The current vehicle FSM uses this for the customer-carrying
        ``phase="service"`` movement.

        ``movement_completed`` is emitted only after physical arrival is
        confirmed.

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
        Emit the pending Sharing-transport movement as ``movement_completed``.

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
        Discard an incomplete Sharing-transport movement without emitting a metric.

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

    def _request_identity_valid(self, content, sender=None):
        """
        Validate a schema-1.0 free-floating Sharing booking request.

        The request must contain service ID, Sharing modality, user ID, and the
        transport ID of this exact vehicle.

        When an XMPP sender is available, its bare JID must match ``user_id``.
        Optional ``customer_id`` must also represent that same customer.

        Args:
            content: Decoded booking request.
            sender: Optional XMPP sender.

        Returns:
            bool: True when the request targets this vehicle and carries a
            consistent customer identity.
        """
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
        """
        Verify that the message payload user matches the XMPP sender.

        Returns:
            bool: True when both represent the same bare JID.
        """
        if not isinstance(content, dict) or content.get("user_id") is None:
            return False
        return self.agent.bare_jid(content.get("user_id")) == self.agent.bare_jid(sender)

    def _service_message_content(self, content=None):
        """
        Build an outgoing Sharing message carrying active service identifiers.

        Args:
            content (dict | None): Additional payload fields.

        Returns:
            dict: Copied payload with canonical service identifiers.

        Raises:
            ValueError: If no active Sharing service context exists.
        """
        context = self.get_service_context()
        if context is None:
            raise ValueError("Cannot build a sharing service message without an open context.")
        result = dict(content or {})
        result.update(self._service_event_details(context))
        return result

    def _request_reply_content(self, request_content, content=None):
        """
        Build a reply to a booking request that is not the active service.

        Canonical identifiers are copied from the incoming request rather than
        from the transport's currently active service context.

        This prevents refusal of a stale or competing booking from being
        correlated with the customer that already owns the reserved vehicle.

        Args:
            request_content: Incoming booking payload.
            content (dict | None): Additional reply fields.

        Returns:
            dict: Reply payload preserving the request's own identifiers.
        """
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
        """
        Legacy helper for directly starting Sharing transport with a customer.

        The helper stores the customer as onboard and attempts to start movement
        toward the supplied destination.

        The current free-floating Sharing FSM does not use this helper; booking,
        service start, and movement are implemented explicitly in
        SharingBookedState.

        Args:
            customer_id: Customer JID.
            origin: Customer origin.
            dest: Customer destination.
        """
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
        """
        Accept one free-floating Sharing booking request.

        REQUEST_PROTOCOL / ACCEPT_PERFORMATIVE is sent to the requesting customer
        with the active service identifiers, this transport's JID, and current
        position.

        The method itself does not emit ``service_assigned``; assignment ownership
        belongs to the customer after it validates this ACCEPT.

        Args:
            customer_id: Customer JID.
        """
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
        """
        Refuse one Sharing booking request.

        When refusing a request for the currently active service, the active
        service identifiers are used.

        When ``request_content`` is supplied, identifiers are copied from that
        request instead. This allows the vehicle to reject stale or competing
        booking attempts without associating them with its active customer.

        Args:
            customer_id: Customer JID.
            request_content (dict | None): Original request when refusing a
                non-active or competing booking.
        """
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
        """
        Inform the active Sharing customer of a transport status change.

        REQUEST_PROTOCOL / INFORM_PERFORMATIVE carries the active service
        identifiers together with ``status``.

        Args:
            customer_id: Customer JID.
            status: Operational Sharing status.
            data (dict | None): Additional payload fields.
        """
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
        """
        Notify the active Sharing customer that service execution is cancelled.

        REQUEST_PROTOCOL / CANCEL_PERFORMATIVE propagates the active service
        identifiers and any failure information.

        The method does not itself emit ``service_failed``.

        Args:
            customer_id: Customer JID.
            data (dict | None): Failure or cancellation fields.
        """
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
        Legacy helper for clearing historical Sharing customer-assignment fields.

        The current free-floating Sharing FSM does not use this method; active
        assignment and onboard-customer state are managed through the generic
        TransportAgent customer mappings.

        The helper is retained temporarily for compatibility review.
        """
        # Delete saved values (destination, etc.) belonging to booked customer
        self.agent.set("current_customer", None)
        self.agent.current_customer_orig = None
        self.agent.current_customer_dest = None

    async def run(self):
        """
        Execute the concrete Sharing transport FSM state.

        Raises:
            NotImplementedError: When a concrete state does not implement
                execution.
        """
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
    """
    Idle booking-entry state of a free-floating Sharing transport.

    The vehicle waits for REQUEST_PROTOCOL / PROPOSE_PERFORMATIVE booking
    requests sent directly by Sharing customers.

    A request is accepted only when its canonical service identifiers,
    customer identity, XMPP sender, and requested transport all identify this
    exact vehicle.

    A valid booking creates the transport-side mirror of the customer-owned
    service context, marks assignment locally without emitting
    ``service_assigned``, stores the customer as assigned, marks the vehicle
    busy, and sends ACCEPT_PERFORMATIVE.

    Execution then advances to ``TRANSPORT_BOOKED``.

    Missing, malformed, stale, unsupported, or unsafe requests leave the
    vehicle in ``TRANSPORT_WAITING``.
    """

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING
        logger.debug(
            "Agent[{}]: The sharing transport is waiting.".format(
                self.agent.name
            )
        )

    async def run(self):
        """
        Receive and validate one Sharing booking request.

        Only REQUEST_PROTOCOL / PROPOSE_PERFORMATIVE messages may create a
        reservation.

        A valid request:

        1. establishes the transport-side service context using the
           customer-created ``service_id``;
        2. mirrors assignment through ``mark_service_assigned()``;
        3. stores the customer in assigned-customer state;
        4. marks the Sharing vehicle busy;
        5. sends ACCEPT_PERFORMATIVE to the customer;
        6. transitions to ``TRANSPORT_BOOKED``.

        No public ``service_assigned`` event is emitted by this state; assignment
        ownership belongs to the customer after it validates the ACCEPT response.
        """
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
    """
    Hold a reserved Sharing vehicle while waiting for its assigned customer.

    The vehicle remains unavailable after accepting a booking.

    Competing PROPOSE_PERFORMATIVE requests may arrive from customers holding
    stale candidate lists. Those requests are refused using their own
    canonical identifiers and never replace or contaminate the active service
    context.

    A matching customer cancellation terminates the local service mirror and
    makes the vehicle available again.

    A matching INFORM_PERFORMATIVE from the assigned customer is interpreted
    by the current implementation as physical arrival at the reserved vehicle.
    The transport then emits ``service_started`` and begins movement toward
    the customer's final destination.

    Successful route setup advances to
    ``TRANSPORT_MOVING_TO_DESTINATION``. Zero-distance service and route
    failures return directly to ``TRANSPORT_WAITING`` after their respective
    terminal processing.
    """

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_BOOKED
        logger.debug(
            "Agent[{}]: The sharing transport is booked.".format(
                self.agent.name
            )
        )

    async def run(self):
        """
        Resolve customer arrival, cancellation, or competing booking requests.

        A competing valid PROPOSE_PERFORMATIVE is refused using identifiers copied
        from that request while the active reservation remains unchanged.

        A matching CANCEL_PERFORMATIVE fails the local service mirror, clears the
        assigned customer and service context, restores availability, and returns
        to ``TRANSPORT_WAITING``.

        A valid INFORM_PERFORMATIVE from the assigned customer starts vehicle use:

        1. ``service_started`` is emitted;
        2. the customer moves from assigned to onboard state;
        3. movement to the final destination is requested;
        4. the routed distance is registered as pending ``phase="service"``
           movement.

        Successful route setup informs the customer with
        ``CUSTOMER_IN_TRANSPORT`` and advances to
        ``TRANSPORT_MOVING_TO_DESTINATION``.

        When origin and destination already coincide, an explicit zero-distance
        service movement is completed and the transport locally mirrors successful
        completion before returning directly to waiting.

        Route and unexpected setup failures mirror local failure, notify the
        customer through cancellation, clear service state, and restore vehicle
        availability.
        """
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
    """
    Monitor active free-floating Sharing movement to the customer destination.

    MovableMixin performs physical vehicle movement while this state tracks
    completion and handles messages relevant to the active service.

    Competing booking proposals are refused without affecting the onboard
    customer.

    A matching customer cancellation mirrors local service failure, discards
    the incomplete movement, removes the onboard customer, clears service
    state, and restores vehicle availability.

    Physical arrival emits the pending ``phase="service"``
    ``movement_completed`` event. The customer is then informed with
    ``CUSTOMER_IN_DEST`` and the transport mirrors successful completion
    locally before returning directly to ``TRANSPORT_WAITING``.
    """

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
        logger.debug(
            "Agent[{}]: The sharing transport is moving to the customer destination.".format(
                self.agent.name
            )
        )

    async def run(self):
        """
        Monitor Sharing service movement until destination, cancellation, or
        recovery.

        Missing onboard-customer or service context state triggers defensive
        recovery to ``TRANSPORT_WAITING``.

        While physical movement remains incomplete, competing valid booking
        proposals are refused and a matching cancellation from the active customer
        terminates the local service mirror.

        Confirmed physical arrival requires an existing pending movement. That
        movement is emitted as ``movement_completed`` before the customer receives
        ``CUSTOMER_IN_DEST``.

        The transport then mirrors customer-owned completion, removes onboard
        state, increments completed assignments, clears the service context,
        restores availability, and returns to waiting.

        Missing canonical movement at physical arrival is treated as
        ``service_movement_missing`` and propagated to the customer as a terminal
        cancellation.
        """
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
    Finite-state operational strategy for a free-floating Sharing transport.

    The FSM contains three states:

    ``TRANSPORT_WAITING``
        Accept one valid direct customer booking.

    ``TRANSPORT_BOOKED``
        Reserve the vehicle for that customer, reject competing bookings, and
        wait for the customer to reach the vehicle.

    ``TRANSPORT_MOVING_TO_DESTINATION``
        Execute and monitor the customer-carrying vehicle movement.

    The customer owns public ``service_assigned`` and terminal
    ``service_completed`` / ``service_failed`` events. The transport owns
    public ``service_started`` and vehicle ``movement_completed``.

    Zero-distance service may terminate directly from
    ``TRANSPORT_BOOKED`` to ``TRANSPORT_WAITING`` without entering the moving
    state.

    Service cancellation and failure also recover directly to waiting after
    cleanup.

    Generic FSM lifecycle instrumentation is inherited from
    FSMSimfleetBehaviour.
    """

    def setup(self):
        """
        Register free-floating Sharing transport states and permitted transitions.
        """
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


