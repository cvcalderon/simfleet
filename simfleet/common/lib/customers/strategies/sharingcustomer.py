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
    """
    Base SPADE State shared by the free-floating Sharing customer FSM.

    The customer owns the public lifecycle of a Sharing mobility request:

    - creation of the logical ``service_id`` and ``service_requested``;
    - validation of the selected vehicle and ``service_assigned``;
    - walking approach movement toward the reserved vehicle;
    - terminal ``service_completed`` or ``service_failed``.

    The Sharing transport owns the public ``service_started`` milestone and
    vehicle-service movement. The customer mirrors that start locally when it
    receives the corresponding transport status.

    Candidate discovery is performed through Sharing FleetManagers. A customer
    may receive several vehicle candidates, filter them by walking
    reachability, select the nearest remaining candidate, and retry another
    candidate after a booking refusal without creating a second service ID.

    This class provides service-correlation, movement-metric, candidate
    booking, and transport-messaging helpers. It is an FSM State helper; the
    complete customer FSM is defined by FSMSharingCustomerStrategyBehaviour.
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
        Return the active canonical Sharing service context.

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

        The Sharing customer normally creates the ``service_id`` when candidate
        discovery begins and emits ``service_requested`` at that point.

        The same context survives candidate refusals so retries do not create
        additional logical services.

        Args:
            service_id: Optional existing logical service identifier.
            user_id: Sharing customer JID.
            transport_id: Optional selected Sharing transport JID.
            origin: Customer trip origin.
            destination: Final customer destination.
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
        Return the matching active Sharing service context or create one.

        Candidate retries reuse the existing context and therefore preserve the
        original ``service_id``.

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
        Build canonical identifiers shared by Sharing service events.

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
        Add canonical Sharing service identifiers to a copied payload.

        Args:
            content (dict | None): Existing payload.
            context (dict | None): Service context.
            transport_id: Optional Sharing transport identifier.

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
        Validate that a message belongs to the expected Sharing service.

        Service ID, modality, user identifier, and any already assigned transport
        identifier must match the local context.

        Returns:
            bool: True when the message belongs to the active service.
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
        Mirror Sharing assignment state without emitting ``service_assigned``.

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
        Mirror Sharing service start emitted by the reserved transport.

        The customer uses this when receiving transport status indicating that
        vehicle use has started. No duplicate ``service_started`` event is emitted.

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

    def assign_service(self, transport_id, extra_details=None):
        """
        Bind the accepted Sharing vehicle and emit ``service_assigned`` once.

        The customer owns this public assignment milestone because it is emitted
        only after validating the selected vehicle's ACCEPT response.

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
        Mark Sharing service use as started and emit ``service_started``.

        The helper remains part of the generic metrics contract, although the
        current Sharing customer FSM does not normally own this public milestone;
        the Sharing transport emits it and the customer mirrors it through
        ``mark_service_started()``.

        Returns:
            bool: True when the start event was newly emitted.
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

        Completion requires the local context to have been marked started.

        In the current Sharing lifecycle, the customer owns this terminal public
        event after receiving ``CUSTOMER_IN_DEST`` from the reserved transport.

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

        Candidate exhaustion, walking-route failure, transport cancellation, and
        other terminal customer-side errors use this public failure contract.

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
        Register one planned Sharing-customer movement for deferred emission.

        The current customer FSM uses this principally for the walking approach
        from the customer's position to the reserved free-floating vehicle.

        That movement is recorded as ``phase="approach"`` with
        ``movement_mode="walking"``.

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
        Emit the pending Sharing-customer movement as ``movement_completed``.

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
        Discard an incomplete Sharing-customer movement without emitting a metric.

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

    def reset_service_assignment(self):
        """
        Remove a refused pre-start Sharing transport from the service context.

        The logical service itself remains open so another candidate may be tried
        with the same ``service_id``.

        The reset is allowed only before service start and before any terminal
        state.

        Returns:
            bool: True when assignment state was reset.
        """
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
        """
        Validate a booking response against the open Sharing request.

        Service ID, modality, user ID, and transport ID must be present. Service,
        modality, and user identity must match the current customer context.

        Returns:
            bool: True when the response is compatible with the open request.
        """
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
        """
        Verify that the payload transport identifier matches the XMPP sender.

        Returns:
            bool: True when both identify the same bare JID.
        """
        if not isinstance(content, dict) or content.get("transport_id") is None:
            return False
        return self.agent.bare_jid(content.get("transport_id")) == self.agent.bare_jid(sender)

    def _service_message_content(self, content=None, transport_id=None):
        """
        Build a Sharing message payload carrying canonical service identifiers.

        An explicit transport identifier may be inserted for candidate-specific
        booking messages without changing the logical service identifier.

        Returns:
            dict: Payload extended with service correlation fields.

        Raises:
            ValueError: If no open service context exists.
        """
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
        """
        Emit the terminal Sharing failure, clear transient customer state, and
        stop the customer agent.

        Any pending movement is discarded before ``service_failed`` is emitted.
        The service context, pending booking, selected Sharing vehicle, and
        candidate cache are then cleared.

        Args:
            failure_reason (str): Canonical failure reason.
        """
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
        """
        Start pedestrian movement toward the currently reserved Sharing vehicle.

        The target vehicle and its last known position come from the customer's
        active Sharing transport context.

        Returns:
            tuple: Result returned by the customer's ``move_to()`` operation.

        Raises:
            RuntimeError: If no current Sharing transport or position is known.
        """
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
        """
        Request available free-floating Sharing vehicles from one FleetManager.

        The request carries the open service identifiers, customer ID, current
        origin, and optionally ``max_walking_distance``.

        Args:
            fleetmanager_id: FleetManager JID receiving the candidate query.

        Raises:
            ValueError: If no Sharing service context is open.
        """
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
        """
        Select the nearest candidate remaining after customer-side filtering.

        Candidates are ordered by their precomputed ``distance`` field.

        Returns:
            dict | None: Selected Sharing transport candidate.
        """
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
        """
        Request booking of one selected free-floating Sharing vehicle.

        The current protocol represents the customer booking request as
        REQUEST_PROTOCOL / PROPOSE_PERFORMATIVE.

        The payload carries the shared service identifiers together with customer
        origin and final destination.

        Args:
            transport (dict): Selected candidate containing at least its JID.
        """
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
        """
        Cancel the current Sharing booking attempt.

        REQUEST_PROTOCOL / CANCEL_PERFORMATIVE propagates the same service
        identifiers and any recorded failure reason to the selected vehicle.

        Args:
            transport_id: Sharing transport JID.
        """
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
        """
        Inform the reserved Sharing vehicle that the customer reached it.

        The message uses REQUEST_PROTOCOL / INFORM_PERFORMATIVE and carries
        ``status=CUSTOMER_IN_TRANSPORT``.

        This notification ends the pedestrian approach from the customer's
        perspective and allows the Sharing transport to emit the public
        ``service_started`` milestone.

        The method itself does not emit ``service_started``.
        """
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
        """
        Execute the concrete Sharing customer FSM state.

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
#                       Customer Strategy                      #
#                                                              #
################################################################
class SharingCustomerWaitingState(SharingCustomerStrategyBehaviour):
    """
    Discover and select a usable free-floating Sharing transport.

    FleetManager discovery is treated as operational bootstrap. No canonical
    user service is created while no Sharing FleetManager is available.

    Once at least one FleetManager can receive a candidate query, the customer
    creates or reuses its canonical service context and emits
    ``service_requested`` exactly once.

    Candidate responses from multiple FleetManagers are correlated through the
    same ``service_id`` and merged by transport JID. Candidates are then
    filtered by pedestrian reachability and the nearest remaining vehicle is
    selected for booking.

    Candidate exhaustion after the service has started is terminal and emits
    ``service_failed`` through the common failure path.

    A selected vehicle is stored as the pending transport and execution
    advances to ``CUSTOMER_WAITING_FOR_APPROVAL``.
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
        """
        Discover FleetManagers, gather Sharing candidates, and select one vehicle.

        Absence of FleetManagers does not create a canonical service. The customer
        retries discovery after an operational delay.

        Once candidate querying can begin, ``service_requested`` is emitted through
        the persistent service context. Responses are accepted only when their
        protocol, performative, request type, and service identifiers match the
        open Sharing request.

        Candidate vehicles are deduplicated by JID, filtered by
        ``can_walk()``, and the nearest remaining candidate is selected.

        When no usable candidate remains, the service fails terminally and the
        customer agent stops.

        A selected candidate receives the booking request and the FSM advances to
        ``CUSTOMER_WAITING_FOR_APPROVAL``.
        """
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
    """
    Resolve the selected Sharing vehicle's booking response.

    Only responses from the currently pending transport are considered.
    Protocol, performative, canonical service identifiers, payload transport
    ID, and XMPP sender must all match the open request.

    A matching REFUSE_PERFORMATIVE removes only that candidate. When other
    candidates remain, the customer returns to ``CUSTOMER_WAITING`` and retries
    another vehicle while preserving the same logical ``service_id``.

    A matching ACCEPT_PERFORMATIVE emits the unique public
    ``service_assigned`` milestone, stores the accepted Sharing vehicle, and
    begins pedestrian movement toward its reported position.

    Successful walking setup advances to
    ``CUSTOMER_MOVING_TO_TRANSPORT``.

    If the customer is already at the vehicle, an explicit zero-distance
    walking approach is emitted, the transport is immediately informed of
    customer arrival, and execution advances directly to
    ``CUSTOMER_IN_TRANSPORT``.

    Walking-route and unexpected setup failures terminate the Sharing service.
    """

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_WAITING_FOR_APPROVAL
        logger.debug(
            "Agent[{}]: Waiting for sharing transport booking approval.".format(
                self.agent.name
            )
        )

    async def run(self):
        """
        Process acceptance or refusal of the current Sharing booking attempt.

        Timeouts, unrelated protocols, responses from another transport, malformed
        JSON, unsupported performatives, and stale service identifiers leave the
        current booking unresolved.

        A valid refusal removes the pending vehicle. If other candidates remain,
        the FSM returns to ``CUSTOMER_WAITING`` without clearing the service
        context, allowing the next candidate to be tried with the same
        ``service_id``.

        A valid acceptance emits ``service_assigned`` exactly once and stores the
        selected transport. Pedestrian movement to that vehicle is registered as
        canonical ``phase="approach"`` with ``movement_mode="walking"``.

        AlreadyInDestination emits an explicit zero-distance approach and enters
        ``CUSTOMER_IN_TRANSPORT`` directly after notifying the vehicle.

        Route or unexpected approach errors emit ``service_failed``, cancel the
        accepted booking, clear transient Sharing state, and stop the customer.
        """
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
    """
    Monitor the customer's pedestrian approach to the reserved Sharing vehicle.

    Physical walking is performed by the customer's movement infrastructure.
    This state remains active until the reserved vehicle position is reached.

    Confirmed arrival emits the pending ``phase="approach"``
    ``movement_completed`` event, including
    ``movement_mode="walking"``, and informs the reserved transport with
    ``CUSTOMER_IN_TRANSPORT``.

    Execution then advances to ``CUSTOMER_IN_TRANSPORT``.

    Missing reserved-transport state or inconsistent canonical movement is
    treated as terminal service failure.
    """

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_MOVING_TO_TRANSPORT
        logger.debug(
            "Agent[{}]: The sharing customer is moving to the reserved transport.".format(
                self.agent.name
            )
        )

    async def run(self):
        """
        Monitor pedestrian movement until the customer reaches the booked vehicle.

        Missing Sharing transport assignment terminates the service.

        While walking remains incomplete, execution stays in
        ``CUSTOMER_MOVING_TO_TRANSPORT`` after a one-second asynchronous wait.

        Physical arrival requires an existing pending approach movement.
        Successful completion emits ``movement_completed``, informs the vehicle of
        customer arrival, and enters ``CUSTOMER_IN_TRANSPORT``.

        Arrival without canonical pending movement fails the service with
        ``approach_movement_missing``, cancels the booking, clears Sharing state,
        and stops the customer.
        """
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
    """
    Track the customer's active use of the reserved Sharing vehicle.

    The state accepts service messages only from the assigned transport and
    only when their canonical identifiers match the active Sharing service.

    ``CUSTOMER_IN_TRANSPORT`` mirrors the public ``service_started`` milestone
    already owned by the transport without emitting a duplicate event.

    ``CUSTOMER_IN_DEST`` closes the customer-owned service lifecycle. The
    customer first ensures that local start state is established and then
    emits the public ``service_completed`` event.

    Explicit start mirroring before completion also supports zero-distance
    vehicle services where the transport may notify destination directly
    without a preceding ``CUSTOMER_IN_TRANSPORT`` message.

    Transport cancellation emits the public ``service_failed`` terminal and
    stops the customer.
    """

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_TRANSPORT
        logger.debug(
            "Agent[{}]: The sharing customer is in the transport.".format(
                self.agent.name
            )
        )

    async def run(self):
        """
        Process active service messages from the reserved Sharing transport.

        Timeouts, unrelated protocols, wrong senders, malformed payloads, stale
        service identifiers, and unsupported performatives keep the customer in
        ``CUSTOMER_IN_TRANSPORT``.

        A matching CANCEL_PERFORMATIVE emits ``service_failed`` using the
        transport-provided failure reason when available and terminates the
        customer.

        INFORM with ``CUSTOMER_IN_TRANSPORT`` mirrors ``service_started`` locally.

        INFORM with ``CUSTOMER_IN_DEST`` first guarantees local start state,
        allowing zero-distance services to preserve lifecycle ordering, and then
        emits customer-owned ``service_completed``.

        Successful terminal processing clears the Sharing transport and service
        context and advances to ``CUSTOMER_IN_DEST``.
        """
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
    """
    Terminal successful state of the free-floating Sharing customer FSM.

    The canonical ``service_completed`` event has already been emitted before
    this state is entered.

    Entering the state defensively clears pending booking, active Sharing
    transport, and candidate-selection state.

    The state has no outgoing FSM transitions. Returning from ``run()`` allows
    the complete Sharing strategy to terminate normally.
    """

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
        """
        Log successful Sharing destination completion and terminate this FSM path.
        """
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
    Finite-state customer strategy for free-floating Sharing mobility.

    The FSM coordinates five phases:

    ``CUSTOMER_WAITING``
        Discover FleetManagers and usable Sharing vehicles.

    ``CUSTOMER_WAITING_FOR_APPROVAL``
        Resolve booking of the selected candidate. Refusal may retry another
        candidate with the same ``service_id``.

    ``CUSTOMER_MOVING_TO_TRANSPORT``
        Monitor the pedestrian approach to the reserved vehicle and emit its
        canonical walking movement.

    ``CUSTOMER_IN_TRANSPORT``
        Mirror transport-owned service start and process service completion or
        failure.

    ``CUSTOMER_IN_DEST``
        Terminal successful Sharing state.

    The customer owns ``service_requested``, ``service_assigned``,
    ``service_completed``, ``service_failed``, and walking-approach movement.
    The Sharing transport owns public ``service_started`` and vehicle-service
    movement.

    Normal FSM termination invokes ``notify_modal_completion()`` so a
    MultiModalCustomerAgent may continue to its next itinerary leg. For
    ordinary legacy customers that hook remains a no-op.

    Generic FSM lifecycle instrumentation is inherited from
    FSMSimfleetBehaviour.
    """

    async def on_end(self):
        """
        Finalize the Sharing customer FSM and notify modal orchestration.

        ``FSMSimfleetBehaviour.on_end()`` emits the generic strategy lifecycle end
        event. The customer hook is then invoked so multimodal orchestration may
        observe successful modal completion.

        Legacy non-multimodal customers retain the default no-op hook.
        """
        await super().on_end()
        self.agent.notify_modal_completion()

    def setup(self):
        """
        Register Sharing customer states and permitted transitions.
        """
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
