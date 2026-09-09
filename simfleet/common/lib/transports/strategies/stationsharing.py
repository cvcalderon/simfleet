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
    """
    Base SPADE State shared by the StationSharing transport FSM.

    The transport participates in a customer-owned station-based mobility
    service identified by ``service_id``.

    Its responsibilities include:

    - validating the direct trip request sent by the assigned customer;
    - maintaining a transport-side canonical service context;
    - mirroring customer-owned assignment;
    - emitting the public ``service_started`` milestone;
    - emitting the station-to-station vehicle ``movement_completed`` event;
    - requesting registration in the destination station;
    - handling transport-side completion or failure after station operations;
    - propagating canonical service identifiers to the customer and stations.

    Public ``service_assigned`` and final door-to-door ``service_completed`` /
    ``service_failed`` ownership belongs to the customer strategy.

    A successful vehicle registration at the destination station terminates
    the transport's participation but does not necessarily terminate the
    customer's complete service, because a final pedestrian leg may still
    remain.

    This class is an FSM State helper. State registration and transitions
    belong to FSMStationSharingStrategyBehaviour.
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
        Return the active transport-side StationSharing service context.

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
        Create and store one canonical StationSharing transport service context.

        The transport normally receives the ``service_id`` already created by the
        customer after the origin station assigns this concrete vehicle.

        Transport-side contexts therefore normally use
        ``emit_requested=False`` and require the customer ``user_id``.

        The context correlates customer, vehicle, station-to-station origin and
        destination, lifecycle state, and any pending vehicle movement.

        An unfinished or uncleared context is never silently replaced.

        Args:
            service_id: Customer-created logical service identifier.
            user_id: StationSharing customer JID.
            transport_id: StationSharing vehicle JID.
            origin: Origin-station position.
            destination: Destination-station position.
            emit_requested (bool): Whether this agent owns the request milestone;
                normally False for StationSharing transports.

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

        An explicitly supplied ``service_id`` must match any already active
        context rather than replacing it.

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
        Clear a terminal StationSharing transport context when no movement remains.

        Unfinished contexts and services still owning pending movement are
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
        Build canonical identifiers shared by StationSharing transport events.

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
            transport_id: Optional vehicle identifier to bind.

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
        Validate that a message belongs to the active StationSharing service.

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
        Mirror customer-owned StationSharing assignment without public emission.

        The origin station has already supplied this concrete vehicle to the
        customer before the customer sends the direct trip request.

        The public ``service_assigned`` event is therefore owned by the customer
        rather than emitted again by the vehicle.

        Args:
            transport_id: Assigned StationSharing transport JID.

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
        Mirror StationSharing service-start state without emitting a public event.

        This helper is retained as part of the generic lifecycle contract. The
        current StationSharing transport FSM normally owns service start directly
        through ``start_service()``.

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
        Mark the transport-side StationSharing participation as successfully
        completed without emitting ``service_completed``.

        The current transport FSM uses this only after the destination station has
        accepted registration of the vehicle.

        This local terminal state does not mean that the customer's complete
        door-to-door service has finished: the customer may still need to walk
        from the destination station to its final destination.

        Public ``service_completed`` remains owned by the customer strategy.

        Returns:
            bool: True when the local transport context is successfully terminal.
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
        Mark the transport-side StationSharing participation as failed without
        emitting the public ``service_failed`` event.

        The transport uses this before notifying the customer of route or station
        failures. The customer subsequently owns terminal public failure
        processing.

        Args:
            failure_reason (str | None): Optional transport-side failure reason.

        Returns:
            bool: True when the local context is terminally failed.
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
        Mark a StationSharing service assigned and emit ``service_assigned``.

        This generic helper remains available to custom strategies. The current
        StationSharing transport FSM does not normally own public assignment and
        instead uses ``mark_service_assigned()``.

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
        Mark StationSharing vehicle service as started and emit
        ``service_started``.

        The current transport FSM owns this public milestone. It is emitted when
        the already-assigned vehicle accepts the customer's direct
        station-to-station trip request.

        Service start occurs before physical route execution. A later route failure
        therefore produces a valid lifecycle containing ``service_started``
        followed by terminal failure.

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
        Mark a StationSharing service completed and emit ``service_completed``.

        The helper remains available for custom strategies. The current transport
        FSM does not normally emit public completion because final door-to-door
        completion belongs to the customer after its post-station walking leg.

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
        Mark a StationSharing service failed and emit ``service_failed``.

        This generic helper remains available to custom strategies. The current
        transport FSM normally records local failure through
        ``mark_service_failed()`` and notifies the customer, which owns the public
        terminal event.

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
        Register one planned StationSharing vehicle movement for deferred emission.

        The active transport FSM uses ``phase="service"`` for physical movement
        from the origin station to the destination station.

        The pedestrian approach and final walking segments are emitted separately
        by the StationSharing customer.

        ``movement_completed`` is emitted only after physical vehicle arrival is
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
        Emit the pending StationSharing vehicle movement as
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
        Discard an incomplete StationSharing vehicle movement without emitting a
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

    def _request_identity_valid(self, content, sender=None):
        """
        Validate a direct StationSharing trip request for this exact vehicle.

        The request must contain ``service_id``, ``modality``, ``user_id``, and
        ``transport_id``.

        Modality must be ``station_sharing`` and the requested transport must be
        this vehicle.

        When an XMPP sender is available, it must match ``user_id``. Optional
        ``customer_id`` must identify that same customer.

        Args:
            content: Decoded trip request.
            sender: Optional XMPP sender JID.

        Returns:
            bool: True when the request belongs to this vehicle and carries a
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
        Verify that the payload customer identity matches the XMPP sender.

        Returns:
            bool: True when both represent the same bare JID.
        """
        if not isinstance(content, dict) or content.get("user_id") is None:
            return False
        return self.agent.bare_jid(content.get("user_id")) == self.agent.bare_jid(sender)

    def _service_message_content(self, content=None):
        """
        Build an outgoing StationSharing message with active service identifiers.

        Args:
            content (dict | None): Additional message fields.

        Returns:
            dict: Payload extended with canonical service identifiers.

        Raises:
            ValueError: If no active StationSharing service context exists.
        """
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
        """
        Send a correlated StationSharing service message to the active customer.

        Messages use REQUEST_PROTOCOL and the supplied performative. Canonical
        service identifiers from the active transport context are added to the
        outgoing payload.

        The method is used for both successful status notifications and failure
        notifications.

        Args:
            customer_id: StationSharing customer JID.
            performative: SPADE/FIPA performative to send.
            content (dict | None): Service-status or failure payload.
        """
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
        """
        Request registration of this StationSharing vehicle in a station.

        The request uses REGISTER_PROTOCOL / REQUEST_PERFORMATIVE and carries the
        vehicle name, JID, and fleet type.

        When a canonical service context is active, its service identifiers are
        also propagated so station interaction remains correlated with the current
        vehicle trip.

        The same helper is used for normal registration in the destination station
        and for recovery registration in the origin station after a route failure.

        Station registration is an operational infrastructure handshake and does
        not itself emit a canonical service event.

        Args:
            station_id: Target sharing-station JID.
        """
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
        """
        Execute the concrete StationSharing transport FSM state.

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
#                  Station Transport Strategy                  #
#                                                              #
################################################################
class StationSharingWaitingState(
    StationSharingStrategyBehaviour
):
    """
    Wait for a direct station-to-station trip request from the customer
    assigned to this StationSharing vehicle.

    The incoming REQUEST_PROTOCOL / PROPOSE_PERFORMATIVE must identify this
    exact vehicle and carry a valid customer-owned StationSharing service
    context.

    A valid request creates the transport-side service mirror, mirrors the
    already public customer assignment, emits ``service_started``, stores the
    customer as onboard, and temporarily removes the vehicle's station
    registration before route execution.

    Successful route setup registers pending ``phase="service"`` movement and
    advances to ``TRANSPORT_MOVING_TO_DESTINATION``.

    If the vehicle is already at the destination station, an explicit
    zero-distance service movement is completed and station registration is
    requested immediately.

    A route-resolution failure marks the transport-side service failed and
    notifies the customer. When the origin station is known, the vehicle then
    attempts recovery by registering back in that station.

    Unexpected service errors terminate the transport through
    ``TRANSPORT_IN_DEST`` after canonical failure cleanup.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING
        logger.debug("{} in Station Sharing Waiting State".format(self.agent.jid))

    async def run(self):
        """
        Validate and start one assigned StationSharing vehicle trip.

        Only a valid customer PROPOSE for this vehicle can start service.

        The state:

        1. establishes the transport-side service context;
        2. mirrors customer-owned assignment;
        3. emits ``service_started``;
        4. stores the customer as onboard;
        5. remembers origin and destination station identities;
        6. marks the vehicle as no longer registered in a station;
        7. requests physical movement to the destination station;
        8. registers that route as pending ``phase="service"`` movement.

        Successful setup enters ``TRANSPORT_MOVING_TO_DESTINATION``.

        AlreadyInDestination completes a zero-distance service movement and
        requests destination-station registration directly.

        PathRequestException marks the service failed and informs the customer.
        When an origin station is available, the vehicle attempts to restore its
        station registration there before becoming usable again.

        Other unexpected failures clear active service state and terminate this
        transport through ``TRANSPORT_IN_DEST``.
        """
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
    """
    Monitor the active station-to-station vehicle movement.

    MovableMixin performs physical movement while this state waits for arrival
    at the configured destination station.

    Confirmed arrival requires the pending ``phase="service"`` movement to be
    completed successfully before station registration is requested.

    The transport does not become available merely because it has physically
    reached the destination coordinates. It must first register successfully
    in the destination station.

    Missing active-customer state, missing service context, missing movement
    correlation, or missing destination-station identity follow the existing
    failure recovery paths and may terminate the transport through
    ``TRANSPORT_IN_DEST``.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
        logger.debug("{} moving to destination station".format(self.agent.jid))

    async def run(self):
        """
        Monitor physical StationSharing movement until station arrival.

        Missing active-customer or canonical service state triggers defensive
        recovery through ``TRANSPORT_IN_DEST``.

        While movement remains incomplete, the FSM stays in
        ``TRANSPORT_MOVING_TO_DESTINATION``.

        Physical arrival must successfully emit the pending canonical
        ``movement_completed`` event.

        Missing movement correlation fails the service with
        ``service_movement_missing``.

        A valid destination-station identifier is also required. When present, the
        vehicle requests station registration and advances to
        ``TRANSPORT_WAITING_FOR_STATION_APPROVAL``.

        Missing destination-station state is treated as terminal
        ``destination_station_missing`` failure.
        """
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
    """
    Wait for the target station to accept or refuse vehicle registration.

    The target station may represent either:

    - the normal destination station after successful physical service
      movement; or
    - the original station during recovery from a route failure that occurred
      before the vehicle could complete its service movement.

    Normal destination ACCEPT registers the vehicle, informs the customer that
    the vehicle reached the destination station, marks transport-side
    participation completed, clears local service state, and returns the
    transport to ``TRANSPORT_WAITING``.

    Recovery ACCEPT restores the failed vehicle to its origin station, clears
    the already-failed service context, and returns it to
    ``TRANSPORT_WAITING`` without emitting a second customer terminal message.

    Registration REFUSE represents station-operation failure. The customer is
    informed and the transport enters terminal ``TRANSPORT_IN_DEST``.

    Timeouts, unrelated protocols, wrong station senders, and unsupported
    performatives keep the transport waiting for station approval.
    """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING_FOR_STATION_APPROVAL
        logger.debug("{} waiting for destination station approval".format(self.agent.jid))

    async def run(self):
        """
        Resolve station registration for normal completion or route-failure
        recovery.

        A service context already marked failed identifies origin-station recovery.

        The active registration target must exist and station responses must use
        REGISTER_PROTOCOL and originate from that exact station.

        On ACCEPT:

        - station registration state is restored;
        - a failed recovery service clears its customer and service state and
          returns directly to waiting;
        - a normal service informs the customer with ``CUSTOMER_IN_DEST``, mirrors
          transport-side completion, increments completed assignments, clears
          station/service state, and returns to waiting.

        On REFUSE, the transport records ``station_operation_failed``, informs the
        customer with ``failure_operation="drop"``, clears active service state,
        and enters terminal ``TRANSPORT_IN_DEST``.
        """
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
    """
    Terminal unsuccessful state of the StationSharing transport FSM.

    The normal successful path never terminates here; successful destination
    registration returns the vehicle to ``TRANSPORT_WAITING``.

    This state is reached when the vehicle cannot safely recover from a
    service, movement, or station-registration failure.

    Entering the state marks the transport as ``TRANSPORT_IN_DEST`` for
    diagnostic compatibility.

    ``run()`` stops the transport agent permanently under the current failure
    policy.
    """
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
        """
        Stop the StationSharing transport after an unrecoverable trip failure.
        """
        await self.agent.stop()


class FSMStationSharingStrategyBehaviour(
    FSMSimfleetBehaviour
):
    """
    Finite-state operational strategy for a StationSharing transport.

    The FSM contains four states:

    ``TRANSPORT_WAITING``
        Receive the assigned customer's direct station-to-station trip request,
        emit service start, and initialize movement.

    ``TRANSPORT_MOVING_TO_DESTINATION``
        Monitor physical vehicle movement and request registration after
        destination-station arrival.

    ``TRANSPORT_WAITING_FOR_STATION_APPROVAL``
        Resolve either normal destination registration or origin-station
        recovery after a failed route.

    ``TRANSPORT_IN_DEST``
        Terminal unsuccessful state that stops an unrecoverable transport.

    Successful vehicle participation does not terminate the FSM. Instead,
    destination-station acceptance returns the vehicle to
    ``TRANSPORT_WAITING`` for future assignments.

    The customer owns public assignment and final door-to-door terminal events.
    The vehicle owns public ``service_started`` and station-to-station
    ``movement_completed``.

    Generic FSM lifecycle instrumentation is inherited from
    FSMSimfleetBehaviour.
    """
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
