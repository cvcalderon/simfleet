import math
from uuid import uuid4
import json

from loguru import logger

from spade.behaviour import State
from spade.message import Message

from simfleet.utils.abstractstrategies import (
    FSMSimfleetBehaviour,
)

from simfleet.utils.helpers import (
    AlreadyInDestination,
    PathRequestException,
    distance_in_meters,
)

from simfleet.utils.status import (
    TRANSPORT_WAITING,
    TRANSPORT_MOVING_TO_DESTINATION,
    TRANSPORT_IN_DEST,
    TRANSPORT_BOARDING,
)

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    INFORM_PERFORMATIVE,
)



class PublicTransportStrategyBehaviour(
    State
):
    """
    Base SPADE State shared by the PublicTransport vehicle FSM.

    A PublicTransport vehicle may carry passengers belonging to several
    independent door-to-door services at the same time. Transport-side
    canonical contexts are therefore stored by ``service_id`` rather than as
    one global active service.

    Each accepted boarding emits one public ``service_assigned`` event for the
    corresponding customer service. Multiple boardings may therefore use the
    same ``service_id`` across different vehicles during a transfer journey.

    Physical vehicle movement is modeled separately from individual passenger
    services. One completed stop-to-stop segment is emitted once for the
    vehicle and records route metadata, onboard passenger count, and capacity
    rather than duplicating the movement for every passenger.

    The class also provides customer boarding, alighting, stop notification,
    service-correlation, and segment-metric helpers.

    Concrete operational states and transitions belong to
    FSMPublicTransportStrategyBehaviour.
    """

    METRICS_MODALITY = "public_transport"
    _METRICS_SERVICE_CONTEXTS_ATTR = "_metrics_service_contexts"
    _METRICS_PENDING_MOVEMENT_ATTR = "_metrics_pending_movement"
    _METRICS_MOVEMENT_PHASES = {"approach", "service", "auxiliary"}

    def _metrics_modality(self):
        """
        Return the canonical PublicTransport metrics modality.

        Returns:
            str: ``"public_transport"``.
        """
        return self.METRICS_MODALITY

    def get_service_contexts(self):
        """
        Return the transport-side PublicTransport contexts indexed by service ID.

        The dictionary may contain several simultaneous customer services because
        one PublicTransport vehicle can carry multiple passengers at once.

        The storage is created lazily on the transport agent.

        Returns:
            dict: Mapping from canonical ``service_id`` to service context.
        """
        contexts = getattr(
            self.agent,
            self._METRICS_SERVICE_CONTEXTS_ATTR,
            None,
        )
        if contexts is None:
            contexts = {}
            setattr(
                self.agent,
                self._METRICS_SERVICE_CONTEXTS_ATTR,
                contexts,
            )
        return contexts

    def get_service_context(self, service_id):
        """
        Return one PublicTransport service context by canonical service ID.

        Args:
            service_id: Logical door-to-door service identifier.

        Returns:
            dict | None: Matching transport-side context.
        """
        if service_id is None:
            return None
        return self.get_service_contexts().get(str(service_id))

    def get_service_context_for_user(self, user_id):
        """
        Resolve the unique active PublicTransport context for one customer.

        Customer identity is normalized to a bare JID.

        A context is returned only when exactly one active service belongs to that
        user on this vehicle. Multiple matches are treated as ambiguous and produce
        a warning rather than selecting one arbitrarily.

        Args:
            user_id: Customer JID.

        Returns:
            dict | None: Unique matching service context.
        """
        user_id = self.agent.bare_jid(user_id)
        if user_id is None:
            return None
        matches = [
            context
            for context in self.get_service_contexts().values()
            if context.get("user_id") == user_id
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            logger.warning(
                "Transport {} has multiple metrics service contexts for user {}.".format(
                    self.agent.name,
                    user_id,
                )
            )
        return None

    def create_service_context(
        self,
        service_id,
        user_id,
        transport_id=None,
    ):
        """
        Create one transport-side PublicTransport boarding context.

        The ``service_id`` originates from the customer's door-to-door journey.
        Several different service contexts may coexist on the same vehicle.

        The transport itself does not emit ``service_requested``. The context is
        created when a boarding request for that customer is being correlated.

        ``assignment_keys`` tracks boardings already emitted for this service and
        prevents duplicate ``service_assigned`` events.

        Args:
            service_id: Customer-owned logical service identifier.
            user_id: Boarding customer JID.
            transport_id: Vehicle JID; defaults to this transport.

        Returns:
            dict | None: Created or already existing context.
        """
        if service_id is None or user_id is None:
            return None
        service_id = str(service_id)
        contexts = self.get_service_contexts()
        if service_id in contexts:
            return contexts[service_id]
        context = {
            "service_id": service_id,
            "modality": self.METRICS_MODALITY,
            "user_id": self.agent.bare_jid(user_id),
            "transport_id": self.agent.bare_jid(
                transport_id if transport_id is not None else self.agent.jid
            ),
            "requested": True,
            "assignment_keys": set(),
            "pending_movement": None,
        }
        contexts[service_id] = context
        return context

    def get_or_create_service_context(self, service_id, user_id, transport_id=None):
        """
        Return the matching PublicTransport context or create one.

        Reuse is allowed only when the supplied customer identity matches the
        customer already associated with that ``service_id``.

        Args:
            service_id: Canonical service identifier.
            user_id: Expected customer JID.
            transport_id: Optional vehicle JID.

        Returns:
            dict | None: Matching or newly created service context.
        """
        context = self.get_service_context(service_id)
        if context is not None:
            if self.agent.bare_jid(user_id) != context.get("user_id"):
                return None
            return context
        return self.create_service_context(
            service_id=service_id,
            user_id=user_id,
            transport_id=transport_id,
        )

    def clear_service_context(self, service_id):
        """
        Remove one PublicTransport service context from this vehicle.

        Clearing one passenger service does not affect other passengers currently
        carried by the same transport.

        This is typically used after that customer alights or after a rejected
        boarding whose provisional context is no longer needed.

        Args:
            service_id: Service context to remove.

        Returns:
            bool: True after the context is absent.
        """
        contexts = self.get_service_contexts()
        if str(service_id) not in contexts:
            return True
        del contexts[str(service_id)]
        return True

    def _service_event_details(self, context):
        """
        Build canonical identifiers for one passenger service on this vehicle.

        Args:
            context (dict): PublicTransport service context.

        Returns:
            dict: Modality, service, user, and vehicle identifiers.
        """
        return {
            "modality": context["modality"],
            "service_id": context["service_id"],
            "user_id": context.get("user_id"),
            "transport_id": context.get("transport_id"),
        }

    def add_service_identifiers(self, content, service_id, user_id=None):
        """
        Add one passenger service's canonical identifiers to an outgoing payload.

        When ``user_id`` is supplied, it must match the customer already bound to
        the requested service context.

        Args:
            content (dict | None): Existing message payload.
            service_id: Canonical passenger service identifier.
            user_id: Optional expected customer JID.

        Returns:
            dict: Copied payload extended with service identifiers.

        Raises:
            ValueError: If the service is unknown or the supplied customer does not
                match its context.
        """
        context = self.get_service_context(service_id)
        if context is None:
            raise ValueError("Unknown public-transport service_id.")
        if user_id is not None and self.agent.bare_jid(user_id) != context.get("user_id"):
            raise ValueError("Public-transport user_id does not match service context.")
        result = dict(content or {})
        result.update(self._service_event_details(context))
        return result

    def message_matches_service(self, content, service_id=None):
        """
        Validate a message against one PublicTransport passenger service.

        A valid message must carry ``service_id``, ``modality``, ``user_id``, and
        ``transport_id``.

        Service, customer, modality, and transport must all match the selected
        local context.

        Args:
            content: Decoded message payload.
            service_id: Optional expected service identifier.

        Returns:
            bool: True when the message belongs to the expected boarding context.
        """
        if not isinstance(content, dict):
            return False
        for key in ("service_id", "modality", "user_id"):
            if content.get(key) is None:
                return False
        context = self.get_service_context(
            service_id if service_id is not None else content["service_id"]
        )
        if context is None:
            return False
        if str(content["service_id"]) != context["service_id"]:
            return False
        if content["modality"] != context["modality"]:
            return False
        if self.agent.bare_jid(content["user_id"]) != context.get("user_id"):
            return False
        if content.get("transport_id") is None:
            return False
        if (
            self.agent.bare_jid(content["transport_id"])
            != context.get("transport_id")
        ):
            return False
        return True

    def assign_service(
        self,
        service_id,
        boarding_key=None,
        extra_details=None,
    ):
        """
        Emit one PublicTransport ``service_assigned`` boarding milestone.

        Assignment belongs publicly to the vehicle that accepts the boarding.

        ``boarding_key`` makes the operation idempotent for one boarding while
        still allowing the same customer ``service_id`` to receive additional
        assignment milestones during later transfers.

        Args:
            service_id: Customer's persistent door-to-door service identifier.
            boarding_key: Boarding-specific correlation key.
            extra_details (dict | None): Pattern, route, mode, stop, and leg
                metadata.

        Returns:
            bool: True when a new ``service_assigned`` event was emitted.
        """
        context = self.get_service_context(service_id)
        if context is None:
            return False
        key = str(
            boarding_key
            if boarding_key is not None
            else context.get("transport_id")
        )
        if key in context["assignment_keys"]:
            return False
        context["assignment_keys"].add(key)
        details = self._service_event_details(context)
        details.update(extra_details or {})
        self.agent.events_store.emit(
            event_type="service_assigned",
            details=details,
        )
        return True

    def set_pending_movement(self, phase, distance_m, extra_details=None):
        """
        Register one physical PublicTransport vehicle movement for deferred metric
        emission.

        Unlike passenger service contexts, movement belongs to the vehicle itself
        rather than to one ``service_id``.

        A stop-to-stop segment is therefore emitted once even when several
        passengers are onboard.

        Args:
            phase: Canonical movement phase.
            distance_m: Planned segment distance in metres.
            extra_details (dict | None): Pattern, route, stop, occupancy, and
                capacity metadata.

        Returns:
            bool: True when the vehicle movement was registered.

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
        if getattr(self.agent, self._METRICS_PENDING_MOVEMENT_ATTR, None) is not None:
            return False
        details = {
            "modality": self.METRICS_MODALITY,
            "transport_id": self.agent.bare_jid(self.agent.jid),
            "phase": phase,
            "distance_m": float(distance_m),
        }
        details.update(extra_details or {})
        setattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            {"details": details},
        )
        return True

    def complete_pending_movement(self):
        """
        Emit the pending physical PublicTransport segment as
        ``movement_completed``.

        The event is vehicle-level and may represent movement shared by several
        passenger services.

        Returns:
            bool: True when one pending segment existed and was emitted.
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
        setattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            None,
        )
        return True

    def discard_pending_movement(self):
        """
        Discard an incomplete PublicTransport vehicle segment without emitting a
        movement metric.

        Returns:
            bool: True when one pending movement existed.
        """
        if getattr(self.agent, self._METRICS_PENDING_MOVEMENT_ATTR, None) is None:
            return False
        setattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            None,
        )
        return True

    async def on_start(
        self
    ):

        logger.debug(
            "Strategy {} started in public transport {}".format(
                type(self).__name__,
                self.agent.name
            )
        )

    def get_customers_from_current_stop(
        self
    ):
        """
        Return onboard customers whose requested alighting stop is the current
        stop.

        Returns:
            list: Customer identifiers scheduled to leave the vehicle here.
        """
        result = []

        current_customers = (
            self.agent.get(
                "current_customer"
            )
        )

        for customer_id, data in (
            current_customers.items()
        ):

            if data.get(
                "dest"
            ) == self.agent.current_stop:

                result.append(
                    customer_id
                )

        return result

    async def drop_customers(
        self
    ):
        """
        Alight every onboard customer whose destination is the current stop.

        Each customer is correlated with its unique local service context before an
        INFORM_PERFORMATIVE ``public_transport_arrival`` message is sent.

        Successful alighting removes the customer from onboard state, restores one
        unit of vehicle capacity, and clears only that passenger's service context.

        No public ``service_completed`` event is emitted by the vehicle. The
        customer remains responsible for transfers, final walking, and the terminal
        outcome of the complete door-to-door journey.
        """
        customers = (
            self.get_customers_from_current_stop()
        )

        for customer_id in customers:
            logger.info(
                "Transport {} dropping customer {} at stop {}".format(
                    self.agent.name,
                    customer_id,
                    self.agent.current_stop
                )
            )

            context = self.get_service_context_for_user(
                customer_id
            )

            if context is None:
                logger.error(
                    "Transport {} cannot correlate alighting customer {} "
                    "with a public-transport service context.".format(
                        self.agent.name,
                        customer_id,
                    )
                )
                continue

            content = self.add_service_identifiers(
                {
                    "request_type":
                        "public_transport_arrival",

                    "vehicle_id":
                        str(
                            self.agent.jid
                        ),

                    "pattern_id":
                        self.agent.pattern_id,

                    "stop":
                        self.agent.current_stop,
                },
                context["service_id"],
                user_id=customer_id,
            )

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
                INFORM_PERFORMATIVE
            )

            msg.body = json.dumps(
                content
            )

            await self.send(
                msg
            )

            self.agent.remove_customer_in_transport(
                customer_id
            )

            self.agent.current_capacity += 1

            self.clear_service_context(
                context["service_id"]
            )

    async def inform_stop_arrival(
        self
    ):
        """
        Notify the current PublicTransport stop that this vehicle has arrived.

        The operational notification contains vehicle ID, Pattern, stop, and
        current free capacity.

        It is stop-infrastructure communication and does not belong to one
        passenger ``service_id``.

        Missing stop JID is logged and the notification is skipped.
        """
        stop_jid = (
            self.agent.get_stop_jid(
                self.agent.current_stop
            )
        )

        if stop_jid is None:

            logger.error(
                "Transport {} cannot find JID for stop {}".format(
                    self.agent.name,
                    self.agent.current_stop
                )
            )

            return

        content = {
            "request_type":
                "public_transport_vehicle_arrival",

            "vehicle_id":
                str(
                    self.agent.jid
                ),

            "pattern_id":
                self.agent.pattern_id,

            "stop":
                self.agent.current_stop,

            "free_capacity":
                self.agent.current_capacity,
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
            INFORM_PERFORMATIVE
        )

        msg.body = json.dumps(
            content
        )

        await self.send(
            msg
        )

    async def inform_stop_customer_boarded(
        self,
        customer_id
    ):
        """
        Inform the current stop that one waiting customer boarded this vehicle.

        The notification lets stop infrastructure update its Pattern queue using
        customer identity and ``pattern_id``.

        It is operational stop communication rather than a canonical passenger
        service event.

        Args:
            customer_id: Customer that successfully boarded.
        """
        stop_jid = (
            self.agent.get_stop_jid(
                self.agent.current_stop
            )
        )

        if stop_jid is None:
            return

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
            INFORM_PERFORMATIVE
        )

        msg.body = json.dumps(
            {
                "request_type":
                    "public_transport_customer_boarded",

                "customer_id":
                    str(
                        customer_id
                    ),

                "pattern_id":
                    self.agent.pattern_id,
            }
        )

        await self.send(
            msg
        )

    async def accept_customer(
        self,
        customer_id,
        content
    ):
        """
        Complete one valid PublicTransport boarding acceptance.

        A correlated service context must already exist.

        The customer is added to onboard state with the current stop as origin and
        its requested alighting stop as destination. Free capacity is decreased by
        one.

        One boarding-specific public ``service_assigned`` event is then emitted.

        REQUEST_PROTOCOL / ACCEPT_PERFORMATIVE returns the canonical service
        identifiers to the customer, the stop is informed that this customer has
        boarded, and updated vehicle Presence is published.

        Args:
            customer_id: Boarding customer JID.
            content (dict): Validated boarding-request payload.

        Returns:
            bool: True when boarding acceptance completed successfully.
        """
        destination_stop = (
            content.get(
                "destination_stop"
            )
        )

        service_id = content.get(
            "service_id"
        )

        context = self.get_service_context(
            service_id
        )

        if context is None:
            logger.error(
                "Transport {} cannot accept customer {} without a "
                "correlated service context.".format(
                    self.agent.name,
                    customer_id,
                )
            )
            return False

        logger.info(
            "Transport {} accepted customer {} from {} to {}".format(
                self.agent.name,
                customer_id,
                self.agent.current_stop,
                destination_stop
            )
        )

        self.agent.add_customer_in_transport(
            customer_id,
            origin=self.agent.current_stop,
            dest=destination_stop,
        )

        self.agent.current_capacity -= 1

        boarding_key = content.get(
            "boarding_key"
        )

        if boarding_key is None:
            boarding_key = "{}:{}:{}:{}".format(
                content.get("leg_index"),
                self.agent.pattern_id,
                self.agent.current_stop,
                destination_stop,
            )

        self.assign_service(
            service_id,
            boarding_key=boarding_key,
            extra_details={
                "pattern_id": self.agent.pattern_id,
                "route_id": self.agent.route_id,
                "mode": self.agent.mode,
                "origin_stop": self.agent.current_stop,
                "destination_stop": destination_stop,
                "leg_index": content.get("leg_index"),
            },
        )

        reply = Message()

        reply.to = str(
            customer_id
        )

        reply.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        reply.set_metadata(
            "performative",
            ACCEPT_PERFORMATIVE
        )

        reply.body = json.dumps(
            self.add_service_identifiers(
                content,
                service_id,
                user_id=customer_id,
            )
        )

        await self.send(
            reply
        )

        await self.inform_stop_customer_boarded(
            customer_id
        )

        self.agent.publish_public_transport_presence()

        return True

    async def reject_customer(
        self,
        customer_id,
        content
    ):
        """
        Refuse one PublicTransport boarding request.

        The original request payload is preserved. When a matching provisional
        service context exists, canonical service identifiers are added before the
        REFUSE_PERFORMATIVE is returned.

        A provisional context is cleared when the customer is not already onboard,
        allowing the same journey service to wait for another compatible vehicle.

        Args:
            customer_id: Customer JID.
            content (dict | None): Original boarding-request payload.

        Returns:
            bool: True after the refusal message is sent.
        """
        logger.info(
            "Transport {} rejected customer {}".format(
                self.agent.name,
                customer_id
            )
        )

        reply_content = dict(
            content or {}
        )

        service_id = reply_content.get(
            "service_id"
        )

        context = self.get_service_context(
            service_id
        )

        if context is not None:
            reply_content = self.add_service_identifiers(
                reply_content,
                service_id,
                user_id=customer_id,
            )

        reply = Message()

        reply.to = str(
            customer_id
        )

        reply.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        reply.set_metadata(
            "performative",
            REFUSE_PERFORMATIVE
        )

        reply.body = json.dumps(
            reply_content
        )

        await self.send(
            reply
        )

        if (
            context is not None
            and str(customer_id)
            not in self.agent.get("current_customer")
        ):
            self.clear_service_context(
                service_id
            )

        return True

    def get_segment_distance(
        self,
        origin_stop,
        destination_stop,
        route_distance=None,
    ):
        """
        Resolve the completed distance for one PublicTransport stop-to-stop segment.

        An explicit routed distance has priority.

        Otherwise the helper falls back to geographic distance between configured
        stop positions.

        Args:
            origin_stop: Segment origin stop identifier.
            destination_stop: Segment destination stop identifier.
            route_distance: Optional routed distance in metres.

        Returns:
            float | None: Segment distance in metres, or None when stop identifiers
            are missing.
        """
        if (
            origin_stop is None
            or destination_stop is None
        ):
            return None

        if route_distance is not None:
            return float(
                route_distance
            )

        origin_position = (
            self.agent.get_stop_position(
                origin_stop
            )
        )

        destination_position = (
            self.agent.get_stop_position(
                destination_stop
            )
        )

        if (
            origin_position is None
            or destination_position is None
        ):
            return 0.0

        return float(
            distance_in_meters(
                origin_position,
                destination_position
            )
        )

    def set_pending_segment_movement(
        self,
        origin_stop,
        destination_stop,
        route_distance=None,
    ):
        """
        Register one physical PublicTransport segment with occupancy metadata.

        Segment distance uses the routed distance when available and otherwise the
        stop-position fallback provided by ``get_segment_distance()``.

        Occupancy is captured at segment registration time as
        ``capacity - current_capacity``.

        The pending movement records Pattern, route, mode, movement mode, origin
        and destination stops, onboard passenger count, and total vehicle
        capacity.

        Args:
            origin_stop: Segment origin stop.
            destination_stop: Segment destination stop.
            route_distance: Optional routed segment distance.

        Returns:
            bool: True when the physical segment movement was registered.
        """
        distance = self.get_segment_distance(
            origin_stop,
            destination_stop,
            route_distance=route_distance,
        )

        if distance is None:
            return False

        onboard = (
            self.agent.capacity
            - self.agent.current_capacity
        )

        return self.set_pending_movement(
            "service",
            distance,
            extra_details={
                "pattern_id": self.agent.pattern_id,
                "route_id": self.agent.route_id,
                "mode": self.agent.mode,
                "movement_mode": self.agent.movement_mode,
                "origin_stop": origin_stop,
                "destination_stop": destination_stop,
                "onboard": onboard,
                "capacity": self.agent.capacity,
            },
        )


class PublicTransportSelectStopState(
    PublicTransportStrategyBehaviour
):
    """
    Select the next operational step of a PublicTransport vehicle.

    This state runs after boarding dwell has finished and determines whether
    the vehicle should move to another stop, process a newly resolved Pattern,
    restart an end-to-end Pattern, or wait for missing operational data.

    When the current Pattern has a next stop, that stop becomes
    ``next_stop``, PublicTransport Presence is updated, and execution advances
    to ``TRANSPORT_MOVING_TO_DESTINATION``.

    Pattern changes are directional. ``prepare_next_pattern()`` clears the old
    Pattern structure and asynchronously resolves the configured next Pattern.
    Once the new Pattern is available, ``pattern_changed`` routes the vehicle
    through ``TRANSPORT_IN_DEST`` so the shared terminal stop is processed
    under the new Pattern before departure.

    An end-to-end Pattern without a configured next Pattern is restarted at
    ``start_stop`` after reaching its terminal stop.

    Missing Pattern data, completed Pattern waiting, and unresolved next-stop
    conditions are currently retryable through ``TRANSPORT_WAITING``.
    """
    async def on_start(
        self
    ):

        await super().on_start()

        self.agent.status = (
            TRANSPORT_WAITING
        )

    async def run(
        self
    ):
        """
        Select the next stop or resolve Pattern-boundary behaviour.

        Missing Pattern information is retryable and remains in
        ``TRANSPORT_WAITING``.

        After a newly configured Pattern has been resolved, ``pattern_changed`` is
        cleared and the vehicle enters ``TRANSPORT_IN_DEST`` so the shared stop is
        processed before movement under the new direction.

        A normal next stop is stored in ``next_stop``, published through Presence,
        and dispatched to ``TRANSPORT_MOVING_TO_DESTINATION``.

        When the current Pattern is finished, a configured ``next_pattern_id`` is
        prepared first.

        If no next Pattern exists for an end-to-end route, the current Pattern is
        restarted at its configured start stop and that stop is processed through
        ``TRANSPORT_IN_DEST``.

        Other unresolved next-stop conditions are logged and retried.
        """
        if self.agent.pattern is None:

            await self.agent.sleep(
                1
            )

            self.set_next_state(
                TRANSPORT_WAITING
            )

            return

        if self.agent.pattern_changed:

            self.agent.pattern_changed = False

            self.set_next_state(
                TRANSPORT_IN_DEST
            )

            return

        next_stop = (
            self.agent.get_next_stop()
        )

        if next_stop is not None:

            self.agent.next_stop = (
                next_stop
            )

            self.agent.publish_public_transport_presence()

            self.set_next_state(
                TRANSPORT_MOVING_TO_DESTINATION
            )

            return

        if self.agent.is_pattern_finished():

            next_pattern_id = (
                self.agent.get_next_pattern_id()
            )

            if next_pattern_id is not None:

                changed = (
                    self.agent.prepare_next_pattern()
                )

                if changed:

                    self.set_next_state(
                        TRANSPORT_WAITING
                    )

                    return

            if (
                self.agent.route_type == "end-to-end"
                and self.agent.start_stop is not None
                and self.agent.current_stop != self.agent.start_stop
            ):

                start_position = (
                    self.agent.get_stop_position(
                        self.agent.start_stop
                    )
                )

                self.agent.current_stop = (
                    self.agent.start_stop
                )

                self.agent.next_stop = None
                self.agent.dest = None

                if start_position is not None:
                    await self.agent.set_position(
                        start_position
                    )

                logger.info(
                    "Transport {} restarted pattern {} at stop {}".format(
                        self.agent.name,
                        self.agent.pattern_id,
                        self.agent.current_stop
                    )
                )

                self.agent.publish_public_transport_presence()

                self.set_next_state(
                    TRANSPORT_IN_DEST
                )

                return

            self.agent.next_stop = None

            self.agent.publish_public_transport_presence()

            logger.info(
                "Transport {} finished pattern {} at stop {}".format(
                    self.agent.name,
                    self.agent.pattern_id,
                    self.agent.current_stop
                )
            )

            await self.agent.sleep(
                1
            )

            self.set_next_state(
                TRANSPORT_WAITING
            )

            return

        logger.error(
            "Transport {} could not determine next stop in pattern {}".format(
                self.agent.name,
                self.agent.pattern_id
            )
        )

        await self.agent.sleep(
            1
        )

        self.set_next_state(
            TRANSPORT_WAITING
        )


class PublicTransportMovingState(
    PublicTransportStrategyBehaviour
):
    """
    Execute and monitor one physical PublicTransport stop-to-stop segment.

    ``next_stop`` identifies the operational destination selected by
    PublicTransportSelectStopState.

    Route and teleport movement modes share the same canonical segment metric:
    one vehicle-level ``movement_completed`` event with Pattern, route, stop,
    onboard-passenger, and capacity metadata.

    The movement event is completed before ``current_stop`` advances and before
    passengers alight at the destination stop. This preserves the occupancy of
    the segment that was actually travelled.

    Route-based movement creates the pending segment when route execution
    starts and completes it only after physical arrival.

    Teleport movement returns only after the simulated travel time and physical
    position update, then emits the corresponding segment before stop
    processing.

    Successful arrival advances to ``TRANSPORT_IN_DEST``.

    Route-resolution failures are currently retryable and remain in
    ``TRANSPORT_MOVING_TO_DESTINATION``.
    """
    async def on_start(
        self
    ):

        await super().on_start()

        self.agent.status = (
            TRANSPORT_MOVING_TO_DESTINATION
        )

    async def run(
        self
    ):
        """
        Execute or continue movement toward the selected next stop.

        Missing ``next_stop`` or missing destination-stop coordinates discard any
        pending segment and return to ``TRANSPORT_WAITING`` for redispatch.

        In teleport mode, the movement helper completes simulated travel before
        returning. The segment metric is then registered and completed before the
        vehicle enters the destination-stop state.

        In route mode, an already active destination remains in this state until
        physical arrival. At arrival, the existing pending segment is completed; if
        necessary, the metric is defensively reconstructed from the last routed
        distance.

        A newly requested route registers a pending segment containing occupancy
        captured before destination-stop alighting.

        ``AlreadyInDestination`` emits an explicit zero-distance segment.

        ``PathRequestException`` discards incomplete movement and retries the same
        stop after an operational delay.

        Successful movement updates ``current_stop``, clears ``next_stop``, and
        enters ``TRANSPORT_IN_DEST``.
        """
        next_stop = (
            self.agent.next_stop
        )

        if next_stop is None:

            logger.error(
                "Transport {} has no next stop "
                "while moving".format(
                    self.agent.name
                )
            )

            self.discard_pending_movement()

            self.set_next_state(
                TRANSPORT_WAITING
            )

            return

        origin_stop = (
            self.agent.current_stop
        )

        destination = (
            self.agent.get_stop_position(
                next_stop
            )
        )

        if destination is None:

            logger.error(
                "Transport {} cannot resolve "
                "position for stop {}".format(
                    self.agent.name,
                    next_stop
                )
            )

            self.discard_pending_movement()

            self.set_next_state(
                TRANSPORT_WAITING
            )

            return

        #
        # TELEPORT
        #

        if (
            self.agent.movement_mode
            == "teleport"
        ):

            try:

                await (
                    self.agent
                    .move_to_public_transport_stop(
                        next_stop
                    )
                )

            except Exception as exc:

                logger.error(
                    "Transport {} could not "
                    "teleport from stop {} to "
                    "stop {}: {}".format(
                        self.agent.name,
                        origin_stop,
                        next_stop,
                        exc
                    )
                )

                self.discard_pending_movement()

                self.set_next_state(
                    TRANSPORT_WAITING
                )

                return

            # Teleport movement returns only after the
            # vehicle has physically reached the stop.
            self.set_pending_segment_movement(
                origin_stop,
                next_stop,
            )
            self.complete_pending_movement()

            self.agent.current_stop = (
                next_stop
            )

            self.agent.next_stop = None

            self.set_next_state(
                TRANSPORT_IN_DEST
            )

            return

        #
        # ROUTE
        #

        if (
            self.agent.dest
            == destination
        ):

            if (
                self.agent.is_in_destination()
            ):

                if not self.complete_pending_movement():
                    route_distance = None
                    if self.agent.route_count > 0:
                        route_distance = self.agent.last_route_distance
                    self.set_pending_segment_movement(
                        origin_stop,
                        next_stop,
                        route_distance=route_distance,
                    )
                    self.complete_pending_movement()

                self.agent.current_stop = (
                    next_stop
                )

                self.agent.next_stop = None

                self.set_next_state(
                    TRANSPORT_IN_DEST
                )

                return

            await self.agent.sleep(
                1
            )

            self.set_next_state(
                TRANSPORT_MOVING_TO_DESTINATION
            )

            return

        try:

            await (
                self.agent
                .move_to_public_transport_stop(
                    next_stop
                )
            )

            route_distance = None
            if self.agent.route_count > 0:
                route_distance = self.agent.last_route_distance

            self.set_pending_segment_movement(
                origin_stop,
                next_stop,
                route_distance=route_distance,
            )

        except AlreadyInDestination:

            self.agent.dest = (
                destination
            )

            self.set_pending_segment_movement(
                origin_stop,
                next_stop,
                route_distance=0.0,
            )
            self.complete_pending_movement()

            self.agent.current_stop = (
                next_stop
            )

            self.agent.next_stop = None

            self.set_next_state(
                TRANSPORT_IN_DEST
            )

            return

        except PathRequestException:

            logger.error(
                "Transport {} could not obtain "
                "route from stop {} to stop {}".format(
                    self.agent.name,
                    origin_stop,
                    next_stop
                )
            )

            self.discard_pending_movement()

            await self.agent.sleep(
                1
            )

            self.set_next_state(
                TRANSPORT_MOVING_TO_DESTINATION
            )

            return

        await self.agent.sleep(
            1
        )

        self.set_next_state(
            TRANSPORT_MOVING_TO_DESTINATION
        )


class PublicTransportInStopState(
    PublicTransportStrategyBehaviour
):
    """
    Process normal PublicTransport arrival at the current stop.

    Despite the historical ``TRANSPORT_IN_DEST`` status name, this is a normal,
    recurrent operational stop state rather than a terminal state.

    It is also the initial FSM state so a newly started vehicle processes its
    configured origin stop before the first departure.

    Passengers whose requested destination is the current stop are dropped
    first. Their capacity is released before Presence and stop-arrival
    notification are published.

    This ordering ensures that ``free_capacity`` advertised to the stop already
    reflects alighting passengers.

    After alighting and arrival publication, execution always enters
    ``TRANSPORT_BOARDING`` so waiting customers may request boarding.
    """
    async def on_start(
        self
    ):

        await super().on_start()

        self.agent.status = (
            TRANSPORT_IN_DEST
        )

    async def run(
        self
    ):
        """
        Process arrival, alighting, Presence, and stop notification.

        Passengers destined for the current stop leave before new boarding begins.

        Each successful alighting clears only that passenger's transport-side
        service context and restores one unit of vehicle capacity.

        PublicTransport Presence is then published with the updated capacity.

        The current stop receives a ``public_transport_vehicle_arrival``
        notification carrying that same post-alighting free capacity.

        Boarding is opened only after these operations and execution advances to
        ``TRANSPORT_BOARDING``.
        """
        logger.info(
            "Transport {} arrived at stop {} "
            "on pattern {}".format(
                self.agent.name,
                self.agent.current_stop,
                self.agent.pattern_id
            )
        )

        #
        # Passengers leave before new Customers
        # are allowed to board.
        #

        await self.drop_customers()

        #
        # Detect whether this Stop closes one complete
        # execution of the current Pattern.
        #

        pattern_completed = False

        if (
            self.agent.route_type
            == "end-to-end"
        ):

            pattern_completed = (
                self.agent.is_pattern_finished()
            )

        elif (
            self.agent.route_type
            == "circular"
        ):

            pattern_completed = (
                bool(
                    self.agent.stop_list
                )
                and self.agent.current_stop
                == self.agent.start_stop
                and self.agent.rounds > 0
            )

        onboard = (
            self.agent.capacity
            - self.agent.current_capacity
        )

        self.agent.publish_public_transport_presence()

        await self.inform_stop_arrival()

        self.set_next_state(
            TRANSPORT_BOARDING
        )

class PublicTransportBoardingState(
    PublicTransportStrategyBehaviour
):
    """
    Process PublicTransport boarding requests during the current stop dwell.

    The vehicle receives direct REQUEST_PROTOCOL / REQUEST_PERFORMATIVE
    ``public_transport_board`` messages one at a time.

    A boarding request is valid only when:

    - canonical service identity matches this vehicle and customer;
    - requested Pattern equals the active directional Pattern;
    - requested origin stop equals the current stop;
    - a destination stop is supplied and reachable under the current Pattern;
    - free capacity remains;
    - the customer is not already onboard.

    Valid requests add the customer to the vehicle, consume one capacity unit,
    emit the vehicle-owned ``service_assigned`` boarding event, acknowledge the
    customer, update stop queue state, and publish new Presence.

    Invalid requests are explicitly refused and any provisional service context
    is cleaned when appropriate.

    After every processed message the state returns to itself, allowing several
    customers to board during one stop visit.

    Departure occurs after one complete ``stop_time`` receive interval without
    a message, at which point execution returns to ``TRANSPORT_WAITING`` for
    next-stop selection.
    """
    async def on_start(
        self
    ):

        await super().on_start()

        self.agent.status = (
            TRANSPORT_BOARDING
        )

    async def run(
        self
    ):
        """
        Process one boarding message or close the current stop dwell.

        No received message during ``stop_time`` ends boarding and returns the
        vehicle to ``TRANSPORT_WAITING``.

        Messages using another protocol, performative, malformed payload, or
        unrelated request type are ignored by self-transitioning to
        ``TRANSPORT_BOARDING``.

        A syntactically valid boarding request first establishes or reuses a
        provisional service context for the requesting customer.

        The request is then validated against canonical service identity, Pattern,
        current stop, destination reachability, available capacity, and existing
        onboard state.

        Valid requests are accepted through ``accept_customer()``; all other
        requests are explicitly rejected through ``reject_customer()``.

        After either result, execution remains in ``TRANSPORT_BOARDING`` so
        additional customers may be processed during the same stop visit.
        """
        msg = await self.receive(
            timeout=self.agent.stop_time
        )

        if not msg:

            self.set_next_state(
                TRANSPORT_WAITING
            )

            return

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
            or performative
            != REQUEST_PERFORMATIVE
        ):

            self.set_next_state(
                TRANSPORT_BOARDING
            )

            return

        try:

            content = json.loads(
                msg.body
            )

        except Exception:

            self.set_next_state(
                TRANSPORT_BOARDING
            )

            return

        if content.get(
            "request_type"
        ) != (
            "public_transport_board"
        ):

            self.set_next_state(
                TRANSPORT_BOARDING
            )

            return

        customer_id = str(
            msg.sender
        )

        bare_customer_id = self.agent.bare_jid(
            customer_id
        )

        service_id = content.get(
            "service_id"
        )

        identity_valid = (
            service_id is not None
            and content.get("modality") == self.METRICS_MODALITY
            and self.agent.bare_jid(content.get("user_id")) == bare_customer_id
            and self.agent.bare_jid(content.get("transport_id"))
            == self.agent.bare_jid(self.agent.jid)
        )

        context = None

        if identity_valid:
            context = self.get_or_create_service_context(
                service_id=service_id,
                user_id=bare_customer_id,
                transport_id=self.agent.jid,
            )
            identity_valid = (
                context is not None
                and self.message_matches_service(
                    content,
                    service_id=service_id,
                )
            )

        destination_stop = (
            content.get(
                "destination_stop"
            )
        )

        already_onboard = (
            customer_id
            in self.agent.get(
                "current_customer"
            )
        )

        valid = (
            identity_valid

            and content.get(
                "pattern_id"
            ) == self.agent.pattern_id

            and content.get(
                "origin_stop"
            ) == self.agent.current_stop

            and destination_stop
            is not None

            and self.agent.current_capacity
            > 0

            and not already_onboard

            and self.agent.can_reach_stop(
                destination_stop
            )
        )

        if valid:

            await self.accept_customer(
                customer_id,
                content
            )

        else:

            await self.reject_customer(
                customer_id,
                content
            )

        self.set_next_state(
            TRANSPORT_BOARDING
        )


class FSMPublicTransportStrategyBehaviour(
    FSMSimfleetBehaviour
):
    """
    Finite-state operational strategy for a persistent PublicTransport vehicle.

    The FSM contains four recurrent states:

    ``TRANSPORT_WAITING``
        Select the next stop and resolve Pattern boundaries.

    ``TRANSPORT_MOVING_TO_DESTINATION``
        Execute and measure one physical stop-to-stop vehicle segment.

    ``TRANSPORT_IN_DEST``
        Process normal stop arrival, alighting, Presence, and stop
        notification. Despite its historical name, this is not a terminal
        state and is also the initial FSM state.

    ``TRANSPORT_BOARDING``
        Process zero or more customer boarding requests until the stop dwell
        closes.

    Normal operation repeats:

        stop processing -> boarding -> next-stop selection ->
        movement -> stop processing.

    Multiple passenger services may coexist on the vehicle, while each physical
    segment is emitted once with onboard and capacity observations.

    The FSM has no normal terminal state. PublicTransport vehicles remain
    persistent resources and continue through stops and directional Patterns
    until stopped externally or by the surrounding simulation lifecycle.

    Generic FSM lifecycle instrumentation is inherited from
    FSMSimfleetBehaviour.
    """
    def setup(
        self
    ):
        """
        Register PublicTransport vehicle states and permitted transitions.
        """
        self.add_state(
            TRANSPORT_WAITING,
            PublicTransportSelectStopState(),
        )

        self.add_state(
            TRANSPORT_MOVING_TO_DESTINATION,
            PublicTransportMovingState(),
        )

        self.add_state(
            TRANSPORT_IN_DEST,
            PublicTransportInStopState(),
            initial=True,
        )

        self.add_state(
            TRANSPORT_BOARDING,
            PublicTransportBoardingState(),
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING,
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_MOVING_TO_DESTINATION,
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_IN_DEST,
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_MOVING_TO_DESTINATION,
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_IN_DEST,
        )

        self.add_transition(
            TRANSPORT_IN_DEST,
            TRANSPORT_BOARDING,
        )

        self.add_transition(
            TRANSPORT_BOARDING,
            TRANSPORT_BOARDING,
        )

        self.add_transition(
            TRANSPORT_BOARDING,
            TRANSPORT_WAITING,
        )
