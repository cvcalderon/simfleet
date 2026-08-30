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


    METRICS_MODALITY = "public_transport"
    _METRICS_SERVICE_CONTEXTS_ATTR = "_metrics_service_contexts"
    _METRICS_PENDING_MOVEMENT_ATTR = "_metrics_pending_movement"
    _METRICS_MOVEMENT_PHASES = {"approach", "service", "auxiliary"}

    def _metrics_modality(self):
        return self.METRICS_MODALITY

    def get_service_contexts(self):
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
        if service_id is None:
            return None
        return self.get_service_contexts().get(str(service_id))

    def get_service_context_for_user(self, user_id):
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
        contexts = self.get_service_contexts()
        if str(service_id) not in contexts:
            return True
        del contexts[str(service_id)]
        return True

    def _service_event_details(self, context):
        return {
            "modality": context["modality"],
            "service_id": context["service_id"],
            "user_id": context.get("user_id"),
            "transport_id": context.get("transport_id"),
        }

    def add_service_identifiers(self, content, service_id, user_id=None):
        context = self.get_service_context(service_id)
        if context is None:
            raise ValueError("Unknown public-transport service_id.")
        if user_id is not None and self.agent.bare_jid(user_id) != context.get("user_id"):
            raise ValueError("Public-transport user_id does not match service context.")
        result = dict(content or {})
        result.update(self._service_event_details(context))
        return result

    def message_matches_service(self, content, service_id=None):
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

    def setup(
        self
    ):

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
