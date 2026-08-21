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
                }
            )

            await self.send(
                msg
            )

            self.agent.remove_customer_in_transport(
                customer_id
            )

            self.agent.current_capacity += 1

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
            content
        )

        await self.send(
            reply
        )

        await self.inform_stop_customer_boarded(
            customer_id
        )

        self.agent.publish_public_transport_presence()

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
            content
        )

        await self.send(
            reply
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
                "Transport {} has no next stop while moving".format(
                    self.agent.name
                )
            )

            self.set_next_state(
                TRANSPORT_WAITING
            )

            return

        destination = (
            self.agent.get_stop_position(
                next_stop
            )
        )

        if destination is None:

            logger.error(
                "Transport {} cannot resolve position for stop {}".format(
                    self.agent.name,
                    next_stop
                )
            )

            self.set_next_state(
                TRANSPORT_WAITING
            )

            return

        #
        # TELEPORT MOVEMENT
        #

        if self.agent.movement_mode == (
            "teleport"
        ):

            try:

                await self.agent.move_to_public_transport_stop(
                    next_stop
                )

            except Exception as exc:

                logger.error(
                    "Transport {} could not teleport to stop {}: {}".format(
                        self.agent.name,
                        next_stop,
                        exc
                    )
                )

                self.set_next_state(
                    TRANSPORT_WAITING
                )

                return

            self.agent.current_stop = (
                next_stop
            )

            self.agent.next_stop = None

            self.set_next_state(
                TRANSPORT_IN_DEST
            )

            return

        #
        # ROUTE MOVEMENT
        #

        if self.agent.movement_mode != (
            "route"
        ):

            logger.error(
                "Transport {} has unsupported movement mode {}".format(
                    self.agent.name,
                    self.agent.movement_mode
                )
            )

            self.set_next_state(
                TRANSPORT_WAITING
            )

            return

        #
        # MovableMixin.dest represents the destination
        # of the currently active movement.
        #
        # We must first verify that this destination is
        # the same stop that the Public Transport FSM
        # currently wants to reach.
        #

        if self.agent.dest == destination:

            #
            # A route towards this stop already exists.
            #
            # Only now is is_in_destination() meaningful.
            #

            if self.agent.is_in_destination():

                self.agent.current_stop = (
                    next_stop
                )

                self.agent.next_stop = None

                self.set_next_state(
                    TRANSPORT_IN_DEST
                )

                return

            #
            # The vehicle is still travelling toward the
            # same destination.
            #
            # Do not request the route again.
            #

            await self.agent.sleep(
                1
            )

            self.set_next_state(
                TRANSPORT_MOVING_TO_DESTINATION
            )

            return

        #
        # No route towards the current next_stop is active.
        #
        # self.agent.dest may be None or may still contain
        # the destination of the previous segment.
        #

        try:

            await self.agent.move_to_public_transport_stop(
                next_stop
            )

        except AlreadyInDestination:

            #
            # Physically the vehicle is already at the
            # requested stop, although MovableMixin.dest
            # may still refer to the previous movement.
            #
            # Synchronize both states.
            #

            self.agent.dest = (
                destination
            )

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
                "Transport {} could not obtain route to stop {}".format(
                    self.agent.name,
                    next_stop
                )
            )

            await self.agent.sleep(
                1
            )

            self.set_next_state(
                TRANSPORT_MOVING_TO_DESTINATION
            )

            return

        #
        # move_to() starts MovableMixin.MovingBehaviour
        # asynchronously.
        #
        # It does not wait until the vehicle has reached
        # the destination.
        #

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
            "Transport {} arrived at stop {} on pattern {}".format(
                self.agent.name,
                self.agent.current_stop,
                self.agent.pattern_id
            )
        )

        await self.drop_customers()

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
            content.get(
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
