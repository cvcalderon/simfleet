import json

from asyncio import CancelledError

from loguru import logger

from spade.template import Template
from spade.behaviour import CyclicBehaviour
from spade.message import Message

from simfleet.common.agents.transport import (
    TransportAgent,
)

from simfleet.communications.protocol import (
    QUERY_PROTOCOL,
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
)


class PublicTransportAgent(
    TransportAgent
):

    def __init__(
        self,
        agentjid,
        password,
        **kwargs
    ):

        super().__init__(
            agentjid,
            password
        )

        #
        # Initial configuration.
        #

        self.pattern_id = kwargs.get(
            "pattern_id"
        )

        self.start_stop = kwargs.get(
            "start_stop"
        )

        self.stop_time = kwargs.get(
            "stop_time",
            1
        )

        if not self.pattern_id:

            raise ValueError(
                "Public transport vehicle needs a pattern_id."
            )

        if not self.start_stop:

            raise ValueError(
                "Public transport vehicle needs a start_stop."
            )

        if self.stop_time < 0:

            raise ValueError(
                "Public transport stop_time cannot be negative."
            )

        #
        # Structural information.
        #

        self.pattern = None

        self.route_id = None
        self.mode = None
        self.route_type = None
        self.movement_mode = None

        self.next_pattern_id = None
        self.travel_times = None

        self.stop_list = []
        self.stops = {}

        #
        # Runtime state.
        #

        self.current_stop = None
        self.next_stop = None

        self.capacity = None
        self.current_capacity = None

        self.rounds = 0

        #
        # Marks that a new directional Pattern has just
        # been resolved at the terminal stop.
        #

        self.pattern_changed = False

    def set_capacity(
        self,
        capacity
    ):

        capacity = int(
            capacity
        )

        if capacity <= 0:

            raise ValueError(
                "Public transport capacity must be greater than zero."
            )

        self.capacity = capacity
        self.current_capacity = capacity

    def dependencies_ready(
        self
    ):

        if not super().dependencies_ready():
            return False

        if self.pattern is None:
            return False

        return True

    def get_registration_content(
        self
    ):

        content = (
            super().get_registration_content()
        )

        content.update(
            {
                "resource_type":
                    "vehicle",

                "pattern_id":
                    self.pattern_id,

                "capacity":
                    self.capacity,
            }
        )

        return content

    async def set_pattern_data(
        self,
        pattern,
        stops
    ):

        if pattern.get(
            "pattern_id"
        ) != self.pattern_id:

            raise ValueError(
                "Received pattern does not match vehicle pattern_id."
            )

        pattern_stops = pattern.get(
            "stops",
            []
        )

        if not pattern_stops:

            raise ValueError(
                "Public transport pattern has no stops."
            )

        route_id = pattern.get(
            "route_id"
        )

        mode = pattern.get(
            "mode"
        )

        route_type = pattern.get(
            "route_type"
        )

        movement_mode = pattern.get(
            "movement_mode"
        )

        next_pattern_id = pattern.get(
            "next_pattern_id"
        )

        travel_times = pattern.get(
            "travel_times"
        )

        if mode not in (
            "bus",
            "metro",
            "tram"
        ):

            raise ValueError(
                "Unsupported public transport mode: {}".format(
                    mode
                )
            )

        if route_type not in (
            "end-to-end",
            "circular"
        ):

            raise ValueError(
                "Unsupported route_type: {}".format(
                    route_type
                )
            )

        if movement_mode not in (
            "route",
            "teleport"
        ):

            raise ValueError(
                "Unsupported movement_mode: {}".format(
                    movement_mode
                )
            )

        resolved_stops = {
            stop["id"]: stop
            for stop in stops
        }

        if self.start_stop not in (
            pattern_stops
        ):

            raise ValueError(
                "Start stop {} is not part of pattern {}.".format(
                    self.start_stop,
                    self.pattern_id
                )
            )

        if self.start_stop not in (
            resolved_stops
        ):

            raise ValueError(
                "Start stop {} has not been resolved.".format(
                    self.start_stop
                )
            )

        start_position = (
            resolved_stops[
                self.start_stop
            ].get(
                "position"
            )
        )

        if start_position is None:

            raise ValueError(
                "Start stop {} has no position.".format(
                    self.start_stop
                )
            )

        #
        # Only after ALL validations succeeded
        # do we change the operational state.
        #

        self.pattern = dict(
            pattern
        )

        self.route_id = (
            route_id
        )

        self.mode = (
            mode
        )

        self.route_type = (
            route_type
        )

        self.movement_mode = (
            movement_mode
        )

        self.next_pattern_id = (
            next_pattern_id
        )

        self.travel_times = (
            travel_times
        )

        self.stop_list = list(
            pattern_stops
        )

        self.stops = (
            resolved_stops
        )

        self.current_stop = (
            self.start_stop
        )

        self.next_stop = None

        await self.set_position(
            start_position
        )

    def get_next_stop(
        self
    ):

        if not self.stop_list:
            return None

        if self.current_stop not in (
            self.stop_list
        ):
            return None

        current_index = (
            self.stop_list.index(
                self.current_stop
            )
        )

        next_index = (
            current_index + 1
        )

        if next_index < len(
            self.stop_list
        ):

            return self.stop_list[
                next_index
            ]

        if self.route_type == (
            "circular"
        ):

            self.rounds += 1

            return self.stop_list[0]

        return None

    def is_pattern_finished(
        self
    ):

        if not self.stop_list:
            return True

        if self.route_type == (
            "circular"
        ):
            return False

        return (
            self.current_stop
            == self.stop_list[-1]
        )

    def get_next_pattern_id(
        self
    ):

        return self.next_pattern_id

    def can_reach_stop(
        self,
        destination_stop
    ):

        if destination_stop not in (
            self.stop_list
        ):
            return False

        if self.current_stop not in (
            self.stop_list
        ):
            return False

        if destination_stop == (
            self.current_stop
        ):
            return False

        if self.route_type == (
            "circular"
        ):
            return True

        current_index = (
            self.stop_list.index(
                self.current_stop
            )
        )

        destination_index = (
            self.stop_list.index(
                destination_stop
            )
        )

        return (
            destination_index
            > current_index
        )

    def get_stop(
        self,
        stop_id
    ):

        return self.stops.get(
            stop_id
        )

    def get_stop_position(
        self,
        stop_id
    ):

        stop = self.get_stop(
            stop_id
        )

        if stop is None:
            return None

        return stop.get(
            "position"
        )

    def get_stop_jid(
        self,
        stop_id
    ):

        stop = self.get_stop(
            stop_id
        )

        if stop is None:
            return None

        return stop.get(
            "jid"
        )

    def get_teleport_travel_time(
        self,
        destination_stop
    ):

        if self.movement_mode != (
            "teleport"
        ):
            return None

        if self.travel_times is None:

            raise ValueError(
                "Teleport pattern {} has no travel_times.".format(
                    self.pattern_id
                )
            )

        if self.current_stop not in (
            self.stop_list
        ):

            raise ValueError(
                "Current stop {} is not part of pattern {}.".format(
                    self.current_stop,
                    self.pattern_id
                )
            )

        current_index = (
            self.stop_list.index(
                self.current_stop
            )
        )

        if (
            current_index + 1
            < len(self.stop_list)
        ):

            expected_destination = (
                self.stop_list[
                    current_index + 1
                ]
            )

            if destination_stop != (
                expected_destination
            ):

                raise ValueError(
                    "Stop {} is not the next stop after {}.".format(
                        destination_stop,
                        self.current_stop
                    )
                )

            return self.travel_times[
                current_index
            ]

        if self.route_type == (
            "circular"
        ):

            if destination_stop != (
                self.stop_list[0]
            ):

                raise ValueError(
                    "Invalid circular destination stop {}.".format(
                        destination_stop
                    )
                )

            return self.travel_times[
                current_index
            ]

        raise ValueError(
            "No teleport segment exists from {} to {}.".format(
                self.current_stop,
                destination_stop
            )
        )

    async def move_to_public_transport_stop(
        self,
        stop_id
    ):

        destination = (
            self.get_stop_position(
                stop_id
            )
        )

        if destination is None:

            raise ValueError(
                "Unknown public transport stop {}.".format(
                    stop_id
                )
            )

        self.next_stop = (
            stop_id
        )

        if self.movement_mode == (
            "route"
        ):

            await self.move_to(
                destination
            )

            return

        if self.movement_mode == (
            "teleport"
        ):

            travel_time = (
                self.get_teleport_travel_time(
                    stop_id
                )
            )

            self.dest = destination

            await self.sleep(
                travel_time
            )

            await self.set_position(
                destination
            )

            return

        raise ValueError(
            "Unknown movement mode: {}".format(
                self.movement_mode
            )
        )

    def get_public_transport_presence(
        self
    ):

        return {
            "p":
                self.get_position(),

            "m":
                self.mode,

            "r":
                self.route_id,

            "pt":
                self.pattern_id,

            "s":
                self.current_stop,

            "n":
                self.next_stop,

            "f":
                self.current_capacity,

            "c":
                self.capacity,
        }

    def publish_public_transport_presence(
        self
    ):

        self.set_agent_presence(
            status=json.dumps(
                self.get_public_transport_presence()
            )
        )

    def start_pattern_resolution(
        self
    ):

        template = Template()

        template.set_metadata(
            "protocol",
            QUERY_PROTOCOL
        )

        pattern_behaviour = (
            PublicTransportPatternBehaviour()
        )

        self.add_behaviour(
            pattern_behaviour,
            template
        )

    def prepare_next_pattern(
        self
    ):

        next_pattern_id = (
            self.get_next_pattern_id()
        )

        if next_pattern_id is None:
            return False

        current_stop = (
            self.current_stop
        )

        logger.info(
            "Transport {} changing pattern from {} to {}".format(
                self.name,
                self.pattern_id,
                next_pattern_id
            )
        )

        #
        # Patterns are directional.
        #
        # We never reverse the current stop list.
        # Instead, the vehicle loads the next configured
        # directional Pattern.
        #

        self.pattern_id = (
            next_pattern_id
        )

        #
        # The terminal stop of the previous Pattern is
        # the initial stop of the next Pattern.
        #

        self.start_stop = (
            current_stop
        )

        #
        # Clear Pattern-dependent structural information.
        #
        # Capacity and onboard customers are preserved.
        #

        self.pattern = None

        self.route_id = None
        self.mode = None
        self.route_type = None
        self.movement_mode = None

        self.next_pattern_id = None
        self.travel_times = None

        self.stop_list = []
        self.stops = {}

        self.next_stop = None

        #
        # Once the new Pattern has been resolved, the FSM
        # must process the shared terminal stop before
        # departing on the new direction.
        #

        self.pattern_changed = True

        self.start_pattern_resolution()

        return True

    def run_strategy(
        self
    ):
        """
        Starts the operational Public Transport strategy.

        QUERY_PROTOCOL is intentionally excluded from this
        behaviour because Pattern resolution is handled by
        PublicTransportPatternBehaviour.

        The operational FSM only receives REQUEST_PROTOCOL
        messages.
        """

        if self.running_strategy:
            return

        if self.strategy is None:
            logger.error(
                "Public transport {} has no strategy configured.".format(
                    self.name
                )
            )

            return

        template = Template()

        template.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        self.add_behaviour(
            self.strategy(),
            template
        )

        self.running_strategy = True

    async def setup(
        self
    ):

        await super().setup()

        self.start_pattern_resolution()

        self.ready = True


class PublicTransportPatternBehaviour(
    CyclicBehaviour
):

    async def request_pattern(
        self
    ):

        fleet_id = (
            self.agent.get_registration_fleet()
        )

        if fleet_id is None:
            return

        content = {
            "request_type":
                "public_transport_pattern",

            "pattern_id":
                self.agent.pattern_id,
        }

        msg = Message()

        msg.to = str(
            fleet_id
        )

        msg.set_metadata(
            "protocol",
            QUERY_PROTOCOL
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

    async def run(
        self
    ):

        try:

            if self.agent.pattern is not None:

                self.kill()

                return

            await self.request_pattern()

            msg = await self.receive(
                timeout=3
            )

            if not msg:
                return

            performative = (
                msg.get_metadata(
                    "performative"
                )
            )

            if performative == (
                INFORM_PERFORMATIVE
            ):

                content = json.loads(
                    msg.body
                )

                if content.get(
                    "request_type"
                ) != (
                    "public_transport_pattern"
                ):

                    return

                pattern = content.get(
                    "pattern"
                )

                stops = content.get(
                    "stops"
                )

                if (
                    pattern is None
                    or stops is None
                ):

                    logger.warning(
                        "Invalid pattern response received by transport {}".format(
                            self.agent.name
                        )
                    )

                    return

                await self.agent.set_pattern_data(
                    pattern,
                    stops
                )

                logger.info(
                    "Transport {} resolved pattern {}".format(
                        self.agent.name,
                        self.agent.pattern_id
                    )
                )

                self.kill()

                return

            if performative == (
                CANCEL_PERFORMATIVE
            ):

                logger.debug(
                    "Pattern {} not ready yet for transport {}.".format(
                        self.agent.pattern_id,
                        self.agent.name
                    )
                )

        except CancelledError:

            logger.debug(
                "Cancelling public transport pattern behaviour."
            )

        except Exception as exc:

            logger.error(
                "Exception resolving pattern {} for transport {}: {}".format(
                    self.agent.pattern_id,
                    self.agent.name,
                    exc
                )
            )
