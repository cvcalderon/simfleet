import json
from collections import deque

from loguru import logger
from spade.message import Message

from simfleet.common.agents.fleetmanager import (
    FleetManagerStrategyBehaviour,
)

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
)

from simfleet.utils.helpers import distance_in_meters


class PublicTransportFleetManagerStrategy(
    FleetManagerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        errors = self.agent.validate_network()

        if errors:
            logger.error(
                "Public transport network validation failed in {}: {}".format(
                    self.agent.name,
                    errors
                )
            )
        else:
            logger.info(
                "Public transport network validated in {}".format(
                    self.agent.name
                )
            )

    def get_accessible_stops(
        self,
        position,
        max_distance
    ):

        result = []

        for stop_name, stop in self.agent.get_stops().items():

            stop_position = stop.get(
                "position"
            )

            if stop_position is None:
                continue

            distance = distance_in_meters(
                position,
                stop_position
            )

            if distance <= max_distance:

                result.append(
                    {
                        "stop": stop_name,
                        "distance": distance,
                    }
                )

        result.sort(
            key=lambda item:
                item["distance"]
        )

        return result

    def build_walking_leg(
        self,
        origin,
        destination,
        distance,
        origin_stop=None,
        destination_stop=None
    ):

        leg = {
            "type": "walking",
            "origin": origin,
            "destination": destination,
            "distance": distance,
        }

        if origin_stop is not None:

            leg["origin_stop"] = (
                self.agent.get_stop_info(
                    origin_stop
                )
            )

        if destination_stop is not None:

            leg["destination_stop"] = (
                self.agent.get_stop_info(
                    destination_stop
                )
            )

        return leg

    def build_public_transport_leg(
        self,
        pattern_id,
        origin_stop,
        destination_stop
    ):

        pattern = self.agent.get_pattern(
            pattern_id
        )

        if pattern is None:
            return None

        path = self.agent.get_pattern_path(
            pattern_id,
            origin_stop,
            destination_stop
        )

        if not path:
            return None

        return {
            "type": "public_transport",

            "mode":
                pattern.get("mode"),

            "route_id":
                pattern.get("route_id"),

            "pattern_id":
                pattern_id,

            "origin_stop":
                self.agent.get_stop_info(
                    origin_stop
                ),

            "destination_stop":
                self.agent.get_stop_info(
                    destination_stop
                ),

            "number_of_stops":
                len(path) - 1,
        }

    def build_journey(self, legs):

        public_transport_legs = [
            leg
            for leg in legs
            if leg.get("type")
            == "public_transport"
        ]

        walking_legs = [
            leg
            for leg in legs
            if leg.get("type")
            == "walking"
        ]

        return {
            "transfers":
                max(
                    len(
                        public_transport_legs
                    ) - 1,
                    0
                ),

            "walking_distance":
                sum(
                    leg.get(
                        "distance",
                        0
                    )
                    for leg
                    in walking_legs
                ),

            "public_transport_stops":
                sum(
                    leg.get(
                        "number_of_stops",
                        0
                    )
                    for leg
                    in public_transport_legs
                ),

            "legs":
                legs,
        }

    def journey_key(
        self,
        journey
    ):

        key = []

        for leg in journey.get(
            "legs",
            []
        ):

            if leg.get(
                "type"
            ) == "public_transport":

                key.append(
                    (
                        "public_transport",

                        leg.get(
                            "pattern_id"
                        ),

                        self._stop_id(
                            leg.get(
                                "origin_stop"
                            )
                        ),

                        self._stop_id(
                            leg.get(
                                "destination_stop"
                            )
                        ),
                    )
                )

            elif leg.get(
                "type"
            ) == "walking":

                key.append(
                    (
                        "walking",

                        self._stop_id(
                            leg.get(
                                "origin_stop"
                            )
                        ),

                        self._stop_id(
                            leg.get(
                                "destination_stop"
                            )
                        ),
                    )
                )

        return tuple(
            key
        )

    def _stop_id(
        self,
        stop
    ):

        if stop is None:
            return None

        return stop.get(
            "id"
        )

    def generate_journey_candidates(
        self,
        origin,
        destination,
        max_access_walking_distance,
        max_transfer_walking_distance,
        max_transfers
    ):

        origin_stops = self.get_accessible_stops(
            origin,
            max_access_walking_distance
        )

        destination_stops = self.get_accessible_stops(
            destination,
            max_access_walking_distance
        )

        if not origin_stops:
            return []

        if not destination_stops:
            return []

        destination_map = {
            item["stop"]:
                item["distance"]
            for item
            in destination_stops
        }

        max_public_transport_legs = (
            max_transfers + 1
        )

        queue = deque()

        journeys = []
        seen_journeys = set()

        #
        # Initial states
        #

        for origin_item in origin_stops:

            stop_name = origin_item[
                "stop"
            ]

            walking_distance = origin_item[
                "distance"
            ]

            legs = []

            if walking_distance > 0:

                legs.append(
                    self.build_walking_leg(
                        origin=origin,
                        destination=(
                            self.agent.get_stop_position(
                                stop_name
                            )
                        ),
                        distance=walking_distance,
                        destination_stop=stop_name,
                    )
                )

            queue.append(
                {
                    "current_stop":
                        stop_name,

                    "legs":
                        legs,

                    "pt_legs":
                        0,

                    "used_patterns":
                        set(),

                    "visited_stops":
                        {stop_name},
                }
            )

        #
        # BFS
        #

        while queue:

            state = queue.popleft()

            if (
                state["pt_legs"]
                >= max_public_transport_legs
            ):
                continue

            current_stop = state[
                "current_stop"
            ]

            patterns = (
                self.agent.get_patterns_for_stop(
                    current_stop
                )
            )

            for pattern_id in patterns:

                #
                # Do not reuse exact same pattern
                #

                if pattern_id in state[
                    "used_patterns"
                ]:
                    continue

                #
                # Pattern needs at least one vehicle
                #

                vehicles = (
                    self.agent
                    .get_vehicles_for_pattern(
                        pattern_id
                    )
                )

                if not vehicles:
                    continue

                reachable_stops = (
                    self.agent
                    .get_reachable_stops_on_pattern(
                        pattern_id,
                        current_stop
                    )
                )

                for destination_stop in (
                    reachable_stops
                ):

                    #
                    # Avoid cycles
                    #

                    if destination_stop in state[
                        "visited_stops"
                    ]:
                        continue

                    pt_leg = (
                        self.build_public_transport_leg(
                            pattern_id,
                            current_stop,
                            destination_stop
                        )
                    )

                    if pt_leg is None:
                        continue

                    new_legs = (
                        list(state["legs"])
                        + [pt_leg]
                    )

                    new_pt_legs = (
                        state["pt_legs"] + 1
                    )

                    new_used_patterns = (
                        set(
                            state["used_patterns"]
                        )
                    )

                    new_used_patterns.add(
                        pattern_id
                    )

                    new_visited_stops = (
                        set(
                            state["visited_stops"]
                        )
                    )

                    new_visited_stops.add(
                        destination_stop
                    )

                    #
                    # Can finish here?
                    #

                    if destination_stop in (
                        destination_map
                    ):

                        final_legs = list(
                            new_legs
                        )

                        final_walking_distance = (
                            destination_map[
                                destination_stop
                            ]
                        )

                        if final_walking_distance > 0:

                            final_legs.append(
                                self.build_walking_leg(
                                    origin=(
                                        self.agent
                                        .get_stop_position(
                                            destination_stop
                                        )
                                    ),
                                    destination=destination,
                                    distance=(
                                        final_walking_distance
                                    ),
                                    origin_stop=(
                                        destination_stop
                                    ),
                                )
                            )

                        journey = (
                            self.build_journey(
                                final_legs
                            )
                        )

                        key = self.journey_key(
                            journey
                        )

                        if key not in seen_journeys:

                            seen_journeys.add(
                                key
                            )

                            journeys.append(
                                journey
                            )

                        #
                        # Do not add unnecessary
                        # extra transfers after reaching
                        # destination.
                        #

                        continue

                    #
                    # No more PT legs available
                    #

                    if (
                        new_pt_legs
                        >= max_public_transport_legs
                    ):
                        continue

                    #
                    # Transfer at same physical Stop
                    #

                    queue.append(
                        {
                            "current_stop":
                                destination_stop,

                            "legs":
                                new_legs,

                            "pt_legs":
                                new_pt_legs,

                            "used_patterns":
                                new_used_patterns,

                            "visited_stops":
                                new_visited_stops,
                        }
                    )

                    #
                    # Walking transfers
                    #

                    transfer_stops = (
                        self.agent
                        .get_transfer_stops(
                            destination_stop,
                            max_transfer_walking_distance
                        )
                    )

                    for transfer in transfer_stops:

                        transfer_stop = transfer[
                            "to_stop"
                        ]

                        if transfer_stop in (
                            new_visited_stops
                        ):
                            continue

                        #
                        # No reason to walk to
                        # a Stop without services.
                        #

                        if not (
                            self.agent
                            .get_patterns_for_stop(
                                transfer_stop
                            )
                        ):
                            continue

                        walking_leg = (
                            self.build_walking_leg(
                                origin=(
                                    self.agent
                                    .get_stop_position(
                                        destination_stop
                                    )
                                ),

                                destination=(
                                    self.agent
                                    .get_stop_position(
                                        transfer_stop
                                    )
                                ),

                                distance=(
                                    transfer[
                                        "distance"
                                    ]
                                ),

                                origin_stop=(
                                    destination_stop
                                ),

                                destination_stop=(
                                    transfer_stop
                                ),
                            )
                        )

                        transfer_visited_stops = (
                            set(
                                new_visited_stops
                            )
                        )

                        transfer_visited_stops.add(
                            transfer_stop
                        )

                        queue.append(
                            {
                                "current_stop":
                                    transfer_stop,

                                "legs":
                                    new_legs
                                    + [walking_leg],

                                "pt_legs":
                                    new_pt_legs,

                                "used_patterns":
                                    set(
                                        new_used_patterns
                                    ),

                                "visited_stops":
                                    transfer_visited_stops,
                            }
                        )

        return journeys

    async def send_inform(
        self,
        receiver,
        content
    ):

        reply = Message()
        reply.to = str(receiver)

        reply.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        reply.set_metadata(
            "performative",
            INFORM_PERFORMATIVE
        )

        reply.body = json.dumps(
            content
        )

        await self.send(
            reply
        )

    async def send_refuse(
        self,
        receiver,
        content
    ):

        reply = Message()
        reply.to = str(receiver)

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

    async def process_journey_request(
        self,
        msg,
        content
    ):

        origin = content.get(
            "origin"
        )

        destination = content.get(
            "dest"
        )

        if (
            origin is None
            or destination is None
        ):

            await self.send_refuse(
                msg.sender,
                {
                    "request_type":
                        "public_transport_journeys",

                    "reason":
                        "invalid_origin_or_destination",
                }
            )

            return

        max_access_walking_distance = (
            content.get(
                "max_access_walking_distance",
                600
            )
        )

        max_transfer_walking_distance = (
            content.get(
                "max_transfer_walking_distance",
                300
            )
        )

        max_transfers = content.get(
            "max_transfers",
            2
        )

        journeys = (
            self.generate_journey_candidates(
                origin=origin,
                destination=destination,
                max_access_walking_distance=(
                    max_access_walking_distance
                ),
                max_transfer_walking_distance=(
                    max_transfer_walking_distance
                ),
                max_transfers=max_transfers,
            )
        )

        await self.send_inform(
            msg.sender,
            {
                "request_type":
                    "public_transport_journeys",

                "journeys":
                    journeys,
            }
        )

    async def run(
        self
    ):

        msg = await self.receive(
            timeout=5
        )

        if not msg:
            return

        protocol = msg.get_metadata(
            "protocol"
        )

        performative = msg.get_metadata(
            "performative"
        )

        if protocol != (
            REQUEST_PROTOCOL
        ):
            return

        if performative != (
            REQUEST_PERFORMATIVE
        ):
            return

        try:

            content = json.loads(
                msg.body
            )

        except Exception:

            await self.send_refuse(
                msg.sender,
                {
                    "reason":
                        "invalid_json",
                }
            )

            return

        request_type = content.get(
            "request_type"
        )

        if request_type == (
            "public_transport_journeys"
        ):
            await self.process_journey_request(
                msg,
                content
            )

            return

        await self.send_refuse(
            msg.sender,
            {
                "request_type":
                    request_type,

                "reason":
                    "unsupported_request",
            }
        )
