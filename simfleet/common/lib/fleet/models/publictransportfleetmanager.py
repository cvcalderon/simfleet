import json
from loguru import logger
from asyncio import CancelledError

from spade.behaviour import CyclicBehaviour
from spade.message import Message
from spade.template import Template

from simfleet.common.agents.fleetmanager import FleetManagerAgent
from simfleet.utils.helpers import distance_in_meters

from simfleet.communications.protocol import (
    QUERY_PROTOCOL,
    REQUEST_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
)


class PublicTransportFleetManagerAgent(FleetManagerAgent):

    def __init__(self, agentjid, password, **kwargs):
        super().__init__(agentjid, password)

        self.patterns = {}

        patterns = kwargs.get("patterns", [])

        for pattern in patterns:
            self.add_pattern(pattern)

    async def setup(
        self
    ):

        await super().setup()

        #
        # FleetManager generic setup may already
        # have marked local setup as ready.
        # Public Transport still needs its
        # QUERY infrastructure.
        #

        self.ready = False

        template = Template()

        template.set_metadata(
            "protocol",
            QUERY_PROTOCOL
        )

        query_behaviour = (
            PublicTransportNetworkQueryBehaviour()
        )

        self.add_behaviour(
            query_behaviour,
            template
        )

        self.ready = True



    def add_pattern(self, pattern):

        if not self.validate_pattern(pattern):
            logger.error(
                "PublicTransportFleetManager {}: invalid pattern {}".format(
                    self.name,
                    pattern.get("pattern_id")
                )
            )
            return False

        pattern_id = pattern["pattern_id"]

        self.patterns[pattern_id] = dict(pattern)

        return True

    def validate_pattern(self, pattern):

        required_fields = [
            "pattern_id",
            "route_id",
            "mode",
            "route_type",
            "movement_mode",
            "stops",
        ]

        for field in required_fields:
            if field not in pattern:
                logger.error(
                    "Pattern {} missing required field '{}'".format(
                        pattern.get("pattern_id"),
                        field
                    )
                )
                return False

        if pattern["route_type"] not in [
            "end-to-end",
            "circular",
        ]:
            logger.error(
                "Pattern {} has invalid route_type '{}'".format(
                    pattern["pattern_id"],
                    pattern["route_type"]
                )
            )
            return False

        if pattern["movement_mode"] not in [
            "route",
            "teleport",
        ]:
            logger.error(
                "Pattern {} has invalid movement_mode '{}'".format(
                    pattern["pattern_id"],
                    pattern["movement_mode"]
                )
            )
            return False

        stops = pattern["stops"]

        if not isinstance(stops, list):
            logger.error(
                "Pattern {} stops must be a list".format(
                    pattern["pattern_id"]
                )
            )
            return False

        if len(stops) < 2:
            logger.error(
                "Pattern {} needs at least two stops".format(
                    pattern["pattern_id"]
                )
            )
            return False

        if len(stops) != len(set(stops)):
            logger.error(
                "Pattern {} contains duplicated stops".format(
                    pattern["pattern_id"]
                )
            )
            return False

        if pattern["movement_mode"] == "teleport":

            travel_times = pattern.get("travel_times")

            if travel_times is None:
                logger.error(
                    "Pattern {} needs travel_times for teleport movement".format(
                        pattern["pattern_id"]
                    )
                )
                return False

            if pattern["route_type"] == "circular":
                expected_times = len(stops)
            else:
                expected_times = len(stops) - 1

            if len(travel_times) != expected_times:
                logger.error(
                    "Pattern {} needs {} travel_times but has {}".format(
                        pattern["pattern_id"],
                        expected_times,
                        len(travel_times)
                    )
                )
                return False

            for travel_time in travel_times:
                if not isinstance(travel_time, (int, float)):
                    return False

                if travel_time < 0:
                    return False

        return True

    def get_pattern(self, pattern_id):
        return self.patterns.get(pattern_id)

    def get_patterns(self):
        return self.patterns

    def get_patterns_for_stop(self, stop_name):

        result = []

        for pattern_id, pattern in self.patterns.items():

            if stop_name in pattern.get("stops", []):
                result.append(pattern_id)

        return result

    def get_patterns_for_route(self, route_id):

        result = []

        for pattern_id, pattern in self.patterns.items():

            if pattern.get("route_id") == route_id:
                result.append(pattern_id)

        return result


    def _bare_jid(self, jid):

        if jid is None:
            return None

        return str(jid).split("/")[0]

    def get_stop(
        self,
        stop_id
    ):

        resource = (
            self.get_fleet_resources().get(
                stop_id
            )
        )

        if resource is None:
            return None

        if resource.get(
            "resource_type"
        ) != "stop":
            return None

        return resource

    def get_stops(
        self
    ):

        return {
            name: resource

            for name, resource
            in self.get_fleet_resources().items()

            if resource.get(
                "resource_type"
            ) == "stop"
        }

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

    def get_stop_info(
        self,
        stop_id
    ):

        stop = self.get_stop(
            stop_id
        )

        if stop is None:
            return None

        return {
            "id":
                stop_id,

            "jid":
                stop.get("jid"),

            "position":
                stop.get("position"),
        }

    def get_reachable_stops_on_pattern(
        self,
        pattern_id,
        origin_stop
    ):

        pattern = self.get_pattern(
            pattern_id
        )

        if pattern is None:
            return []

        stops = pattern.get(
            "stops",
            []
        )

        if origin_stop not in stops:
            return []

        origin_index = stops.index(
            origin_stop
        )

        if pattern.get("route_type") == "circular":

            return (
                stops[origin_index + 1:]
                + stops[:origin_index]
            )

        return stops[
            origin_index + 1:
        ]

    def get_pattern_path(
        self,
        pattern_id,
        origin_stop,
        destination_stop
    ):

        pattern = self.get_pattern(
            pattern_id
        )

        if pattern is None:
            return []

        stops = pattern.get(
            "stops",
            []
        )

        if origin_stop not in stops:
            return []

        if destination_stop not in stops:
            return []

        if origin_stop == destination_stop:
            return []

        origin_index = stops.index(
            origin_stop
        )

        destination_index = stops.index(
            destination_stop
        )

        if pattern.get("route_type") != "circular":

            if destination_index <= origin_index:
                return []

            return stops[
                origin_index:
                destination_index + 1
            ]

        if destination_index > origin_index:

            return stops[
                origin_index:
                destination_index + 1
            ]

        return (
            stops[origin_index:]
            + stops[:destination_index + 1]
        )

    def get_transfer_stops(
        self,
        stop_id,
        max_distance
    ):

        origin_position = (
            self.get_stop_position(
                stop_id
            )
        )

        if origin_position is None:
            return []

        result = []

        for candidate_id, candidate in (
            self.get_stops().items()
        ):

            if candidate_id == stop_id:
                continue

            candidate_position = (
                candidate.get(
                    "position"
                )
            )

            if candidate_position is None:
                continue

            distance = distance_in_meters(
                origin_position,
                candidate_position
            )

            if distance <= max_distance:
                result.append(
                    {
                        "from_stop":
                            stop_id,

                        "to_stop":
                            candidate_id,

                        "distance":
                            distance,
                    }
                )

        result.sort(
            key=lambda item:
            item["distance"]
        )

        return result

    def get_vehicles(
        self
    ):

        return {
            name: resource

            for name, resource
            in self.get_fleet_resources().items()

            if resource.get(
                "resource_type"
            ) == "vehicle"
        }

    def get_vehicles_for_pattern(
        self,
        pattern_id
    ):
        """
        Returns the public transport vehicles currently
        operating the requested directional Pattern.

        The Pattern advertised during REGISTER is only the
        vehicle's initial Pattern.

        Runtime Pattern information comes from Presence,
        where PublicTransportAgent publishes it using the
        'pt' field.

        REGISTER is used only as a fallback before the first
        valid Presence update is available.
        """

        result = []

        for resource in (
            self.get_vehicles().values()
        ):

            resource_jid = resource.get(
                "jid"
            )

            dynamic_data = None

            #
            # First try the live XMPP Presence.
            #

            if resource_jid:

                presence = (
                    self.get_resource_presence(
                        resource_jid
                    )
                )

                if self.is_resource_available(
                    presence
                ):
                    dynamic_data = (
                        self.get_resource_presence_data(
                            presence
                        )
                    )

            #
            # If direct Presence is not available, use the
            # Presence mirror stored in fleet_resources.
            #
            # This is already the fallback mechanism used by
            # FleetManagerAgent for registered resources.
            #

            if dynamic_data is None:

                mirrored_presence = (
                    resource.get(
                        "presence"
                    )
                )

                if (
                    self.is_resource_presence_mirror_available(
                        mirrored_presence
                    )
                ):
                    dynamic_data = (
                        self.get_resource_presence_mirror_data(
                            mirrored_presence
                        )
                    )

            #
            # Presence is the runtime source of truth.
            #

            if dynamic_data is not None:

                current_pattern_id = (
                    dynamic_data.get(
                        "pt"
                    )
                )

                if current_pattern_id == (
                    pattern_id
                ):
                    result.append(
                        resource
                    )

                continue

            #
            # Bootstrap fallback.
            #
            # Before the Vehicle has published its first
            # Presence, the initial Pattern from REGISTER is
            # still useful.
            #

            if resource.get(
                "pattern_id"
            ) == pattern_id:
                result.append(
                    resource
                )

        return result

    def resolve_pattern(
        self,
        pattern_id
    ):

        pattern = self.get_pattern(
            pattern_id
        )

        if pattern is None:
            return None

        resolved_stops = []

        for stop_id in pattern.get(
            "stops",
            []
        ):

            stop_info = (
                self.get_stop_info(
                    stop_id
                )
            )

            if stop_info is None:
                logger.debug(
                    "Pattern {} cannot be resolved because stop {} is not registered yet".format(
                        pattern_id,
                        stop_id
                    )
                )

                return None

            resolved_stops.append(
                stop_info
            )

        return {
            "pattern":
                dict(pattern),

            "stops":
                resolved_stops,
        }

    def validate_network(
        self
    ):

        errors = []

        for pattern_id, pattern in (
            self.patterns.items()
        ):

            for stop_id in pattern.get(
                "stops",
                []
            ):

                if self.get_stop(
                    stop_id
                ) is None:
                    errors.append(
                        {
                            "pattern_id":
                                pattern_id,

                            "stop":
                                stop_id,

                            "reason":
                                "stop_not_registered",
                        }
                    )

            next_pattern_id = (
                pattern.get(
                    "next_pattern_id"
                )
            )

            if (
                next_pattern_id is not None
                and next_pattern_id
                not in self.patterns
            ):
                errors.append(
                    {
                        "pattern_id":
                            pattern_id,

                        "next_pattern_id":
                            next_pattern_id,

                        "reason":
                            "next_pattern_not_found",
                    }
                )

        return errors


class PublicTransportNetworkQueryBehaviour(
    CyclicBehaviour
):

    async def on_start(
        self
    ):

        logger.debug(
            "Public transport network query behaviour started in {}".format(
                self.agent.name
            )
        )

    async def send_pattern(
        self,
        receiver,
        resolved_pattern
    ):

        msg = Message()

        msg.to = str(
            receiver
        )

        msg.set_metadata(
            "protocol",
            QUERY_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            INFORM_PERFORMATIVE
        )

        msg.body = json.dumps(
            {
                "request_type":
                    "public_transport_pattern",

                "pattern":
                    resolved_pattern[
                        "pattern"
                    ],

                "stops":
                    resolved_pattern[
                        "stops"
                    ],
            }
        )

        await self.send(
            msg
        )

    async def send_pattern_not_ready(
        self,
        receiver,
        pattern_id
    ):

        msg = Message()

        msg.to = str(
            receiver
        )

        msg.set_metadata(
            "protocol",
            QUERY_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            CANCEL_PERFORMATIVE
        )

        msg.body = json.dumps(
            {
                "request_type":
                    "public_transport_pattern",

                "pattern_id":
                    pattern_id,

                "reason":
                    "pattern_not_ready",
            }
        )

        await self.send(
            msg
        )

    async def run(
        self
    ):

        try:

            msg = await self.receive(
                timeout=5
            )

            if not msg:
                return

            performative = (
                msg.get_metadata(
                    "performative"
                )
            )

            if performative != (
                REQUEST_PERFORMATIVE
            ):
                return

            try:

                content = json.loads(
                    msg.body
                )

            except Exception:

                logger.warning(
                    "Invalid QUERY content received by public transport manager {}".format(
                        self.agent.name
                    )
                )

                return

            request_type = content.get(
                "request_type"
            )

            if request_type != (
                "public_transport_pattern"
            ):
                return

            pattern_id = content.get(
                "pattern_id"
            )

            if pattern_id is None:

                await self.send_pattern_not_ready(
                    msg.sender,
                    pattern_id
                )

                return

            resolved_pattern = (
                self.agent.resolve_pattern(
                    pattern_id
                )
            )

            if resolved_pattern is None:

                await self.send_pattern_not_ready(
                    msg.sender,
                    pattern_id
                )

                return

            await self.send_pattern(
                msg.sender,
                resolved_pattern
            )

        except CancelledError:

            logger.debug(
                "Cancelling public transport network query behaviour."
            )

        except Exception as exc:

            logger.error(
                "Exception in public transport network query behaviour of {}: {}".format(
                    self.agent.name,
                    exc
                )
            )
