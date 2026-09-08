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
    """
    FleetManager model for a scheduled public transport network.

    The manager owns the static directional Pattern catalogue and combines it
    with dynamically registered stop and vehicle resources.

    A Pattern describes one directional service topology, including route,
    transport mode, route type, movement mode, ordered stops, optional travel
    times, and an optional next directional Pattern.

    The model provides:

    - Pattern validation and lookup;
    - registered stop and vehicle discovery;
    - directional reachability and Pattern paths;
    - transfer-stop discovery;
    - runtime vehicle lookup using Presence;
    - Pattern resolution for PublicTransportAgent;
    - structural network validation.

    QUERY_PROTOCOL is handled independently from the normal fleet-management
    REQUEST_PROTOCOL strategy.
    """

    def __init__(self, agentjid, password, **kwargs):
        """
        Initialize the generic FleetManager and load configured Patterns.

        Args:
            agentjid (str): XMPP JID used by the manager.
            password (str): XMPP authentication password.
            **kwargs: Optional configuration containing a ``patterns`` list.
        """
        super().__init__(agentjid, password)

        self.patterns = {}

        patterns = kwargs.get("patterns", [])

        for pattern in patterns:
            self.add_pattern(pattern)

    async def setup(
        self
    ):
        """
        Initialize generic fleet management and public transport queries.

        FleetManagerAgent setup first installs REGISTER_PROTOCOL handling. Local
        readiness is temporarily cleared while
        PublicTransportNetworkQueryBehaviour is installed for QUERY_PROTOCOL and
        then restored.
        """
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
        """
        Validate and register one directional Pattern.

        The supplied mapping is copied before storage.

        Args:
            pattern (dict): Directional Pattern definition.

        Returns:
            bool: True when the Pattern was valid and stored.
        """

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
        """
        Validate the structural definition of a directional Pattern.

        Required fields are:

        - ``pattern_id``;
        - ``route_id``;
        - ``mode``;
        - ``route_type``;
        - ``movement_mode``;
        - ``stops``.

        Supported route types are ``end-to-end`` and ``circular``. Supported
        movement modes are ``route`` and ``teleport``.

        Teleport Patterns must also provide non-negative numeric travel times
        matching the number of directional stop segments.

        Args:
            pattern (dict): Pattern definition to validate.

        Returns:
            bool: True when the Pattern is structurally valid.
        """
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
        """
        Return one configured directional Pattern.

        Args:
            pattern_id (str): Pattern identifier.

        Returns:
            dict | None: Stored Pattern definition.
        """
        return self.patterns.get(pattern_id)

    def get_patterns(self):
        """
        Return the complete directional Pattern catalogue.

        Returns:
            dict: Patterns indexed by Pattern identifier.
        """
        return self.patterns

    def get_patterns_for_stop(self, stop_name):
        """
        Return Patterns whose ordered topology contains a stop.

        Args:
            stop_name (str): Stop identifier.

        Returns:
            list[str]: Matching Pattern identifiers.
        """

        result = []

        for pattern_id, pattern in self.patterns.items():

            if stop_name in pattern.get("stops", []):
                result.append(pattern_id)

        return result

    def get_patterns_for_route(self, route_id):
        """
        Return directional Patterns belonging to a route.

        A route may therefore map to more than one Pattern, for example opposite
        directions of the same public transport service.

        Args:
            route_id (str): Route identifier.

        Returns:
            list[str]: Matching Pattern identifiers.
        """
        result = []

        for pattern_id, pattern in self.patterns.items():

            if pattern.get("route_id") == route_id:
                result.append(pattern_id)

        return result


    def _bare_jid(self, jid):
        """
        Normalize an XMPP identifier by removing its resource component.

        Args:
            jid: XMPP identifier.

        Returns:
            str | None: Bare JID representation.
        """
        if jid is None:
            return None

        return str(jid).split("/")[0]

    def get_stop(
        self,
        stop_id
    ):
        """
        Return a registered resource only when it represents a public transport
        stop.

        Args:
            stop_id (str): Stop resource identifier.

        Returns:
            dict | None: Registered stop resource.
        """
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
        """
        Return all registered resources whose resource type is ``stop``.

        Returns:
            dict: Stop resources indexed by resource name.
        """
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
        """
        Return the registered physical position of a stop.

        Args:
            stop_id (str): Stop identifier.

        Returns:
            Any: Stop position, or None when the stop is not registered.
        """
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
        """
        Return the XMPP JID registered for a stop.

        Args:
            stop_id (str): Stop identifier.

        Returns:
            str | None: Stop JID.
        """
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
        """
        Build the compact stop representation sent during Pattern resolution.

        Args:
            stop_id (str): Stop identifier.

        Returns:
            dict | None: Mapping containing stop ID, JID, and position.
        """
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
        """
        Return stops reachable after an origin in one Pattern direction.

        End-to-end Patterns return only stops after the origin. Circular Patterns
        wrap around the terminal boundary but never return the origin itself.

        Args:
            pattern_id (str): Directional Pattern identifier.
            origin_stop (str): Origin stop identifier.

        Returns:
            list[str]: Reachable stop identifiers in travel order.
        """
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
        """
        Return the ordered Pattern segment between two stops.

        For end-to-end Patterns the destination must occur after the origin.
        Circular Patterns may wrap through the Pattern boundary.

        Args:
            pattern_id (str): Directional Pattern identifier.
            origin_stop (str): Segment origin.
            destination_stop (str): Segment destination.

        Returns:
            list[str]: Ordered stop path including origin and destination, or an
            empty list when the requested movement is invalid.
        """
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
        """
        Return registered stops within a geographic transfer radius.

        Transfer distance is computed directly between registered stop
        coordinates using ``distance_in_meters``; it is not a routed pedestrian
        network distance.

        Results are sorted by increasing distance.

        Args:
            stop_id (str): Origin stop identifier.
            max_distance (float): Maximum transfer distance in meters.

        Returns:
            list[dict]: Candidate transfer stops and their distances.
        """
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
        """
        Return all registered resources whose resource type is ``vehicle``.

        Returns:
            dict: Public transport vehicles indexed by resource name.
        """
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
        Return vehicles currently operating a directional Pattern.

        Live XMPP Presence is the runtime source of truth. When live Presence is
        unavailable, the message-based Presence mirror is consulted.

        Only before any valid Presence information exists does the manager fall
        back to the initial ``pattern_id`` advertised during REGISTER.

        Args:
            pattern_id (str): Directional Pattern identifier.

        Returns:
            list[dict]: Matching registered vehicle resources.
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
        """
        Resolve a configured Pattern against currently registered stops.

        Pattern resolution succeeds only when every stop referenced by the
        Pattern is already registered with the FleetManager.

        Args:
            pattern_id (str): Pattern identifier.

        Returns:
            dict | None: Copy of the Pattern together with resolved stop
            information, or None while the Pattern cannot yet be resolved.
        """
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
        """
        Validate references across the configured public transport network.

        Validation reports:

        - Pattern stops that are not registered resources;
        - ``next_pattern_id`` references that do not exist in the Pattern
          catalogue.

        Returns:
            list[dict]: Structural network errors. An empty list represents a
            currently valid network.
        """
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
    """
    Serve directional Pattern-resolution requests over QUERY_PROTOCOL.

    PublicTransportAgent requests its configured Pattern by identifier.
    When both the Pattern and every referenced stop are available, the
    manager responds with INFORM_PERFORMATIVE containing the resolved Pattern.

    If resolution is not yet possible, CANCEL_PERFORMATIVE with
    ``reason="pattern_not_ready"`` is returned so the requesting cyclic
    behaviour may retry later.
    """
    async def on_start(
        self
    ):
        """Log the start of public transport network query handling."""
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
        """
        Send a fully resolved Pattern to a requesting agent.

        Args:
            receiver: Requesting agent JID.
            resolved_pattern (dict): Pattern and resolved stop information.
        """
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
        """
        Inform a requester that a Pattern cannot currently be resolved.

        Args:
            receiver: Requesting agent JID.
            pattern_id: Pattern identifier that is not ready.
        """
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
        """
        Process one public transport network query.

        Only REQUEST_PERFORMATIVE messages with
        ``request_type="public_transport_pattern"`` are handled.

        A successfully resolved Pattern is returned with INFORM_PERFORMATIVE.
        Missing identifiers, unknown Patterns, or Patterns whose stops are not
        yet registered produce a ``pattern_not_ready`` cancellation.

        Malformed or unrelated queries are ignored after logging when
        appropriate.
        """
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
