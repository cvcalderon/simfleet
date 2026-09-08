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
    """
    Transport model for scheduled public transport services.

    A PublicTransportAgent operates over directional Patterns resolved from
    its FleetManager. A Pattern defines the ordered stop sequence, route
    metadata, movement mode, optional travel times, and the next directional
    Pattern to load at a terminal stop.

    The model owns the structural and runtime context required by the
    PublicTransport FSM:

    - configured Pattern and initial stop;
    - resolved route, mode, route type, and movement mode;
    - ordered and resolved stop information;
    - current and next stop;
    - passenger capacity;
    - directional Pattern changes;
    - public transport Presence data.

    Service decisions, boarding, alighting, movement progression, and FSM
    transitions remain responsibilities of the configured strategy.
    """

    def __init__(
        self,
        agentjid,
        password,
        **kwargs
    ):
        """
        Initialize public transport configuration and unresolved runtime state.

        Args:
            agentjid (str): XMPP JID used by the vehicle.
            password (str): XMPP authentication password.
            **kwargs: Public transport configuration. ``pattern_id`` and
                ``start_stop`` are required; ``stop_time`` defaults to 1.

        Raises:
            ValueError: If the Pattern identifier or initial stop is missing,
                or when ``stop_time`` is negative.
        """

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
        """
        Configure the passenger capacity of the vehicle.

        Initial available capacity is reset to the configured total capacity.

        Args:
            capacity: Maximum number of simultaneous passengers.

        Raises:
            ValueError: If capacity is not greater than zero.
        """

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
        """
        Return whether the vehicle dependencies required for startup are ready.

        In addition to the common TransportAgent dependencies, a public
        transport vehicle requires its directional Pattern to have been
        resolved before it may be considered fully ready.

        Returns:
            bool: True when both common dependencies and Pattern resolution
            are complete.
        """

        if not super().dependencies_ready():
            return False

        if self.pattern is None:
            return False

        return True

    def get_registration_content(
        self
    ):
        """
        Extend the common transport registration payload.

        Public transport resources advertise their resource type, configured
        Pattern identifier, and passenger capacity to the FleetManager.

        Returns:
            dict: Fleet registration payload for this vehicle.
        """

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
        """
        Validate and install a resolved directional Pattern.

        Pattern metadata and stop definitions are fully validated before any
        Pattern-dependent operational state is changed. This prevents a
        partially invalid response from leaving the vehicle in an
        inconsistent route state.

        The resolved Pattern defines:

        - route and transport mode;
        - route type (``end-to-end`` or ``circular``);
        - movement mode (``route`` or ``teleport``);
        - ordered stop identifiers;
        - optional travel times;
        - optional next directional Pattern.

        Once validation succeeds, the vehicle is positioned at
        ``start_stop``.

        Args:
            pattern (dict): Pattern definition received from the FleetManager.
            stops (list[dict]): Resolved stop definitions.

        Raises:
            ValueError: If the Pattern does not match this vehicle, contains
                unsupported metadata, has no stops, or cannot resolve the
                configured initial stop.
        """
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
        """
        Return the next stop in the currently resolved Pattern.

        End-to-end Patterns return None after their terminal stop. Circular
        Patterns wrap to the first stop and increment ``rounds`` when the
        vehicle crosses the Pattern boundary.

        Returns:
            str | None: Identifier of the next stop.
        """

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
        """
        Return whether the current directional Pattern reached its terminal.

        Circular Patterns never finish through this condition. An end-to-end
        Pattern is finished when ``current_stop`` is its final ordered stop.

        Returns:
            bool: True when an end-to-end Pattern has finished.
        """

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
        """
        Return the directional Pattern configured after the current one.

        Returns:
            str | None: Next Pattern identifier when one is configured.
        """

        return self.next_pattern_id

    def can_reach_stop(
        self,
        destination_stop
    ):
        """
        Return whether a destination is reachable in the current direction.

        A destination must belong to the active Pattern and differ from the
        current stop. Circular Patterns may reach any other stop. For
        end-to-end Patterns the destination must appear strictly after the
        current stop in the ordered stop sequence.

        Args:
            destination_stop (str): Requested destination stop identifier.

        Returns:
            bool: True when the destination can be served without reversing
            the current directional Pattern.
        """

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
        """
        Return the resolved definition of a Pattern stop.

        Args:
            stop_id (str): Stop identifier.

        Returns:
            dict | None: Resolved stop definition.
        """

        return self.stops.get(
            stop_id
        )

    def get_stop_position(
        self,
        stop_id
    ):
        """
        Return the resolved physical position of a stop.

        Args:
            stop_id (str): Stop identifier.

        Returns:
            Any: Stop position, or None when the stop is unknown.
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
        Return the XMPP JID associated with a resolved stop.

        Args:
            stop_id (str): Stop identifier.

        Returns:
            str | None: Stop JID when available.
        """

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
        """
        Return the configured travel time for the next teleport segment.

        Teleport movement follows the same ordered Pattern topology as routed
        movement. The requested destination must therefore be the immediate
        next stop. Circular Patterns also support the terminal-to-first-stop
        segment.

        Args:
            destination_stop (str): Immediate destination stop identifier.

        Returns:
            Any: Configured travel time for the segment, or None when the
            active movement mode is not ``teleport``.

        Raises:
            ValueError: If teleport travel times are unavailable, the current
                stop is invalid, or the requested destination is not the next
                valid Pattern stop.
        """

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
        """
        Move the vehicle to the next public transport stop.

        ``route`` movement delegates to the common route-based movement
        system. ``teleport`` movement waits for the Pattern segment travel
        time and then updates the physical position directly.

        In both modes ``next_stop`` is set before movement starts.

        Args:
            stop_id (str): Destination stop identifier.

        Raises:
            ValueError: If the stop is unknown or the configured movement
                mode is unsupported.
        """

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
        """
        Build the compact Presence payload advertised by the vehicle.

        The payload contains:

        ``p``
            Current physical position.
        ``m``
            Public transport mode.
        ``r``
            Route identifier.
        ``pt``
            Active directional Pattern identifier.
        ``s``
            Current stop.
        ``n``
            Next stop.
        ``f``
            Currently available passenger capacity.
        ``c``
            Total passenger capacity.

        Returns:
            dict: Public transport Presence payload.
        """

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
        """
        Publish the current public transport state through XMPP Presence.

        The application payload is JSON-encoded and delegated to the generic
        SimfleetAgent Presence infrastructure.
        """

        self.set_agent_presence(
            status=json.dumps(
                self.get_public_transport_presence()
            )
        )

    def start_pattern_resolution(
        self
    ):
        """
        Start asynchronous resolution of the configured directional Pattern.

        Pattern resolution uses a dedicated QUERY_PROTOCOL behaviour and is
        intentionally independent from the operational PublicTransport FSM.
        """

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
        """
        Prepare the vehicle to load the next directional Pattern.

        The terminal stop of the current Pattern becomes ``start_stop`` for
        the next Pattern. Pattern-dependent structural state is cleared and
        resolution of ``next_pattern_id`` is started.

        Passenger capacity and onboard-customer state are intentionally
        preserved across the Pattern change.

        ``pattern_changed`` is set so the operational FSM can process the
        shared terminal stop before departing in the new direction.

        Returns:
            bool: True when transition to a next Pattern was started, or
            False when no next Pattern is configured.
        """

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
        Start the operational PublicTransport FSM once.

        The operational strategy receives only REQUEST_PROTOCOL messages.
        QUERY_PROTOCOL is intentionally excluded because directional Pattern
        resolution is handled independently by
        PublicTransportPatternBehaviour.

        ``running_strategy`` prevents duplicate FSM instances.
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
        """
        Initialize the vehicle and start directional Pattern resolution.

        Common TransportAgent setup is completed first. Pattern resolution is
        then started asynchronously.

        The local ``ready`` flag is enabled here, while
        ``dependencies_ready()`` continues to prevent global simulator
        readiness until the Pattern has actually been resolved.
        """

        await super().setup()

        self.start_pattern_resolution()

        self.ready = True


class PublicTransportPatternBehaviour(
    CyclicBehaviour
):
    """
    Resolve directional Pattern data from the vehicle FleetManager.

    The behaviour communicates through QUERY_PROTOCOL independently from the
    operational PublicTransport FSM. It retries while Pattern data is not yet
    available and terminates itself once a valid Pattern has been installed.
    """

    async def request_pattern(
        self
    ):
        """
        Request the configured Pattern definition from the FleetManager.

        No request is sent when the vehicle has no registration FleetManager.
        """

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
        """
        Execute one Pattern-resolution attempt.

        If the Pattern is already resolved, the behaviour terminates.
        Otherwise it requests the Pattern and waits for a FleetManager
        response.

        INFORM responses containing valid Pattern and stop data are installed
        through ``set_pattern_data()`` and terminate the behaviour.
        CANCEL responses indicate that the Pattern is not ready yet and leave
        the cyclic behaviour active for a later retry.

        Cancellation and unexpected errors are logged without modifying the
        operational PublicTransport FSM.
        """

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
