from asyncio.log import logger
from collections import deque
from simfleet.utils.helpers import AlreadyInDestination, PathRequestException, distance_in_meters, kmh_to_ms
from spade.behaviour import PeriodicBehaviour
from simfleet.utils.routing import chunk_path, request_path

ONESECOND_IN_MS = 1000


class MovableMixin:
    """
    Add route-based movement capabilities to a geolocated agent.

    The mixin requests a route between the agent's current position and a
    destination, converts that route into movement steps according to the
    configured speed, and advances through those steps with a periodic SPADE
    behaviour.

    Route execution keeps two complementary duration estimates:

    ``last_osrm_duration``
        Duration estimated by the external routing service.

    ``last_speed_based_duration``
        Theoretical duration computed from route distance and the agent's
        configured speed.

    Equivalent accumulated metrics are stored across all successfully planned
    routes together with ``route_count``.

    The host agent is expected to provide geolocation, routing configuration,
    SPADE behaviour management, and ``set_position()``.
    """

    def __init__(self):
        """
        Initialize movement, route, speed, and accumulated route metrics.

        No route is active after initialization.
        """
        self.set("path", None)
        self.chunked_path = None
        self.animation_speed = ONESECOND_IN_MS
        self.set("speed_in_kmh", None)
        self.dest = None

        self.last_route_distance = 0.0
        self.last_osrm_duration = 0.0
        self.last_speed_based_duration = 0.0

        self.total_route_distance = 0.0
        self.total_osrm_duration = 0.0
        self.total_speed_based_duration = 0.0

        self.route_count = 0


    async def move_to(self, dest):
        """
        Plan and start route-based movement toward a destination.

        Route calculation is attempted up to five times while no path is
        returned. Once a path is obtained, it is divided into movement steps
        according to the configured agent speed and stored as a deque.

        The method updates both last-route and accumulated route metrics before
        installing a MovingBehaviour that performs the physical movement.

        Args:
            dest (list): Destination coordinates.

        Returns:
            tuple[float, float, float]:
                Route distance in meters, routing-service duration in seconds,
                and theoretical duration derived from configured agent speed.

        Raises:
            AlreadyInDestination: If the current position already equals ``dest``.
            PathRequestException: If no route can be obtained or the returned path
                cannot be converted into movement steps.
        """
        if self.get("current_pos") == dest:
            raise AlreadyInDestination
        counter = 5
        path = None
        distance = 0.0
        osrm_duration = 0.0
        while counter > 0 and path is None:
            logger.debug(
                "Requesting path from {} to {}".format(self.get("current_pos"), dest)
            )
            path, distance, osrm_duration = await self.request_path(
                self.get("current_pos"),
                dest,
            )

            counter -= 1
        if path is None:
            raise PathRequestException("Error requesting route.")

        self.set("path", path)
        try:

           self.chunked_path = deque( chunk_path(path, self.get("speed_in_kmh")) )

        except Exception as e:
            logger.error("Exception chunking path {}: {}".format(path, e))
            raise PathRequestException

        speed_in_ms = kmh_to_ms(self.get("speed_in_kmh"))
        speed_based_duration = (distance / speed_in_ms)

        self.dest = dest

        self.last_route_distance = distance
        self.last_osrm_duration = osrm_duration
        self.last_speed_based_duration = speed_based_duration

        self.total_route_distance += distance
        self.total_osrm_duration += osrm_duration
        self.total_speed_based_duration += speed_based_duration

        self.route_count += 1

        behav = MovingBehaviour(period=1)
        self.add_behaviour(behav)

        return (
            distance,
            osrm_duration,
            speed_based_duration,
        )


    async def request_path(self, origin, destination):
        """
        Request a route from the configured routing service.

        The request delegates to ``simfleet.utils.routing.request_path`` using the
        agent's ``route_host`` and ``route_profile``.

        Args:
            origin (list): Origin coordinates.
            destination (list): Destination coordinates.

        Returns:
            tuple: Route geometry, route distance, and routing-service duration.
        """
        return await request_path(self, origin, destination, self.route_host, self.route_profile)


    async def step(self):
        """
        Advance one physical movement step along the active chunked route.

        The next coordinate is removed from the left side of ``chunked_path``.
        The distance to that coordinate is used to recompute
        ``animation_speed`` so the periodic movement interval reflects the
        configured agent speed.

        When no chunked route remains, the method performs no movement.
        """
        if self.chunked_path:
            _next = self.chunked_path.popleft()
            distance = distance_in_meters(self.get_position(), _next)
            self.animation_speed = (
                distance / kmh_to_ms(self.get("speed_in_kmh")) * ONESECOND_IN_MS
            )
            await self.set_position(_next)


    def is_in_destination(self):
        """
        Return whether the current physical position equals the active destination.

        Returns:
            bool: True when the destination has been reached.
        """
        return self.dest == self.get_position()


    def set_speed(self, speed_in_kmh):
        """
        Configure the movement speed used for route chunking and step timing.

        Args:
            speed_in_kmh (float): Movement speed in kilometres per hour.
        """
        self.set("speed_in_kmh", speed_in_kmh)


class MovingBehaviour(PeriodicBehaviour):
    """
    Periodically advance an agent through an active MovableMixin route.

    After each movement step, the behaviour updates its own period from the
    agent's current ``animation_speed``. This allows variable geographic step
    distances while preserving the configured physical movement speed.

    Once the destination is reached, the behaviour terminates and clears the
    active path and chunked-route state.
    """

    async def run(self):
        """
        Execute one movement tick and stop when the destination is reached.
        """
        await self.agent.step()
        self.period = self.agent.animation_speed / ONESECOND_IN_MS
        if self.agent.is_in_destination():
            self.kill()
            self.agent.set("path", None)
            self.agent.chunked_path = None
