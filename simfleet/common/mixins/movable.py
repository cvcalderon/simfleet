from asyncio.log import logger
from simfleet.utils.helpers import AlreadyInDestination, PathRequestException, distance_in_meters, kmh_to_ms
from spade.behaviour import PeriodicBehaviour
from simfleet.utils.routing import chunk_path, request_path

ONESECOND_IN_MS = 1000


class MovableMixin:
    """
        MovableMixin is a mixin class that provides functionality for agents that need to move along a defined path
        to a specific destination. It manages the movement process, path calculation, and speed handling.

        Attributes:
            path (list): A list of coordinates that represents the path the vehicle should follow.
            chunked_path (list): A list of smaller steps or 'chunks' of the path based on the speed of the vehicle.
            animation_speed (int): The time in milliseconds between steps in the animation or movement process.
            speed_in_kmh (float): The current speed of the vehicle in kilometers per hour.
            dest (list): The destination coordinates (longitude, latitude) of the vehicle.
            last_route_distance: Distance in meters of the last successfully planned route.
            last_osrm_duration: Duration in seconds estimated by OSRM for the last route.
            last_speed_based_duration: Theoretical duration in seconds calculated using the agent configured speed.
            total_route_distance: Accumulated distance of successfully planned routes.
            total_osrm_duration: Accumulated OSRM estimated duration.
            total_speed_based_duration: Accumulated duration calculated from agent speed.
            route_count: Number of successfully planned routes.
        """

    def __init__(self):
        """
            Initializes the MovableMixin with default values for path, speed, and destination.
        """
        self.set("path", None)
        self.chunked_path = None
        self.animation_speed = ONESECOND_IN_MS
        self.set("speed_in_kmh", None)
        self.dest = None

        # self.distances = []
        # self.durations = []

        self.last_route_distance = 0.0
        self.last_osrm_duration = 0.0
        self.last_speed_based_duration = 0.0

        self.total_route_distance = 0.0
        self.total_osrm_duration = 0.0
        self.total_speed_based_duration = 0.0

        self.route_count = 0


    async def move_to(self, dest):
        """
            Moves the transport agent to a new destination by requesting a path from its current position
            and chunking the path based on the transport's speed.

            Args:
                dest (list): The destination coordinates (longitude, latitude).

            Raises:
                AlreadyInDestination: If the transport is already at the destination coordinates.
                PathRequestException: If there is an error in obtaining the path from the current position to the destination.
        """
        if self.get("current_pos") == dest:
            raise AlreadyInDestination
        counter = 5
        path = None
        #distance, duration = 0, 0
        distance = 0.0
        osrm_duration = 0.0
        while counter > 0 and path is None:
            logger.debug(
                "Requesting path from {} to {}".format(self.get("current_pos"), dest)
            )
           # path, distance, duration = await self.request_path(
           #     self.get("current_pos"), dest
           # )
            path, distance, osrm_duration = await self.request_path(
                self.get("current_pos"),
                dest,
            )

            counter -= 1
        if path is None:
            raise PathRequestException("Error requesting route.")

        self.set("path", path)
        try:
            self.chunked_path = chunk_path(path, self.get("speed_in_kmh"))
        except Exception as e:
            logger.error("Exception chunking path {}: {}".format(path, e))
            raise PathRequestException

        speed_in_ms = kmh_to_ms(self.get("speed_in_kmh"))
        speed_based_duration = (distance / speed_in_ms)

        self.dest = dest
        # self.distances.append(distance)
        # self.durations.append(duration)

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
        Requests a path between two points (origin and destination) using the route server.

        Args:
            origin (list): the coordinates of the origin of the requested path
            destination (list): the coordinates of the end of the requested path

        Returns:
            list, float, float: A list of points that represent the path from origin to destination, the distance and
            the estimated duration

        Examples:
            >>> path, distance, duration = await self.request_path(origin=[0,0], destination=[1,1])
            >>> print(path)
            [[0,0], [0,1], [1,1]]
            >>> print(distance)
            2.0
            >>> print(duration)
            3.24
        """
        return await request_path(self, origin, destination, self.route_host, self.route_profile)


    async def step(self):
        """
        Advances one step in the simulation
        """
        if self.chunked_path:
            _next = self.chunked_path.pop(0)
            distance = distance_in_meters(self.get_position(), _next)
            self.animation_speed = (
                distance / kmh_to_ms(self.get("speed_in_kmh")) * ONESECOND_IN_MS
            )
            await self.set_position(_next)


    def is_in_destination(self):
        """
        Checks if the transport has arrived to its destination.

        Returns:
            bool: whether the transport is at its destination or not
        """
        return self.dest == self.get_position()


    def set_speed(self, speed_in_kmh):
        """
        Sets the speed of the transport.

        Args:
            speed_in_kmh (float): the speed of the transport in km per hour
        """
        self.set("speed_in_kmh", speed_in_kmh)


class MovingBehaviour(PeriodicBehaviour):
    """
        This is the internal behaviour that manages the movement of the transport.
        It is triggered when the transport has a new destination and the periodic tick
        is recomputed at every step to show a fine animation.
        This moving behaviour includes to update the transport coordinates as it
        moves along the path at the specified speed.
    """

    async def run(self):
        await self.agent.step()
        self.period = self.agent.animation_speed / ONESECOND_IN_MS
        if self.agent.is_in_destination():
            self.kill()
            self.agent.set("path", None)
            self.agent.chunked_path = None
