import asyncio
import json
import socket
import time
import uuid

import aiohttp
from loguru import logger
from spade.behaviour import OneShotBehaviour
from spade.message import Message
from spade.template import Template

from simfleet.utils.helpers import distance_in_meters, kmh_to_ms


class RequestRouteBehaviour(OneShotBehaviour):
    """
    Execute one asynchronous request to the configured routing server.

    The behaviour delegates HTTP route resolution to
    ``request_route_to_server()`` and exposes the result through its SPADE
    exit code.

    Successful execution returns path geometry, route distance, and routing
    duration. Routing failures are converted into an error exit code rather
    than propagated to the requesting MovableMixin.
    """

    def __init__(self, msg: Message, origin: list, destination: list, route_host: str, route_profile: str):
        """
        Initialize one routing request.

        Args:
            msg (Message): Message object associated with the request.
            origin (list): Origin coordinates in SimFleet ``[lat, lon]`` order.
            destination (list): Destination coordinates in SimFleet
                ``[lat, lon]`` order.
            route_host (str): Base URL of the routing server.
            route_profile (str): Routing profile, such as ``"driving"``.
        """
        self.origin = origin
        self.destination = destination
        self._msg = msg
        self.route_host = route_host
        self.route_profile = route_profile
        self.result = {"path": None, "distance": None, "duration": None}
        super().__init__()

    async def run(self):
        """
        Resolve the configured route and terminate with a structured exit code.

        A successful request stores ``path``, ``distance``, ``duration``, and
        ``type="success"`` in the behaviour exit code.

        Missing route data or an exception terminates the behaviour with an error
        result. Exceptions are logged and are not re-raised.
        """
        try:
            response_time = time.time()
            path, distance, duration = await request_route_to_server(
                self.origin, self.destination, self.route_host, self.route_profile
            )
            response_time = time.time() - response_time
            if path is None:
                logger.error(
                    "There was an unknown error requesting the route. Response time={}".format(
                        response_time
                    )
                )
                self.exit_code = {"type": "error"}
                self.kill()
                return
            logger.debug("Got route in response time={}".format(response_time))
            reply_content = {
                "path": path,
                "distance": distance,
                "duration": duration,
                "type": "success",
            }
            self.kill(json.loads(json.dumps(reply_content)))

        except Exception as e:
            response_time = time.time() - response_time
            logger.error(
                "Exception requesting route, response time={}, error: {} ".format(
                    response_time, e
                )
            )
            self.kill({"type": "error", "error": str(e)})


async def request_path(agent, origin, destination, route_host, route_profile="driving"):
    """
    Resolve a route on behalf of a SPADE agent.

    The helper creates a RequestRouteBehaviour, attaches it to the requesting
    agent, and waits asynchronously until that behaviour terminates.

    When origin and destination are identical, routing-server access is
    skipped and a zero-distance, zero-duration route is returned directly.

    Args:
        agent: SPADE agent on which the one-shot routing behaviour is started.
        origin (list): Origin coordinates in ``[lat, lon]`` order.
        destination (list): Destination coordinates in ``[lat, lon]`` order.
        route_host (str): Base URL of the routing server.
        route_profile (str): Routing profile.

    Returns:
        tuple: ``(path, distance_m, duration_s)`` on success, or
        ``(None, None, None)`` when route resolution fails.
    """
    if origin[0] == destination[0] and origin[1] == destination[1]:
        return [[origin[1], origin[0]]], 0, 0

    msg = Message()
    msg.thread = str(uuid.uuid4()).replace("-", "")
    template = Template()
    template.thread = msg.thread
    behav = RequestRouteBehaviour(msg, origin, destination, route_host, route_profile)
    agent.add_behaviour(behav, template)

    while not behav.is_killed():
        await asyncio.sleep(0.01)

    exit_code = behav.exit_code
    if (
        not isinstance(exit_code, dict)
        or exit_code.get("type") == "error"
    ):
        return None, None, None
    else:
        return (
            exit_code["path"],
            exit_code["distance"],
            exit_code["duration"],
        )


def unused_port(hostname):
    """
    Ask the operating system for an available local TCP port.

    A temporary socket is bound to port zero, allowing the operating system
    to select a currently unused port. The socket is then closed before the
    selected port number is returned.

    Args:
        hostname (str): Local interface or hostname to bind.

    Returns:
        int: Port number selected by the operating system.

    Note:
        The port is not reserved after this function returns.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((hostname, 0))
    port = s.getsockname()[1]
    s.close()
    return port


def chunk_path(path, speed_in_kmh):
    """
    Expand route geometry into movement points based on configured speed.

    Each original route segment is subdivided when its geographic length is
    greater than the distance the agent travels in one second at
    ``speed_in_kmh``.

    The returned sequence therefore provides approximately one-second
    geographic movement increments while preserving the final route
    destination.

    Args:
        path (list): Route coordinates in SimFleet ``[lat, lon]`` order.
        speed_in_kmh (float): Physical movement speed in kilometres per hour.

    Returns:
        list: Expanded route geometry used by MovableMixin.
    """
    meters_per_second = kmh_to_ms(speed_in_kmh)
    length = len(path)
    chunked_lat_lngs = []

    for i in range(1, length):
        _cur = path[i - 1]
        _next = path[i]
        if _cur == _next:
            continue
        distance = distance_in_meters(_cur, _next)
        factor = meters_per_second / distance if distance else 0
        diff_lat = factor * (_next[0] - _cur[0])
        diff_lng = factor * (_next[1] - _cur[1])

        if distance > meters_per_second:
            while distance > meters_per_second:
                _cur = [_cur[0] + diff_lat, _cur[1] + diff_lng]
                distance = distance_in_meters(_cur, _next)
                chunked_lat_lngs.append(_cur)
        else:
            chunked_lat_lngs.append(_cur)

    chunked_lat_lngs.append(path[length - 1])

    return chunked_lat_lngs


def avg(array):
    """
    Calculate the arithmetic mean of truthy values in an iterable.

    Values considered false by Python, including None and numeric zero, are
    excluded before averaging.

    Args:
        array (iterable): Values to aggregate.

    Returns:
        float: Mean of retained values, or 0.0 when none remain.

    Note:
        This compatibility helper intentionally reflects its current
        ``filter(None, ...)`` behaviour. Its treatment of numeric zero requires
        review before use in statistical calculations.
    """
    array_wo_nones = list(filter(None, array))
    return (
        (sum(array_wo_nones, 0.0) / len(array_wo_nones))
        if len(array_wo_nones) > 0
        else 0.0
    )


async def request_route_to_server(
    origin, destination, route_host="http://router.project-osrm.org/", route_profile="driving"
):
    """
    Query an OSRM-compatible routing server for one route.

    SimFleet coordinates are supplied in ``[lat, lon]`` order and converted
    to OSRM's ``lon,lat`` URL representation. Returned GeoJSON coordinates are
    converted back to SimFleet order.

    The first route returned by the server is used. Its full geometry,
    distance in metres, and estimated duration in seconds are returned.

    If the routing response does not end exactly at the requested SimFleet
    destination, that destination is appended to the path.

    Args:
        origin (list): Origin coordinates in ``[lat, lon]`` order.
        destination (list): Destination coordinates in ``[lat, lon]`` order.
        route_host (str): Base URL of an OSRM-compatible routing service.
        route_profile (str): OSRM routing profile.

    Returns:
        tuple: ``(path, distance_m, duration_s)`` on success, or
        ``(None, None, None)`` when any request or response-processing error
        occurs.
    """
    try:

        route_base = route_host.rstrip("/")

        src1 = origin[1]
        src2 = origin[0]
        dest1 = destination[1]
        dest2 = destination[0]

        url = (
            f"{route_base}/route/v1/{route_profile}/"
            f"{src1},{src2};{dest1},{dest2}"
            "?geometries=geojson&overview=full"
        )

        url = url.format(src1=src1, src2=src2, dest1=dest1, dest2=dest2)

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                result = await response.json()

        path = result["routes"][0]["geometry"]["coordinates"]
        path = [[point[1], point[0]] for point in path]
        duration = result["routes"][0]["duration"]
        distance = result["routes"][0]["distance"]
        if path[-1] != destination:
            path.append(destination)
        return path, distance, duration
    except Exception as e:
        return None, None, None
