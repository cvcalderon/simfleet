"""
General geospatial and numeric helpers used across SimFleet.

The module provides scenario geocoding, random routable-position generation,
geographic distance comparisons, speed conversion, and movement-related
exceptions.

Coordinates used by SimFleet helpers follow ``[latitude, longitude]`` order
unless an external service explicitly requires another representation.
"""

import json
import os
import random
import requests

from geopy.distance import geodesic as vincenty
from geopy.geocoders import Nominatim

def get_bbox_from_location(location_str, zoom):
    """
    Geocode a textual location and derive an approximate bounding box.

    Nominatim resolves the location centre. The configured zoom value is then
    converted into a simple angular bounding box around that point.

    Args:
        location_str (str): Human-readable place description.
        zoom (float): Zoom-like value controlling bounding-box dimensions.

    Returns:
        tuple: ``(central_point, bbox)`` where ``central_point`` is
        ``[lat, lon]`` and ``bbox`` is
        ``(min_lat, min_lon, max_lat, max_lon)``.

    Raises:
        Exception: If Nominatim cannot resolve the requested location.
    """

    geolocator = Nominatim(user_agent="zoom_bbox_simfleet")
    location = geolocator.geocode(location_str, addressdetails=True, timeout=10)

    if location is None:
        raise Exception ("Could not find coordinates for the entered location")
        return None

    lat, lon = location.latitude, location.longitude

    bbox_width = 360 / (2 ** zoom)
    bbox_height = bbox_width / 2

    min_lon = lon - bbox_width / 2
    max_lon = lon + bbox_width / 2
    min_lat = lat - bbox_height / 2
    max_lat = lat + bbox_height / 2

    bbox = (min_lat, min_lon, max_lat, max_lon)
    central_point = [lat, lon]

    return (central_point, bbox)


def random_position():
    """
    Return a random predefined position from the bundled taxi-stations data.

    One feature is selected from ``templates/data/taxi_stations.json`` and
    its GeoJSON coordinate order is converted to SimFleet ``[lat, lon]``
    order.

    Returns:
        list: Random ``[lat, lon]`` coordinate rounded to six decimal places.
    """
    base_dir_utils = os.path.dirname(__file__)
    base_dir = os.path.dirname(base_dir_utils)

    path = (
        base_dir
        + os.sep
        + "templates"
        + os.sep
        + "data"
        + os.sep
        + "taxi_stations.json"
    )
    with open(path) as f:
        stations = json.load(f)["features"]
        pos = random.choice(stations)
        coords = [pos["geometry"]["coordinates"][1], pos["geometry"]["coordinates"][0]]
        lat = float("{0:.6f}".format(coords[0]))
        lng = float("{0:.6f}".format(coords[1]))
        return [lat, lng]


def new_random_position(
    bbox,
    route_host,
    route_profile="driving",
):
    """
    Generate a random routable position inside a simulation bounding box.

    A random coordinate is sampled from a central subregion of the supplied
    bounding box. The OSRM ``nearest`` endpoint then snaps that coordinate to
    the nearest routable point for the configured route profile.

    Args:
        bbox (tuple): ``(min_lat, min_lon, max_lat, max_lon)``.
        route_host (str): Base URL of an OSRM-compatible routing server.
        route_profile (str): Routing profile used by the nearest lookup.

    Returns:
        list: Snapped route-network coordinate in ``[lat, lon]`` order.

    Raises:
        Exception: If the OSRM nearest request does not return HTTP 200.
    """

    min_lat, min_lon, max_lat, max_lon = bbox

    # Bias random sampling toward the central area of the bounding box.
    zoom = random.uniform(1, 3)     # Rango 1 (Sin zoom) - 5 (Zoom en el centro del bbox)
    zoom_factor = 1 / zoom
    random_lon = random.uniform(min_lon + (max_lon - min_lon) * (1 - zoom_factor) / 2, max_lon - (max_lon - min_lon) * (1 - zoom_factor) / 2)
    random_lat = random.uniform(min_lat + (max_lat - min_lat) * (1 - zoom_factor) / 2, max_lat - (max_lat - min_lat) * (1 - zoom_factor) / 2)

    route_base = route_host.rstrip("/")

    osrm_url = (
        f"{route_base}/nearest/v1/"
        f"{route_profile}/"
        f"{random_lon},{random_lat}"
    )

    # Realizar la solicitud a la API de OSRM
    response = requests.get(osrm_url)

    # Comprobar si la solicitud fue exitosa
    if response.status_code == 200:
        result = response.json()
        nearest_coordinates = result['waypoints'][0]['location']
        return [nearest_coordinates[1], nearest_coordinates[0]]
    else:
        raise Exception("OSRM request error")
        return None


def are_close(coord1, coord2, tolerance=10):
    """
    Return whether two coordinates are closer than a distance tolerance.

    Geographic distance is calculated with geopy's geodesic implementation.

    Args:
        coord1: First coordinate in ``[lat, lon]`` order.
        coord2: Second coordinate in ``[lat, lon]`` order.
        tolerance (float): Maximum exclusive distance in metres.

    Returns:
        bool: True when distance is strictly less than ``tolerance``.
    """
    return vincenty(coord1, coord2).meters < tolerance


def distance_in_meters(coord1, coord2):
    """
    Calculate geodesic distance between two SimFleet coordinates.

    Args:
        coord1: First ``[lat, lon]`` coordinate.
        coord2: Second ``[lat, lon]`` coordinate.

    Returns:
        float: Geodesic distance in metres.
    """
    return vincenty(coord1, coord2).meters


def kmh_to_ms(speed_in_kmh):
    """
    Convert kilometres per hour to metres per second.

    Args:
        speed_in_kmh (float): Speed in kilometres per hour.

    Returns:
        float: Equivalent speed in metres per second.
    """
    meters_per_second = speed_in_kmh * 1000 / 3600
    return meters_per_second


class PathRequestException(Exception):
    """
    This exception is raised when a path could not be computed.
    """

    pass


class AlreadyInDestination(Exception):
    """
    Raised when route-based movement is requested to the current position.
    """

    pass

