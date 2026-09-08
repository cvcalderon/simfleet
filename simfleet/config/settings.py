"""
Scenario configuration loading and default class resolution for SimFleet.

SimfleetConfig combines JSON scenario values with simulator defaults and
exposes the resulting configuration through mapping-style and attribute-style
access.

This module also resolves configured default strategy and metrics import paths
into Python classes before agents are created.
"""

import json
from loguru import logger

from simfleet.utils.reflection import load_class
from simfleet.utils.helpers import get_bbox_from_location

def hide_passwords(item, key=None):
    """
    Recursively mask values whose dictionary key contains ``"password"``.

    Dictionaries and lists are copied recursively. Matching scalar values are
    replaced by a string of asterisks with the same length.

    Args:
        item: Configuration value to sanitize.
        key (str | None): Dictionary key associated with the current value.

    Returns:
        Any: Sanitized copy or scalar value.
    """
    if isinstance(item, dict):
        d = dict()
        for newk, newv in item.items():
            d[newk] = hide_passwords(newv, newk)
    elif isinstance(item, list):
        d = list()
        for i in item:
            d.append(hide_passwords(i))
    else:
        d = "*" * len(item) if isinstance(key, str) and "password" in key else item
    return d


class SimfleetConfig(object):
    """
    Load and normalize one SimFleet scenario configuration.

    Configuration is initialized with the supported agent collections, then
    optionally updated from a JSON scenario file. Scenario-provided values
    take precedence over constructor fallback values.

    The object also establishes defaults for simulation control, geospatial
    bounds, routing, XMPP/HTTP endpoints, strategy class paths, and the
    mobility metrics processor.

    Configuration values may be accessed through dictionary syntax or, when
    present in the internal configuration mapping, through attributes.
    """

    def __init__(self, filename=None, name=None, max_time=None, verbose=None):
        """
        Initialize and normalize scenario configuration.

        Args:
            filename (str | None): Optional JSON scenario file.
            name (str | None): Fallback simulation name.
            max_time: Fallback maximum simulation duration.
            verbose: Fallback verbosity setting.

        Notes:
            When a scenario file already defines ``simulation_name``,
            ``max_time`` or ``verbose``, those file values take precedence over
            the corresponding constructor fallbacks.
        """

        self.__config = dict()

        self.__config["fleets"] = []
        self.__config["transports"] = []
        self.__config["customers"] = []
        self.__config["stations"] = []
        self.__config["vehicles"] = []
        self.__config["stops"] = []

        if filename:
            self.load_config(filename)

        self.__config["simulation_name"] = self.__config.get("simulation_name", name)
        self.__config["max_time"] = self.__config.get("max_time", max_time)
        self.__config["verbose"] = self.__config.get("verbose", verbose)
        self.__config["simulation_password"] = self.__config.get("simulation_password", "secret")

        #New coords
        if self.__config.get("coords"):
            if self.__config.get("zoom"):
                input_location = get_bbox_from_location(self.__config["coords"], self.__config["zoom"])
                logger.debug(
                    "BoundingBox for {} is {}".format(self.__config["coords"], input_location[1])
                )
                self.__config["coords"] = input_location
            else:
                logger.debug("Default value 12 for Zoom variable")
                input_location = get_bbox_from_location(self.__config["coords"], 12)
                self.__config["zoom"] = 12
                self.__config["coords"] = input_location
        else:
            #raise Exception("Could not find coordinates for the entered location")
            logger.debug(
                "Could not find coordinates for the entered location. Default coordinates: Valencia, ES"
            )
            default_location = get_bbox_from_location("Valencia, ES", 11.75)
            self.__config["zoom"] = self.__config.get("zoom", 11.75)
            self.__config["coords"] = default_location

        #self.__config["coords"] = self.__config.get("coords", [39.47, -0.37])
        #self.__config["zoom"] = self.__config.get("zoom", 12)

        self.__config["transport_strategy"] = self.__config.get(
            "transport_strategy", "simfleet.common.lib.transports.strategies.delivery.FSMDeliveryBehaviour"
        )
        self.__config["customer_strategy"] = self.__config.get(
            "customer_strategy", "simfleet.common.lib.customers.strategies.taxicustomer.AcceptFirstRequestBehaviour"
        )
        self.__config["fleetmanager_strategy"] = self.__config.get(
            "fleetmanager_strategy", "simfleet.common.lib.fleet.strategies.fleetmanager.DelegateRequestBehaviour"
        )
        self.__config["directory_strategy"] = self.__config.get(
            "directory_strategy", "simfleet.common.agents.directory.DirectoryStrategyBehaviour"
        )
        self.__config["station_strategy"] = self.__config.get(
            "station_strategy", "simfleet.common.lib.stations.models.chargingstation.ChargingService"
        )
        #New vehicle
        self.__config["vehicle_strategy"] = self.__config.get(
            "vehicle_strategy", "simfleet.common.lib.vehicles.strategies.vehicle.FSMOneShotVehicleBehaviour"
        )
        # New statistics
        self.__config["mobility_metrics"] = self.__config.get(          #Metric - Renombrar
            "mobility_metrics", "simfleet.metrics.lib.mobilitystatistics.MobilityStatisticsClass"
        )

        self.__config["fleetmanager_name"] = self.__config.get(
            "fleetmanager_name", "fleetmanager"
        )
        self.__config["fleetmanager_password"] = self.__config.get(
            "fleetmanager_passwd", "fleetmanager_passwd"
        )
        self.__config["route_host"] = self.__config.get(
            "route_host", "http://router.project-osrm.org/"
        )
        self.__config["route_name"] = self.__config.get("route_name", "route")
        self.__config["route_password"] = self.__config.get(
            "route_passwd", "route_passwd"
        )
        self.__config["directory_name"] = self.__config.get(
            "directory_name", "directory"
        )
        self.__config["directory_password"] = self.__config.get(
            "directory_passwd", "directory_passwd"
        )

        self.__config["host"] = self.__config.get("host", "127.0.0.1")
        self.__config["xmpp_port"] = self.__config.get("xmpp_port", 5222)
        self.__config["http_port"] = self.__config.get("http_port", 9000)
        self.__config["http_ip"] = self.__config.get("http_ip", "127.0.0.1")

        logger.debug("Config loaded: {}".format(self))

    def load_config(self, filename):
        """
        Merge a JSON scenario file into the current configuration.

        Existing keys are overwritten by values from the file.

        Args:
            filename (str): Path to the scenario JSON file.
        """
        with open(filename, "r") as f:
            logger.info("Reading config {}".format(filename))
            self.__config.update(json.load(f))

    @property
    def num_managers(self):
        """
        Return the number of configured fleet managers.

        Returns:
            int: Number of entries in ``fleets``.
        """
        try:
            return len(self.__config["fleets"])
        except KeyError:
            return 0

    @property
    def num_transport(self):
        """
        Return the number of configured transport agents.

        Returns:
            int: Number of entries in ``transports``.
        """
        try:
            return len(self.__config["transports"])
        except KeyError:
            return 0

    @property
    def num_customers(self):
        """
        Return the number of configured customer agents.

        Returns:
            int: Number of entries in ``customers``.
        """
        try:
            return len(self.__config["customers"])
        except KeyError:
            return 0

    @property
    def num_stations(self):
        """
        Return the number of configured service stations.

        Returns:
            int: Number of entries in ``stations``.
        """
        try:
            return len(self.__config["stations"])
        except KeyError:
            return 0

    #New vehicle
    @property
    def num_vehicles(self):
        """
        Return the number of configured generic vehicles.

        Returns:
            int: Number of entries in ``vehicles``.
        """
        try:
            return len(self.__config["vehicles"])
        except KeyError:
            return 0

    # Bus line
    @property
    def num_stops(self):
        """
        Return the number of configured transport-stop agents.

        Returns:
            int: Number of entries in ``stops``.
        """
        try:
            return len(self.__config["stops"])
        except KeyError:
            return 0

    def __getitem__(self, item):
        """
        Return one configuration value using mapping syntax.

        Args:
            item (str): Configuration key.

        Returns:
            Any: Stored configuration value.

        Raises:
            KeyError: If the key does not exist.
        """
        return self.__config[item]

    def __getattr__(self, item):
        """
        Resolve unknown attributes from the internal configuration mapping.

        Args:
            item (str): Requested attribute name.

        Returns:
            Any: Configuration value when the key exists.
        """
        if item != "__config" and item in self.__config:
            return self.__config[item]
        else:
            return super().__getattribute__(item)

    def __setattr__(self, key, value):
        """
        Update an existing configuration key through attribute assignment.

        Attributes that do not correspond to an existing configuration key are
        stored normally on the SimfleetConfig instance.

        Args:
            key (str): Attribute or configuration key.
            value: Value to assign.
        """
        if "__config" in self.__dict__ and key in self.__config:
            self.__config[key] = value
        else:
            super().__setattr__(key, value)

    def __str__(self):
        """
        Serialize configuration as indented JSON after password masking.

        Returns:
            str: Human-readable sanitized configuration representation.
        """
        d = hide_passwords(self.__config)
        return json.dumps(d, indent=4)


def set_default_metrics(mobility_metrics):
    """
    Resolve the configured mobility metrics processor class.

    Args:
        mobility_metrics (str): Fully qualified metrics class path.

    Returns:
        dict: Mapping containing the resolved class under
        ``"mobility_metrics"``.
    """
    class_dict = {}

    class_dict['mobility_metrics'] = load_class(mobility_metrics)

    return class_dict

def set_default_strategies(
        directory_strategy,
        fleetmanager_strategy,
        transport_strategy,
        customer_strategy,
        station_strategy,
        vehicle_strategy,
):
    """
    Resolve configured default strategy import paths into Python classes.

    The resulting classes are used by agent factories whenever a concrete
    scenario entry does not provide its own strategy override.

    Args:
        directory_strategy (str): Directory strategy class path.
        fleetmanager_strategy (str): FleetManager strategy class path.
        transport_strategy (str): Transport strategy class path.
        customer_strategy (str): Customer strategy class path.
        station_strategy (str): Station service/strategy class path.
        vehicle_strategy (str): Generic vehicle strategy class path.

    Returns:
        dict: Resolved default strategy classes indexed by agent family.
    """

    class_dict = {}

    class_dict['directory'] = load_class(directory_strategy)
    class_dict['fleetmanager'] = load_class(fleetmanager_strategy)
    class_dict['transport'] = load_class(transport_strategy)
    class_dict['customer'] = load_class(customer_strategy)
    class_dict['station'] = load_class(station_strategy)
    class_dict['vehicle'] = load_class(vehicle_strategy)

    logger.debug(
        "Loaded default strategy classes: {}, {}, {}, {} and {}".format(
            class_dict['directory'],
            class_dict['fleetmanager'],
            class_dict['transport'],
            class_dict['customer'],
            class_dict['station'],
            class_dict['vehicle'],
        )
    )

    return class_dict
