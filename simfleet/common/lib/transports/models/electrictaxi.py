from simfleet.common.mixins.chargeable import ChargeableMixin
from simfleet.common.lib.transports.models.taxi import TaxiAgent


class ElectricTaxiAgent(ChargeableMixin, TaxiAgent):
    """
        Represents an electric taxi agent with enhanced functionalities for managing charging stations,
        nearby station selection, and other electric vehicle-specific features.
        This class extends the capabilities of `TaxiAgent` and integrates charging functionality
        through the `ChargeableMixin`.

        Attributes:
            stations (list): A list of available charging stations.
            nearby_station (tuple): The ID and position of the nearest charging station.
            arguments (dict): Additional custom arguments for the access to station agent.

        Methods:
            set_stations(stations):
                Sets the list of available charging stations.
            get_stations():
                Retrieves the list of available charging stations.
            get_number_stations():
                Gets the total number of charging stations.
            set_nearby_station(station):
                Sets the nearest charging station.
            get_nearby_station():
                Retrieves the nearest charging station.
            get_nearby_station_id():
                Retrieves the ID of the nearest charging station.
            get_nearby_station_position():
                Retrieves the position of the nearest charging station.
        """
    def __init__(self, agentjid, password, **kwargs):
        ChargeableMixin.__init__(self)
        TaxiAgent.__init__(self, agentjid, password, **kwargs)

        self.stations = None
        self.nearby_station = None
        #self.set("current_station", None)
        self.current_station = None

        self.arguments = {}

    def set_stations(self, stations):
        """
               Set the list of charging stations.

               Args:
                   stations (list): A list of charging station details.
        """
        self.stations = stations

    def get_stations(self):
        """
                Retrieve the list of charging stations.

                Returns:
                    list: A list of charging station details.
        """
        return self.stations

    def get_number_stations(self):
        """
                Retrieve the number of available charging stations.

                Returns:
                    int: The number of charging stations in the list.
        """
        #return len(self.stations)
        if self.stations is None:
            return 0

        return len(self.stations)

    def set_nearby_station(self, station):
        """
                Set the nearest charging station.

                Args:
                    station (tuple): A tuple containing the ID and position of the station.
        """
        self.nearby_station = station

    def get_nearby_station(self):
        """
                Retrieve the nearest charging station.

                Returns:
                    tuple: A tuple containing the JID and position of the nearest charging station.
        """
        return self.nearby_station

    def get_nearby_station_id(self):
        """
                Retrieve the ID of the nearest charging station.

                Returns:
                    Any: The ID of the nearest charging station.
        """
        return self.nearby_station[0]

    def get_nearby_station_position(self):
        """
                Retrieve the position of the nearest charging station.

                Returns:
                    Any: The position of the nearest charging station.
        """
        return self.nearby_station[1]

    def clear_nearby_station(self):
        self.nearby_station = None

    def set_current_station(self, station_id):
        self.current_station = station_id

    def get_current_station(self):
        return self.current_station

    def clear_current_station(self):
        self.current_station = None

