from simfleet.common.mixins.chargeable import ChargeableMixin
from simfleet.common.lib.transports.models.taxi import TaxiAgent


class ElectricTaxiAgent(ChargeableMixin, TaxiAgent):
    """
    Taxi model with battery and charging-station context.

    ElectricTaxiAgent combines the taxi service lifecycle with the charging
    capabilities provided by ChargeableMixin. The model stores charging
    infrastructure discovered by the strategy and tracks the station being
    considered or currently used.

    Charging decisions and FSM transitions remain responsibilities of the
    electric-taxi strategy.
    """
    def __init__(self, agentjid, password, **kwargs):
        """
        Initialize taxi and charging-specific runtime state.

        Args:
            agentjid (str): XMPP JID used by the electric taxi.
            password (str): XMPP authentication password.
            **kwargs: Additional taxi configuration arguments.
        """
        ChargeableMixin.__init__(self)
        TaxiAgent.__init__(self, agentjid, password, **kwargs)

        self.stations = None
        self.nearby_station = None
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
        Return the identifier of the selected nearby charging station.

        Returns:
            Any: Station identifier stored in the nearby-station tuple.
        """
        return self.nearby_station[0]

    def get_nearby_station_position(self):
        """
        Return the position of the selected nearby charging station.

        Returns:
            Any: Station position stored in the nearby-station tuple.
        """
        return self.nearby_station[1]

    def clear_nearby_station(self):
        """
        Clear the currently selected nearby charging station.
        """
        self.nearby_station = None

    def set_current_station(self, station_id):
        """
        Store the charging station currently associated with the taxi.

        Args:
            station_id: Charging-station identifier.
        """
        self.current_station = station_id

    def get_current_station(self):
        """
        Return the charging station currently associated with the taxi.
        """
        return self.current_station

    def clear_current_station(self):
        """
        Clear the current charging-station association.
        """
        self.current_station = None

