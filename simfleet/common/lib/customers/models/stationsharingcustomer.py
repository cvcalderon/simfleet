from simfleet.common.lib.customers.models.pedestrian import PedestrianAgent


class StationSharingCustomerAgent(PedestrianAgent):
    """
    Customer using a station-based sharing service.

    The customer receives origin and destination station candidates
    from the FleetManager and decides which station pair to use.
    """

    def __init__(self, agentjid, password):
        super().__init__(agentjid, password)

        self.origin_station_candidates = []
        self.destination_station_candidates = []

        self.origin_station = None
        self.destination_station = None

        self.current_transport = None

        self.trip_failed = False
        self.failure_operation = None
        self.failure_reason = None
        self.failure_station_id = None

    def set_origin_station_candidates(self, stations):
        if stations is None:
            self.origin_station_candidates = []
            return

        self.origin_station_candidates = [
            dict(station)
            for station in stations
        ]

    def get_origin_station_candidates(self):
        return self.origin_station_candidates

    def clear_origin_station_candidates(self):
        self.origin_station_candidates = []

    def set_destination_station_candidates(self, stations):
        if stations is None:
            self.destination_station_candidates = []
            return

        self.destination_station_candidates = [
            dict(station)
            for station in stations
        ]

    def get_destination_station_candidates(self):
        return self.destination_station_candidates

    def clear_destination_station_candidates(self):
        self.destination_station_candidates = []

    def clear_station_candidates(self):
        self.clear_origin_station_candidates()
        self.clear_destination_station_candidates()

    def set_origin_station(self, station):
        if station is None:
            self.origin_station = None
            return

        self.origin_station = dict(
            station
        )

    def get_origin_station(self):
        return self.origin_station

    def get_origin_station_id(self):
        if self.origin_station is None:
            return None

        return self.origin_station.get(
            "jid"
        )

    def get_origin_station_position(self):
        if self.origin_station is None:
            return None

        return self.origin_station.get(
            "position"
        )

    def clear_origin_station(self):
        self.origin_station = None

    def set_destination_station(self, station):
        if station is None:
            self.destination_station = None
            return

        self.destination_station = dict(
            station
        )

    def get_destination_station(self):
        return self.destination_station

    def get_destination_station_id(self):
        if self.destination_station is None:
            return None

        return self.destination_station.get(
            "jid"
        )

    def get_destination_station_position(self):
        if self.destination_station is None:
            return None

        return self.destination_station.get(
            "position"
        )

    def clear_destination_station(self):
        self.destination_station = None

    def set_current_transport(self, transport_id):
        self.current_transport = transport_id

    def get_current_transport(self):
        return self.current_transport

    def clear_current_transport(self):
        self.current_transport = None

    def set_trip_failure(
        self,
        operation,
        reason,
        station=None
    ):
        """Store station-sharing failure state without emitting statistics.

        Canonical service_failed events are emitted exclusively by strategy
        modules under the mobility statistics contract.
        """
        self.trip_failed = True
        self.failure_operation = operation
        self.failure_reason = reason
        self.failure_station_id = None
        if station is not None:
            self.failure_station_id = station.get("jid")

    def clear_trip_failure(self):
        self.trip_failed = False
        self.failure_operation = None
        self.failure_reason = None
        self.failure_station_id = None
