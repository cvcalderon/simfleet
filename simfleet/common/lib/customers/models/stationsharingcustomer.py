from simfleet.common.lib.customers.models.pedestrian import PedestrianAgent


class StationSharingCustomerAgent(PedestrianAgent):
    """
    Customer model for station-based sharing services.

    A station-sharing trip is represented by four independent pieces of
    transient context:

    - candidate origin and destination stations;
    - the selected origin and destination station pair;
    - the transport assigned by the origin station;
    - optional failure context for station operations or trip execution.

    Candidate discovery and pair selection are distinct from vehicle
    assignment. Admission to an origin station therefore does not itself
    represent transport assignment.

    Walking, station negotiation, vehicle pickup, transport travel, vehicle
    return, failure handling, and FSM transitions are implemented by the
    configured StationSharing customer strategy.
    """

    def __init__(self, agentjid, password):
        """
        Initialize pedestrian infrastructure and StationSharing-specific state.

        Args:
            agentjid (str): XMPP JID used by the customer.
            password (str): XMPP authentication password.
        """
        super().__init__(agentjid, password)
        self._init_station_sharing_state()

    def _init_station_sharing_state(self):
        """
        Initialize state owned exclusively by the StationSharing capability.

        The helper is separate from ``__init__`` so MultiModalCustomerAgent can
        initialize StationSharing state without traversing the complete
        StationSharingCustomerAgent constructor through multiple inheritance.
        """
        self.origin_station_candidates = []
        self.destination_station_candidates = []

        self.origin_station = None
        self.destination_station = None

        self.station_sharing_transport_id = None

        self.trip_failed = False
        self.failure_operation = None
        self.failure_reason = None
        self.failure_station_id = None

    def set_origin_station_candidates(self, stations):
        """
        Replace the candidate origin stations for the current service.

        Station mappings are copied before storage.

        Args:
            stations (iterable[dict] | None): Candidate origin stations.
                ``None`` clears the candidate set.
        """
        if stations is None:
            self.origin_station_candidates = []
            return

        self.origin_station_candidates = [
            dict(station)
            for station in stations
        ]

    def get_origin_station_candidates(self):
        """
        Return candidate origin stations.

        Returns:
            list[dict]: Locally stored origin-station candidates.
        """
        return self.origin_station_candidates

    def clear_origin_station_candidates(self):
        """Clear candidate origin stations."""
        self.origin_station_candidates = []

    def set_destination_station_candidates(self, stations):
        """
        Replace the candidate destination stations for the current service.

        Station mappings are copied before storage.

        Args:
            stations (iterable[dict] | None): Candidate destination stations.
                ``None`` clears the candidate set.
        """
        if stations is None:
            self.destination_station_candidates = []
            return

        self.destination_station_candidates = [
            dict(station)
            for station in stations
        ]

    def get_destination_station_candidates(self):
        """
        Return candidate destination stations.

        Returns:
            list[dict]: Locally stored destination-station candidates.
        """
        return self.destination_station_candidates

    def clear_destination_station_candidates(self):
        """Clear candidate destination stations."""
        self.destination_station_candidates = []

    def clear_station_candidates(self):
        """
        Clear both origin and destination station candidate sets.

        The already selected origin and destination stations are intentionally
        left unchanged.
        """
        self.clear_origin_station_candidates()
        self.clear_destination_station_candidates()

    def set_origin_station(self, station):
        """
        Store the origin station selected for the current trip.

        The station mapping is copied before storage.

        Args:
            station (dict | None): Selected origin station. ``None`` clears it.
        """
        if station is None:
            self.origin_station = None
            return

        self.origin_station = dict(
            station
        )

    def get_origin_station(self):
        """
        Return the selected origin station.

        Returns:
            dict | None: Origin station definition.
        """
        return self.origin_station

    def get_origin_station_id(self):
        """
        Return the JID of the selected origin station.

        Returns:
            str | None: Origin station JID.
        """
        if self.origin_station is None:
            return None

        return self.origin_station.get(
            "jid"
        )

    def get_origin_station_position(self):
        """
        Return the stored position of the selected origin station.

        Returns:
            Any: Station position, or None when no origin station is selected.
        """
        if self.origin_station is None:
            return None

        return self.origin_station.get(
            "position"
        )

    def clear_origin_station(self):
        """Clear the selected origin station."""
        self.origin_station = None

    def set_destination_station(self, station):
        """
        Store the destination station selected for the current trip.

        The station mapping is copied before storage.

        Args:
            station (dict | None): Selected destination station. ``None`` clears
                it.
        """
        if station is None:
            self.destination_station = None
            return

        self.destination_station = dict(
            station
        )

    def get_destination_station(self):
        """
        Return the selected destination station.

        Returns:
            dict | None: Destination station definition.
        """
        return self.destination_station

    def get_destination_station_id(self):
        """
        Return the JID of the selected destination station.

        Returns:
            str | None: Destination station JID.
        """
        if self.destination_station is None:
            return None

        return self.destination_station.get(
            "jid"
        )

    def get_destination_station_position(self):
        """
        Return the stored position of the selected destination station.

        Returns:
            Any: Station position, or None when no destination station is selected.
        """
        if self.destination_station is None:
            return None

        return self.destination_station.get(
            "position"
        )

    def clear_destination_station(self):
        """Clear the selected destination station."""
        self.destination_station = None

    def set_station_sharing_transport_id(self, transport_id):
        """
        Store the vehicle assigned by the origin station.

        The explicit StationSharing-specific name avoids sharing this state with
        free-floating Sharing transport context.

        Args:
            transport_id: Assigned transport JID.
        """
        self.station_sharing_transport_id = transport_id

    def get_station_sharing_transport_id(self):
        """
        Return the vehicle assigned to the current StationSharing trip.

        Returns:
            str | None: Assigned transport JID.
        """
        return self.station_sharing_transport_id

    def clear_station_sharing_transport_id(self):
        """Clear the vehicle assigned to the current StationSharing trip."""
        self.station_sharing_transport_id = None

    def set_trip_failure(
        self,
        operation,
        reason,
        station=None
    ):
        """
        Store failure context for the current StationSharing trip.

        This method records local model state only. It does not emit canonical
        ``service_failed`` statistics; those events are produced exclusively by
        the strategy under the mobility statistics contract.

        Args:
            operation: Station operation associated with the failure, when
                applicable.
            reason: Failure reason recorded by the strategy.
            station (dict | None): Station associated with the failure.
        """
        self.trip_failed = True
        self.failure_operation = operation
        self.failure_reason = reason
        self.failure_station_id = None
        if station is not None:
            self.failure_station_id = station.get("jid")

    def clear_trip_failure(self):
        """
        Clear all locally stored StationSharing failure context.
        """
        self.trip_failed = False
        self.failure_operation = None
        self.failure_reason = None
        self.failure_station_id = None

    def reset_station_sharing_context(self):
        """
        Reset transient state owned by a completed StationSharing service.

        The reset clears:

        - candidate station sets;
        - selected origin and destination stations;
        - assigned StationSharing transport;
        - local trip-failure context.

        Generic customer position, destination, FleetManagers, and accumulated
        metrics are intentionally preserved.
        """
        self.clear_station_candidates()
        self.clear_origin_station()
        self.clear_destination_station()
        self.clear_station_sharing_transport_id()
        self.clear_trip_failure()
