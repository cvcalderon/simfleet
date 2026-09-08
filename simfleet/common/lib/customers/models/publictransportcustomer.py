from loguru import logger
from spade.template import Template

from simfleet.common.lib.customers.models.pedestrian import (
    PedestrianAgent,
)
from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
)


class PublicTransportCustomerAgent(
    PedestrianAgent
):
    """
    Customer model for journeys using scheduled public transport.

    The customer requests feasible journeys from a
    PublicTransportFleetManager and then executes the selected journey as an
    ordered sequence of walking and public-transport legs.

    The model separates three categories of state:

    Permanent planning constraints
        Maximum access walking distance, transfer walking distance, and
        number of transfers.

    Transient planning state
        Candidate journeys and the selected journey.

    Transient execution state
        Current leg index, boarded or candidate vehicle, logical stop, and
        Pattern currently awaited at a stop.

    Journey planning, boarding negotiation, walking, alighting, and FSM
    progression are responsibilities of the configured customer strategy.
    """

    def __init__(self, agentjid, password, **kwargs):
        """
        Initialize pedestrian infrastructure and PublicTransport state.

        Args:
            agentjid (str): XMPP JID used by the customer.
            password (str): XMPP authentication password.
            **kwargs: Optional public-transport planning constraints.
        """
        super().__init__(agentjid, password)
        self._init_public_transport_state(**kwargs)

    def _init_public_transport_state(self, **kwargs):
        """
        Initialize state owned by the PublicTransport customer capability.

        The helper is intentionally separate from ``__init__`` so
        MultiModalCustomerAgent can initialize PublicTransport state without
        executing the complete PublicTransportCustomerAgent constructor through
        multiple inheritance.

        Args:
            **kwargs: Optional planning constraints used to override defaults.
        """

        # Permanent journey-planning constraints.
        self.max_access_walking_distance = 600
        self.max_transfer_walking_distance = 300
        self.max_transfers = 2

        self.set_max_access_walking_distance(
            kwargs.get(
                "max_access_walking_distance",
                600,
            )
        )

        self.set_max_transfer_walking_distance(
            kwargs.get(
                "max_transfer_walking_distance",
                300,
            )
        )

        self.set_max_transfers(
            kwargs.get(
                "max_transfers",
                2,
            )
        )

        # Transient journey-planning state.
        self.journey_candidates = []
        self.journey = None

        # Transient journey-execution state.
        self.current_leg_index = 0
        self.current_vehicle = None
        self.current_stop = None
        self.waiting_pattern_id = None

    def set_max_access_walking_distance(
        self,
        distance
    ):
        """
        Configure the maximum walking distance to access public transport.

        Args:
            distance: Maximum access walking distance.

        Raises:
            ValueError: If the supplied distance is negative.
        """

        distance = float(
            distance
        )

        if distance < 0:

            raise ValueError(
                "max_access_walking_distance cannot be negative."
            )

        self.max_access_walking_distance = (
            distance
        )

    def get_max_access_walking_distance(
        self
    ):
        """
        Return the configured maximum access walking distance.

        Returns:
            float: Maximum access walking distance.
        """

        return (
            self.max_access_walking_distance
        )

    def set_max_transfer_walking_distance(
        self,
        distance
    ):
        """
        Configure the maximum walking distance allowed between public transport
        legs.

        Args:
            distance: Maximum transfer walking distance.

        Raises:
            ValueError: If the supplied distance is negative.
        """

        distance = float(
            distance
        )

        if distance < 0:

            raise ValueError(
                "max_transfer_walking_distance cannot be negative."
            )

        self.max_transfer_walking_distance = (
            distance
        )

    def get_max_transfer_walking_distance(
        self
    ):
        """
        Return the configured maximum transfer walking distance.

        Returns:
            float: Maximum transfer walking distance.
        """

        return (
            self.max_transfer_walking_distance
        )

    def set_max_transfers(
        self,
        max_transfers
    ):

        max_transfers = int(
            max_transfers
        )

        if max_transfers < 0:

            raise ValueError(
                "max_transfers cannot be negative."
            )

        self.max_transfers = (
            max_transfers
        )

    def get_max_transfers(
        self
    ):
        """
        Return the configured maximum number of transfers.

        Returns:
            int: Maximum permitted transfers.
        """

        return (
            self.max_transfers
        )

    def set_journey_candidates(
        self,
        candidates
    ):
        """
        Replace the candidate journeys available for selection.

        Candidate mappings are copied before storage so the customer owns its
        local planning context.

        Args:
            candidates (iterable[dict] | None): Journey candidates. ``None``
                clears the candidate set.
        """

        if candidates is None:

            self.journey_candidates = []

            return

        self.journey_candidates = [
            dict(candidate)
            for candidate in candidates
        ]

    def get_journey_candidates(
        self
    ):
        """
        Return the currently stored journey candidates.

        Returns:
            list[dict]: Candidate journeys.
        """

        return (
            self.journey_candidates
        )

    def clear_journey_candidates(
        self
    ):
        """Clear all locally stored journey candidates."""

        self.journey_candidates = []

    def set_journey(
        self,
        journey
    ):
        """
        Store a selected journey and reset its execution context.

        Selecting a new journey starts execution from its first leg and clears
        vehicle, stop, and awaited-Pattern state from any previous journey.

        Args:
            journey (dict | None): Selected journey. ``None`` clears the current
                journey and its execution state.
        """

        if journey is None:

            self.clear_journey()

            return

        self.journey = dict(
            journey
        )

        #
        # Selecting a new journey resets its
        # execution state.
        #

        self.current_leg_index = 0

        self.current_vehicle = None

        self.current_stop = None

        self.waiting_pattern_id = None

    def reset_public_transport_context(self):
        """
        Reset transient state owned by a completed PublicTransport service.

        Candidate journeys, the selected journey, and its execution state are
        cleared.

        Permanent planning constraints and generic customer state such as
        physical position, destination, FleetManagers, and accumulated metrics
        are intentionally preserved.
        """
        self.clear_journey_candidates()
        self.clear_journey()

    def get_journey(
        self
    ):
        """
        Return the selected public transport journey.

        Returns:
            dict | None: Selected journey.
        """

        return (
            self.journey
        )

    def clear_journey(
        self
    ):
        """
        Clear the selected journey and all journey-execution state.

        Planning constraints are intentionally preserved.
        """

        self.journey = None

        self.current_leg_index = 0

        self.current_vehicle = None

        self.current_stop = None

        self.waiting_pattern_id = None

    def get_current_leg(
        self
    ):
        """
        Return the journey leg currently being executed.

        Returns:
            dict | None: Current leg, or None when no valid current leg exists.
        """

        if self.journey is None:
            return None

        legs = self.journey.get(
            "legs",
            []
        )

        if self.current_leg_index < 0:
            return None

        if self.current_leg_index >= len(
            legs
        ):
            return None

        return legs[
            self.current_leg_index
        ]

    def advance_leg(
        self
    ):
        """
        Advance the selected journey to its next leg.

        Returns:
            dict | None: New current leg, or None when the journey has finished.
        """

        if self.journey is None:
            return None

        self.current_leg_index += 1

        return (
            self.get_current_leg()
        )

    def is_journey_finished(
        self
    ):
        """
        Return whether every leg of the selected journey has been completed.

        Returns:
            bool: True when the current leg index is beyond the final leg.
                False when no journey is selected.
        """

        if self.journey is None:
            return False

        legs = self.journey.get(
            "legs",
            []
        )

        return (
            self.current_leg_index
            >= len(legs)
        )

    def set_current_vehicle(
        self,
        vehicle_id
    ):
        """
        Store the vehicle associated with the active public transport leg.

        Args:
            vehicle_id: Vehicle JID. ``None`` clears the association.
        """

        if vehicle_id is None:

            self.current_vehicle = None

            return

        self.current_vehicle = str(
            vehicle_id
        )

    def get_current_vehicle(
        self
    ):
        """
        Return the vehicle associated with the active leg.

        Returns:
            str | None: Vehicle JID.
        """

        return (
            self.current_vehicle
        )

    def clear_current_vehicle(
        self
    ):
        """Clear the vehicle associated with the active public transport leg."""

        self.current_vehicle = None

    def set_current_stop(
        self,
        stop_id
    ):
        """
        Store the public transport stop at which the customer is logically
        located.

        Args:
            stop_id: Stop identifier. ``None`` clears the logical stop.
        """

        if stop_id is None:

            self.current_stop = None

            return

        self.current_stop = str(
            stop_id
        )

    def get_current_stop(
        self
    ):
        """
        Return the customer's current logical public transport stop.

        Returns:
            str | None: Stop identifier.
        """

        return (
            self.current_stop
        )

    def clear_current_stop(
        self
    ):
        """Clear the customer's logical public transport stop."""

        self.current_stop = None

    def set_waiting_pattern_id(
        self,
        pattern_id
    ):
        """
        Store the directional Pattern the customer is waiting for at a stop.

        Args:
            pattern_id: Pattern identifier. ``None`` clears the waiting state.
        """

        if pattern_id is None:

            self.waiting_pattern_id = None

            return

        self.waiting_pattern_id = str(
            pattern_id
        )

    def get_waiting_pattern_id(
        self
    ):
        """
        Return the directional Pattern currently awaited by the customer.

        Returns:
            str | None: Pattern identifier.
        """

        return (
            self.waiting_pattern_id
        )

    def clear_waiting_pattern_id(
        self
    ):
        """Clear the directional Pattern currently awaited at a stop."""

        self.waiting_pattern_id = None

    def run_strategy(
        self
    ):
        """
        Start the operational PublicTransport customer strategy once.

        Journey planning and execution use REQUEST_PROTOCOL. The inherited
        TravelBehaviour remains responsible for TRAVEL_PROTOCOL position updates
        while the customer is being transported.

        ``running_strategy`` prevents duplicate strategy instances.
        """

        if self.running_strategy:
            return

        if self.strategy is None:

            logger.error(
                "Public transport customer {} "
                "has no strategy configured.".format(
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
