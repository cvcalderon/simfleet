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
    Customer specialized in executing multimodal
    public transport journeys.

    The customer does not register in a FleetManager.

    It requests journey candidates from a
    PublicTransportFleetManager and executes the
    selected journey leg by leg.
    """

    def __init__(
        self,
        agentjid,
        password,
        **kwargs
    ):

        super().__init__(
            agentjid,
            password
        )

        #
        # Journey-planning constraints.
        #

        self.max_access_walking_distance = 600
        self.max_transfer_walking_distance = 300
        self.max_transfers = 2

        self.set_max_access_walking_distance(
            kwargs.get(
                "max_access_walking_distance",
                600
            )
        )

        self.set_max_transfer_walking_distance(
            kwargs.get(
                "max_transfer_walking_distance",
                300
            )
        )

        self.set_max_transfers(
            kwargs.get(
                "max_transfers",
                2
            )
        )

        #
        # Journey planning state.
        #

        self.journey_candidates = []
        self.journey = None

        #
        # Journey execution state.
        #

        self.current_leg_index = 0

        self.current_vehicle = None

        self.current_stop = None

        self.waiting_pattern_id = None

    def set_max_access_walking_distance(
        self,
        distance
    ):

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

        return (
            self.max_access_walking_distance
        )

    def set_max_transfer_walking_distance(
        self,
        distance
    ):

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

        return (
            self.max_transfers
        )

    def set_journey_candidates(
        self,
        candidates
    ):

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

        return (
            self.journey_candidates
        )

    def clear_journey_candidates(
        self
    ):

        self.journey_candidates = []

    def set_journey(
        self,
        journey
    ):

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

    def get_journey(
        self
    ):

        return (
            self.journey
        )

    def clear_journey(
        self
    ):

        self.journey = None

        self.current_leg_index = 0

        self.current_vehicle = None

        self.current_stop = None

        self.waiting_pattern_id = None

    def get_current_leg(
        self
    ):

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

        if self.journey is None:
            return None

        self.current_leg_index += 1

        return (
            self.get_current_leg()
        )

    def is_journey_finished(
        self
    ):

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

        if vehicle_id is None:

            self.current_vehicle = None

            return

        self.current_vehicle = str(
            vehicle_id
        )

    def get_current_vehicle(
        self
    ):

        return (
            self.current_vehicle
        )

    def clear_current_vehicle(
        self
    ):

        self.current_vehicle = None

    def set_current_stop(
        self,
        stop_id
    ):

        if stop_id is None:

            self.current_stop = None

            return

        self.current_stop = str(
            stop_id
        )

    def get_current_stop(
        self
    ):

        return (
            self.current_stop
        )

    def clear_current_stop(
        self
    ):

        self.current_stop = None

    def set_waiting_pattern_id(
        self,
        pattern_id
    ):

        if pattern_id is None:

            self.waiting_pattern_id = None

            return

        self.waiting_pattern_id = str(
            pattern_id
        )

    def get_waiting_pattern_id(
        self
    ):

        return (
            self.waiting_pattern_id
        )

    def clear_waiting_pattern_id(
        self
    ):

        self.waiting_pattern_id = None

    def run_strategy(
        self
    ):
        """
        Starts the operational Public Transport
        customer strategy.

        Journey planning and execution use
        REQUEST_PROTOCOL.

        The inherited TravelBehaviour remains
        responsible for TRAVEL_PROTOCOL position
        updates while the customer is inside a
        transport.
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
