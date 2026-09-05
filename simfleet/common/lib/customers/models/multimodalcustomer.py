import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING
from spade.template import Template

from simfleet.communications.protocol import (
    MULTIMODAL_CONTROL_PROTOCOL,
)

if TYPE_CHECKING:
    from simfleet.common.lib.customers.strategies.multimodal.strategyprofiles import (
        StrategyProfile,
    )

from simfleet.common.lib.customers.models.pedestrian import (
    PedestrianAgent,
)
from simfleet.common.lib.customers.models.taxicustomer import (
    TaxiCustomerAgent,
)
from simfleet.common.lib.customers.models.sharingcustomer import (
    SharingCustomerAgent,
)
from simfleet.common.lib.customers.models.stationsharingcustomer import (
    StationSharingCustomerAgent,
)
from simfleet.common.lib.customers.models.publictransportcustomer import (
    PublicTransportCustomerAgent,
)

@dataclass(frozen=True)
class DestinationStep:
    """
    One destination/leg of a multimodal customer itinerary.
    """

    id: str
    destination: list
    fleet_type: str
    strategy_path: str
    strategy_class: type
    strategy_profile: "StrategyProfile"
    dwell_time: float = 0.0


class MultiModalCustomerAgent(
    TaxiCustomerAgent,
    SharingCustomerAgent,
    StationSharingCustomerAgent,
    PublicTransportCustomerAgent,
):
    """
    Customer capable of using different mobility strategies sequentially.

    The common Customer/Pedestrian infrastructure is initialized only once.
    Modal capabilities are then initialized independently so the same agent
    can expose the APIs required by taxi, sharing, station-sharing and
    public-transport customer strategies.
    """

    def __init__(
        self,
        agentjid,
        password,
        **kwargs,
    ):
        PedestrianAgent.__init__(
            self,
            agentjid,
            password,
        )

        self._init_taxi_state()
        self._init_sharing_state()
        self._init_station_sharing_state()
        self._init_public_transport_state(
            **kwargs
        )

        #
        # Multimodal orchestration state.
        #
        self.active_strategy = None
        self.active_strategy_profile = None
        self._modal_completion_event = None

        #
        # Multimodal destination plan.
        #
        self.destination_plan = []
        self.current_destination_index = 0

    def run_strategy(self):
        """
        Start the multimodal orchestration strategy.
        """

        if self.running_strategy:
            return

        if self.strategy is None:
            raise RuntimeError(
                "MultiModalCustomerAgent has no multimodal strategy configured."
            )

        template = Template()

        template.set_metadata(
            "protocol",
            MULTIMODAL_CONTROL_PROTOCOL,
        )

        strategy = self.strategy()

        self.add_behaviour(
            strategy,
            template,
        )

        self.running_strategy = True

    def reset_movement_context(self):
        """Reset transient movement state from the completed itinerary leg."""
        self.pedestrian_dest = None
        self.dest = None
        self.set("path", None)
        self.chunked_path = None

    def reset_active_modality_context(self):
        """Reset transient state owned by the last active modal strategy."""
        if self.active_strategy_profile is None:
            return

        reset_method_name = self.active_strategy_profile.reset_method

        reset_method = getattr(
            self,
            reset_method_name,
            None,
        )

        if reset_method is None:
            raise RuntimeError(
                "Reset method '{}' is not available in "
                "MultiModalCustomerAgent.".format(
                    reset_method_name
                )
            )

        reset_method()

    def get_active_strategy_family(self):
        """Return the family of the active strategy profile, if any."""
        if self.active_strategy_profile is None:
            return None

        return self.active_strategy_profile.family

    def set_active_strategy(
        self,
        strategy,
        profile,
    ):
        """Register the currently active modal strategy and its profile."""
        self.active_strategy = strategy
        self.active_strategy_profile = profile

    def clear_active_strategy(self):
        """Clear references to the completed modal strategy."""
        self.active_strategy = None

    def prepare_modal_completion(self):
        """Prepare the completion signal for a cyclic modal strategy."""
        if self._modal_completion_event is None:
            self._modal_completion_event = asyncio.Event()
        else:
            self._modal_completion_event.clear()

    def notify_modal_completion(self):
        """Signal that the active modal strategy has finished."""
        if self._modal_completion_event is not None:
            self._modal_completion_event.set()

    async def wait_modal_completion(self):
        """Wait until the active cyclic modal strategy reports completion."""
        if self._modal_completion_event is None:
            raise RuntimeError(
                "Modal completion signal has not been prepared."
            )

        await self._modal_completion_event.wait()

    def should_stop_completed_modal_strategy(self):
        """
        Return whether completed modal behaviours must terminate
        so the multimodal orchestrator can advance the itinerary.
        """
        return True

    def clear_active_strategy_profile(self):
        """Clear the strategy profile after its modality context is reset."""
        self.active_strategy_profile = None

    def set_destination_plan(self, plan):
        """Set a new multimodal destination plan."""
        self.destination_plan = list(plan)
        self.current_destination_index = 0

    def get_destination_plan(self):
        """Return the multimodal destination plan."""
        return self.destination_plan

    def has_current_destination(self):
        """Return whether the itinerary has a current destination step."""
        return (
            0
            <= self.current_destination_index
            < len(self.destination_plan)
        )

    def get_current_destination_step(self):
        """Return the current DestinationStep, or None if the plan is finished."""
        if not self.has_current_destination():
            return None

        return self.destination_plan[
            self.current_destination_index
        ]

    def advance_destination(self):
        """Advance the itinerary to the next destination step."""
        if self.has_current_destination():
            self.current_destination_index += 1

        return self.get_current_destination_step()

    def get_current_destination_index(self):
        """Return the index of the current itinerary destination."""
        return self.current_destination_index
