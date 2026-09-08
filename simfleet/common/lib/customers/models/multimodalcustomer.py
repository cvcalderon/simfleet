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
    Resolved itinerary step executed by a MultiModalCustomerAgent.

    CustomerFactory resolves the strategy class and StrategyProfile before
    runtime orchestration begins, so the multimodal FSM does not perform
    dynamic strategy discovery for each leg.

    Attributes:
        id (str): Identifier of the itinerary step.
        destination (list): Destination coordinates for this step.
        fleet_type (str): Fleet type exposed while the step is active.
        strategy_path (str): Configured import path of the modal strategy.
        strategy_class (type): Already resolved strategy class.
        strategy_profile (StrategyProfile): Runtime contract describing the
            strategy family, protocols, completion mechanism, and reset
            method.
        dwell_time (float): Time to remain at the reached destination before
            advancing to the next step.
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
    Customer that executes heterogeneous mobility services sequentially.

    The common Customer/Pedestrian infrastructure is initialized exactly once
    through PedestrianAgent. Each modal capability then initializes only the
    transient state it owns through its dedicated ``_init_*_state`` helper.

    A multimodal itinerary is represented by an ordered list of
    DestinationStep objects. For each step the orchestration FSM starts
    exactly one modal strategy, waits for explicit completion, resets that
    modality's transient state, clears movement context, and then advances to
    the next destination.

    Physical position, generic customer identity, accumulated metrics, and
    other global customer state are preserved across itinerary steps.
    """

    def __init__(
        self,
        agentjid,
        password,
        **kwargs,
    ):
        """
        Initialize shared customer infrastructure and all supported modalities.

        Multiple inheritance is handled deliberately: PedestrianAgent initializes
        the common Customer and movement infrastructure once, after which each
        modal capability initializes only its own state.

        Args:
            agentjid (str): XMPP JID used by the multimodal customer.
            password (str): XMPP authentication password.
            **kwargs: Optional modality-specific configuration, currently including
                PublicTransport planning constraints.
        """
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
        Start the multimodal orchestration FSM once.

        The orchestrator receives MULTIMODAL_CONTROL_PROTOCOL messages and is
        responsible only for sequencing modal strategies, not for implementing
        modality-specific decisions.
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
        """
        Reset transient pedestrian/movement state from the completed itinerary
        step.

        The customer's physical position is intentionally preserved. Only pending
        movement targets, route path, and chunked movement state are cleared.
        """
        self.pedestrian_dest = None
        self.dest = None
        self.set("path", None)
        self.chunked_path = None

    def reset_active_modality_context(self):
        """
        Invoke the reset contract associated with the completed modal family.

        The reset method name is supplied by the active StrategyProfile. This
        keeps the multimodal orchestrator independent from modality-specific
        state details.

        Raises:
            RuntimeError: If the configured reset method is not available on the
                multimodal customer.
        """
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
        """
        Store the modal behaviour currently executing and its StrategyProfile.

        The behaviour reference and profile are kept separately because the
        behaviour is cleared immediately after completion, while the profile must
        survive until modality-specific reset has been performed.

        Args:
            strategy: Active modal SPADE behaviour.
            profile: StrategyProfile associated with that behaviour.
        """
        self.active_strategy = strategy
        self.active_strategy_profile = profile

    def clear_active_strategy(self):
        """
        Clear the completed modal behaviour reference.

        The active StrategyProfile is intentionally retained until the modality
        context has been reset.
        """
        self.active_strategy = None

    def prepare_modal_completion(self):
        """
        Prepare the reusable completion Event for the next modal strategy.

        The Event is created on first use and cleared before subsequent modal
        executions. It must be prepared before the modal behaviour is started to
        avoid losing an early completion signal.
        """
        if self._modal_completion_event is None:
            self._modal_completion_event = asyncio.Event()
        else:
            self._modal_completion_event.clear()

    def notify_modal_completion(self):
        """
        Signal explicit completion of the active modal strategy.

        Legacy CustomerAgent implementations treat this hook as a no-op.
        MultiModalCustomerAgent converts it into an asyncio.Event notification
        consumed by the orchestration FSM.
        """
        if self._modal_completion_event is not None:
            self._modal_completion_event.set()

    async def wait_modal_completion(self):
        """
        Suspend orchestration until the active modal strategy reports completion.

        Raises:
            RuntimeError: If completion signalling was not prepared before the
                modal strategy started.
        """
        if self._modal_completion_event is None:
            raise RuntimeError(
                "Modal completion signal has not been prepared."
            )

        await self._modal_completion_event.wait()

    def should_stop_completed_modal_strategy(self):
        """
        Return True so a completed modal behaviour terminates after signalling.

        Legacy customers return False from the base implementation because their
        strategy may remain cyclic. Multimodal execution requires the completed
        modal behaviour to stop before the itinerary can advance.
        """
        return True

    def clear_active_strategy_profile(self):
        """
        Clear the StrategyProfile after modality-specific cleanup is complete.
        """
        self.active_strategy_profile = None

    def set_destination_plan(self, plan):
        """
        Replace the multimodal itinerary and restart it from the first step.

        Args:
            plan (iterable[DestinationStep]): Already resolved itinerary steps.
        """
        self.destination_plan = list(plan)
        self.current_destination_index = 0

    def get_destination_plan(self):
        """
        Return the ordered multimodal destination plan.

        Returns:
            list[DestinationStep]: Current itinerary steps.
        """
        return self.destination_plan

    def has_current_destination(self):
        """
        Return whether the itinerary index currently points to a valid step.
        """
        return (
            0
            <= self.current_destination_index
            < len(self.destination_plan)
        )

    def get_current_destination_step(self):
        """
        Return the itinerary step currently being executed.

        Returns:
            DestinationStep | None: Current step, or None when the itinerary is
            finished.
        """
        if not self.has_current_destination():
            return None

        return self.destination_plan[
            self.current_destination_index
        ]

    def advance_destination(self):
        """
        Advance the itinerary to the next DestinationStep.

        Returns:
            DestinationStep | None: New current step, or None when the itinerary
            is complete.
        """
        if self.has_current_destination():
            self.current_destination_index += 1

        return self.get_current_destination_step()

    def get_current_destination_index(self):
        """
        Return the zero-based index of the current itinerary step.

        Returns:
            int: Current destination-plan index.
        """
        return self.current_destination_index
