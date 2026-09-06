from spade.behaviour import (
    FSMBehaviour,
    State,
)

from simfleet.utils.status import (
    CUSTOMER_IN_DEST,
)

import asyncio

# ==================================================================
# --------------------- Multimodal states --------------------------
# ==================================================================

PREPARE_MODALITY = "PREPARE_MODALITY"
WAIT_MODALITY = "WAIT_MODALITY"
WAIT_DESTINATION = "WAIT_DESTINATION"


class MultiModalState(State):
    """
    Base state for the multimodal FSM.

    SPADE reuses State instances across FSM transitions, so the
    previous next_state must be cleared before each execution.
    """

    async def on_start(self):
        self.next_state = None

class PrepareModalityState(MultiModalState):
    """
    Prepare and launch the strategy associated with the current
    multimodal destination step.
    """

    async def run(self):

        step = (
            self.agent.get_current_destination_step()
        )

        if step is None:
            return

        #
        # Activate the current itinerary destination.
        #

        self.agent.customer_dest = (
            step.destination
        )

        self.agent.set_fleet_type(
            step.fleet_type
        )

        #
        # Shared state from the previous modality must not be reused.
        #

        self.agent.set_fleetmanagers(
            None
        )

        self.agent.status = None

        #
        # Strategy information was already resolved by CustomerFactory.
        #

        profile = (
            step.strategy_profile
        )

        strategy = (
            step.strategy_class()
        )

        template = (
            profile.build_template()
        )

        #
        # Prepare the completion mechanism before starting the strategy.
        #

        profile.prepare_completion(
            self.agent
        )

        #
        # Add exactly one modal strategy for this itinerary step.
        #

        self.agent.add_behaviour(
            strategy,
            template,
        )

        self.agent.set_active_strategy(
            strategy,
            profile,
        )

        self.set_next_state(
            WAIT_MODALITY
        )


class WaitModalityState(MultiModalState):
    """
    Wait until the active modal strategy completes the current trip.
    """

    async def run(self):

        strategy = (
            self.agent.active_strategy
        )

        profile = (
            self.agent.active_strategy_profile
        )

        if strategy is None or profile is None:
            raise RuntimeError(
                "WAIT_MODALITY requires an active strategy "
                "and strategy profile."
            )

        #
        # Suspend this FSM until the modal strategy really finishes.
        #

        await profile.wait_for_completion(
            self.agent,
        )

        #
        # The modal behaviour is no longer active.
        #
        # Keep the profile until WAIT_DESTINATION performs
        # the modality-specific reset.
        #

        self.agent.clear_active_strategy()

        #
        # A finished behaviour is not necessarily a successful trip.
        #

        if self.agent.status != CUSTOMER_IN_DEST:
            await self.agent.stop()
            return

        self.set_next_state(
            WAIT_DESTINATION
        )

class WaitDestinationState(MultiModalState):
    """
    Apply destination dwell time, clean the completed modality
    context and advance the itinerary.
    """

    async def run(self):

        step = (
            self.agent.get_current_destination_step()
        )

        if step is None:
            return

        #
        # Remain at the reached destination for the configured time.
        #

        if step.dwell_time > 0:
            await asyncio.sleep(
                step.dwell_time
            )

        #
        # Clean transient state from the completed modality and movement.
        #

        self.agent.reset_active_modality_context()
        self.agent.reset_movement_context()

        #
        # FleetManager discovery belongs to the active modality.
        #

        self.agent.set_fleetmanagers(
            None
        )

        #
        # Advance the itinerary only after the completed modality
        # has been fully cleaned.
        #

        self.agent.advance_destination()

        #
        # The completed StrategyProfile is no longer required.
        #

        self.agent.clear_active_strategy_profile()

        #
        # Prepare another modality only when another destination exists.
        # Otherwise, no next state means natural FSM termination.
        #

        if self.agent.has_current_destination():
            self.set_next_state(
                PREPARE_MODALITY
            )

# ==================================================================
# ------------------ Multimodal strategy FSM -----------------------
# ==================================================================

class FSMMultiModalCustomerStrategyBehaviour(
    FSMBehaviour
):
    """
    Orchestrates the sequential execution of modal customer
    strategies for a MultiModalCustomerAgent.

    The FSM coordinates modal behaviours but does not implement
    modality-specific decisions.
    """

    def setup(self):

        #
        # States
        #

        self.add_state(
            PREPARE_MODALITY,
            PrepareModalityState(),
            initial=True,
        )

        self.add_state(
            WAIT_MODALITY,
            WaitModalityState(),
        )

        self.add_state(
            WAIT_DESTINATION,
            WaitDestinationState(),
        )

        #
        # Transitions
        #

        self.add_transition(
            PREPARE_MODALITY,
            WAIT_MODALITY,
        )

        self.add_transition(
            WAIT_MODALITY,
            WAIT_DESTINATION,
        )

        self.add_transition(
            WAIT_DESTINATION,
            PREPARE_MODALITY,
        )
