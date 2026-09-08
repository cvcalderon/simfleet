from abc import ABCMeta
from spade.behaviour import CyclicBehaviour, FSMBehaviour


class StrategyBehaviour(CyclicBehaviour, metaclass=ABCMeta):
    """
    Base class for cyclic SimFleet operational strategies.

    StrategyBehaviour combines SPADE's CyclicBehaviour lifecycle with the
    generic SimFleet strategy contract. Concrete subclasses implement
    ``run()`` while inheriting common lifecycle instrumentation.

    Starting and ending the strategy emit ``initial_event`` and
    ``final_event`` respectively into the owning agent's StatisticsStore.
    These events describe behaviour lifecycle only; modality-specific
    canonical service metrics are emitted by concrete strategies.

    The class does not define communication protocols, performatives, or
    decision rules. Those responsibilities belong to each concrete strategy
    family.
    """

    async def on_start(self) -> None:
        """
        Mark the beginning of a cyclic strategy lifecycle.

        The hook emits the generic ``initial_event`` into the owning agent's
        StatisticsStore before normal strategy execution begins.
        """
        self.agent.events_store.emit(
            event_type="initial_event",
            details={}
        )

    async def on_end(self) -> None:
        """
        Mark the end of a cyclic strategy lifecycle.

        The hook emits the generic ``final_event`` into the owning agent's
        StatisticsStore. Concrete strategy families may extend this hook with
        modality-specific completion handling.
        """
        self.agent.events_store.emit(
            event_type="final_event",
            details={}
        )

    async def run(self) -> None:
        """
        Execute one iteration of the concrete cyclic strategy.

        Concrete strategy subclasses must implement this method.

        Raises:
            NotImplementedError: When the strategy does not provide an
                implementation.
        """
        raise NotImplementedError


class FSMSimfleetBehaviour(FSMBehaviour):
    """
    Base class for finite-state SimFleet operational strategies.

    The class extends SPADE's FSMBehaviour with the same generic lifecycle
    instrumentation used by StrategyBehaviour. Concrete FSM implementations
    define their states, transitions, protocols, decisions, and
    modality-specific events.

    ``initial_event`` is emitted when the FSM starts and ``final_event`` when
    the complete FSM behaviour terminates. State transitions themselves do
    not automatically emit either event.

    This class deliberately contains no modality-specific completion hook.
    Multimodal completion remains the responsibility of the concrete modal
    strategy contract.
    """

    async def on_start(self) -> None:
        """
        Mark the beginning of an FSM strategy lifecycle.

        The hook emits one generic ``initial_event`` before the FSM starts
        processing its states.
        """
        self.agent.events_store.emit(
            event_type="initial_event",
            details={}
        )

    async def on_end(self) -> None:
        """
        Mark the end of the complete FSM strategy lifecycle.

        The hook emits one generic ``final_event`` after the FSM terminates.
        """
        self.agent.events_store.emit(
            event_type="final_event",
            details={}
        )
