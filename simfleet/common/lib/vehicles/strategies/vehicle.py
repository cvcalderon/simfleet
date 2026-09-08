import asyncio

from loguru import logger
from spade.behaviour import State, FSMBehaviour

from simfleet.common.lib.vehicles.models.vehicle import VehicleStrategyBehaviour
from simfleet.utils.helpers import PathRequestException, AlreadyInDestination
from simfleet.utils.status import VEHICLE_WAITING, VEHICLE_MOVING_TO_DESTINATION, VEHICLE_IN_DEST
from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour


################################################################
#                                                              #
#                   OneShot Vehicle Strategy                   #
#                                                              #
################################################################

class OneShotVehicleWaitingState(VehicleStrategyBehaviour):
    """
    Initial state of the one-shot Vehicle FSM.

    Entering the state marks the vehicle as ``VEHICLE_WAITING``. The state
    then attempts to start movement toward ``vehicle_dest``.

    Successful route planning transitions to
    ``VEHICLE_MOVING_TO_DESTINATION``. A PathRequestException keeps the FSM in
    the waiting state.
    """
    async def on_start(self):
        """
        Enter the waiting state and publish ``VEHICLE_WAITING`` locally.
        """
        await super().on_start()
        self.agent.status = VEHICLE_WAITING
        logger.debug("{} in Vehicle Waiting State".format(self.agent.jid))

    async def run(self):
        """
        Start the configured one-shot trip.

        The state's ``planned_trip()`` helper delegates route planning and movement
        creation to VehicleAgent.move_to().

        Route success advances to ``VEHICLE_MOVING_TO_DESTINATION``. A route
        planning failure returns to ``VEHICLE_WAITING``.
        """
        if self.agent.status != None and self.agent.status == VEHICLE_WAITING:

            try:
                logger.debug(
                    "Vehicle {} continue the trip".format(
                        self.agent.name
                    )
                )
                await self.planned_trip(self.agent.vehicle_dest)
                self.set_next_state(VEHICLE_MOVING_TO_DESTINATION)
                return
            except PathRequestException:
                logger.error(
                    "Transport {} could not get a path to customer. Cancelling...".format(
                        self.agent.name
                    )
                )
                self.set_next_state(VEHICLE_WAITING)
                return


class OneShotVehicleMovingState(VehicleStrategyBehaviour):
    """
    Poll movement progress for the one-shot Vehicle trip.

    While the vehicle has not reached its target, the FSM waits one second and
    remains in ``VEHICLE_MOVING_TO_DESTINATION``.

    Once the physical destination is reached, execution advances to
    ``VEHICLE_IN_DEST``.
    """
    async def on_start(self):
        """
        Enter the movement-monitoring state and mark the vehicle as moving.
        """
        await super().on_start()
        self.agent.status = VEHICLE_MOVING_TO_DESTINATION
        logger.debug("{} in Vehicle Moving State".format(self.agent.jid))

    async def run(self):
        """
        Check whether the active Vehicle movement has reached its destination.

        Movement itself is executed independently by MovableMixin's
        MovingBehaviour. This FSM state only monitors completion.

        While movement is incomplete, the state sleeps for one second before
        transitioning to itself. Arrival transitions to ``VEHICLE_IN_DEST``.

        Existing AlreadyInDestination and PathRequestException handlers are
        preserved for compatibility with the current state contract.
        """
        try:

            if not self.agent.is_in_destination():
                await asyncio.sleep(1)
                self.set_next_state(VEHICLE_MOVING_TO_DESTINATION)
            else:
                self.set_next_state(VEHICLE_IN_DEST)

        except AlreadyInDestination:
            logger.warning(
                "Vehicle {} has arrived to destination: {}.".format(
                    self.agent.agent_id, self.agent.is_in_destination()
                )
            )
            self.agent.status = VEHICLE_IN_DEST
            self.set_next_state(VEHICLE_IN_DEST)
            return
        except PathRequestException:
            logger.error(
                "Transport {} could not get a path to customer. Cancelling...".format(
                    self.agent.name
                )
            )
            self.agent.status = VEHICLE_WAITING
            self.set_next_state(VEHICLE_WAITING)
            return


class OneShotVehicleInDestState(VehicleStrategyBehaviour):
    """
    Terminal state of the one-shot Vehicle FSM.

    Entering the state marks the vehicle as ``VEHICLE_IN_DEST``. The state
    reports successful arrival and deliberately selects no next state, causing
    the one-shot FSM to finish.
    """
    async def on_start(self):
        """
        Enter the terminal state and mark the vehicle as in destination.
        """
        await super().on_start()
        self.agent.status = VEHICLE_IN_DEST
        logger.debug("{} in Vehicle Moving State".format(self.agent.jid))

    async def run(self):
        """
        Report arrival and allow the one-shot FSM to terminate.
        """
        logger.info("{} arrived at its destination".format(self.agent.jid))


class FSMOneShotVehicleBehaviour(FSMSimfleetBehaviour):
    """
    Finite-state strategy for executing one Vehicle trip.

    State flow:

    ``VEHICLE_WAITING``
        Plan movement toward the configured target.

    ``VEHICLE_MOVING_TO_DESTINATION``
        Monitor MovableMixin movement until arrival.

    ``VEHICLE_IN_DEST``
        Report arrival and terminate the FSM.

    Route-planning failure may return execution to ``VEHICLE_WAITING``.
    """
    def setup(self):
        """
        Register one-shot Vehicle states and allowed transitions.
        """
        # Create states
        self.add_state(VEHICLE_WAITING, OneShotVehicleWaitingState(), initial=True)
        self.add_state(VEHICLE_MOVING_TO_DESTINATION, OneShotVehicleMovingState())
        self.add_state(VEHICLE_IN_DEST, OneShotVehicleInDestState())

        # Create transitions
        self.add_transition(
            VEHICLE_WAITING, VEHICLE_WAITING
        )  # waiting for messages
        self.add_transition(
            VEHICLE_WAITING, VEHICLE_MOVING_TO_DESTINATION
        )  # accepted by customer

        self.add_transition(
            VEHICLE_MOVING_TO_DESTINATION, VEHICLE_MOVING_TO_DESTINATION
        )  # transport refused
        self.add_transition(
            VEHICLE_MOVING_TO_DESTINATION, VEHICLE_WAITING
        )  # transport refused
        self.add_transition(
            VEHICLE_MOVING_TO_DESTINATION, VEHICLE_IN_DEST
        )  # going to pick up customer


################################################################
#                                                              #
#                   Cycle Vehicle Strategy                     #
#                                                              #
################################################################

class CycleVehicleWaitingState(VehicleStrategyBehaviour):
    """
    Waiting and route-planning state of the cyclic Vehicle FSM.

    The state behaves like OneShotVehicleWaitingState: it marks the vehicle as
    waiting and starts movement toward the current ``vehicle_dest``.

    The difference between one-shot and cyclic operation occurs after arrival,
    not during route planning.
    """
    async def on_start(self):
        """
        Enter the cyclic waiting state and mark the vehicle as waiting.
        """
        await super().on_start()
        self.agent.status = VEHICLE_WAITING
        logger.debug("{} in Vehicle Waiting State".format(self.agent.jid))

    async def run(self):
        """
        Plan the current cyclic Vehicle trip.

        Successful planning advances to movement monitoring. Path-request failure
        keeps the FSM in the waiting state.
        """
        if self.agent.status != None and self.agent.status == VEHICLE_WAITING:
            try:
                logger.debug(
                    "Vehicle {} continue the trip".format(
                        self.agent.name
                    )
                )
                await self.planned_trip(self.agent.vehicle_dest)
                self.set_next_state(VEHICLE_MOVING_TO_DESTINATION)
                return
            except PathRequestException:
                logger.error(
                    "Transport {} could not get a path to customer. Cancelling...".format(
                        self.agent.name
                    )
                )
                self.set_next_state(VEHICLE_WAITING)
                return


class CycleVehicleMovingState(VehicleStrategyBehaviour):
    """
    Monitor physical movement during a cyclic Vehicle trip.

    MovableMixin performs the actual position updates. This state remains
    active while the destination has not been reached and transitions to
    ``VEHICLE_IN_DEST`` after arrival.
    """
    async def on_start(self):
        """
        Enter cyclic movement monitoring and mark the vehicle as moving.
        """
        await super().on_start()
        self.agent.status = VEHICLE_MOVING_TO_DESTINATION
        logger.debug("{} in Vehicle Moving State".format(self.agent.jid))

    async def run(self):
        """
        Monitor the current cyclic trip until destination or recovery.

        Incomplete movement remains in the same state after a one-second wait.
        Arrival advances to ``VEHICLE_IN_DEST``. Existing exception handlers may
        recover to ``VEHICLE_WAITING`` according to the current implementation.
        """
        try:

            if not self.agent.is_in_destination():
                await asyncio.sleep(1)
                self.set_next_state(VEHICLE_MOVING_TO_DESTINATION)
            else:
                self.set_next_state(VEHICLE_IN_DEST)

        except AlreadyInDestination:
            logger.warning(
                "Vehicle {} has arrived to destination: {}.".format(
                    self.agent.agent_id, self.agent.is_in_destination()
                )
            )
            self.agent.status = VEHICLE_IN_DEST
            self.set_next_state(VEHICLE_IN_DEST)
            return
        except PathRequestException:
            logger.error(
                "Transport {} could not get a path to customer. Cancelling...".format(
                    self.agent.name
                )
            )
            self.agent.status = VEHICLE_WAITING
            self.set_next_state(VEHICLE_WAITING)
            return


class CycleVehicleInDestState(VehicleStrategyBehaviour):
    """
    Arrival state of the cyclic Vehicle FSM.

    After reporting arrival, the vehicle generates a new target through
    ``VehicleAgent.set_target_position()`` without explicit coordinates,
    returns to ``VEHICLE_WAITING``, and begins another trip cycle.

    Unlike the one-shot arrival state, this state always schedules another
    FSM transition.
    """
    async def on_start(self):
        """
        Enter the cyclic arrival state and mark the vehicle as in destination.
        """
        await super().on_start()
        self.agent.status = VEHICLE_IN_DEST
        logger.debug("{} in Vehicle Moving State".format(self.agent.jid))

    async def run(self):
        """
        Report arrival, generate the next target, and return to waiting.
        """
        logger.info("{} arrived at its destination".format(self.agent.jid))

        logger.debug("{} processes a new destination address".format(self.agent.jid))
        self.agent.set_target_position()
        self.agent.status = VEHICLE_WAITING
        self.set_next_state(VEHICLE_WAITING)
        return

class FSMCycleVehicleBehaviour(FSMSimfleetBehaviour):
    """
    Finite-state strategy for repeatedly executing Vehicle trips.

    The FSM uses the same waiting and movement phases as the one-shot
    strategy. After reaching ``VEHICLE_IN_DEST``, however, a new target is
    generated and execution returns to ``VEHICLE_WAITING``.

    The cycle continues for the lifetime of the behaviour unless external
    lifecycle control stops it.
    """
    def setup(self):
        """
        Register cyclic Vehicle states and allowed transitions.
        """
        # Create states
        self.add_state(VEHICLE_WAITING, CycleVehicleWaitingState(), initial=True)
        self.add_state(VEHICLE_MOVING_TO_DESTINATION, CycleVehicleMovingState())
        self.add_state(VEHICLE_IN_DEST, CycleVehicleInDestState())

        # Create transitions
        self.add_transition(
            VEHICLE_WAITING, VEHICLE_WAITING
        )  # waiting for messages
        self.add_transition(
            VEHICLE_WAITING, VEHICLE_MOVING_TO_DESTINATION
        )  # accepted by customer

        self.add_transition(
            VEHICLE_MOVING_TO_DESTINATION, VEHICLE_MOVING_TO_DESTINATION
        )  # transport refused
        self.add_transition(
            VEHICLE_MOVING_TO_DESTINATION, VEHICLE_WAITING
        )  # transport refused
        self.add_transition(
            VEHICLE_MOVING_TO_DESTINATION, VEHICLE_IN_DEST
        )  # going to pick up customer
        self.add_transition(
            VEHICLE_IN_DEST, VEHICLE_WAITING
        )  # going to pick up customer
