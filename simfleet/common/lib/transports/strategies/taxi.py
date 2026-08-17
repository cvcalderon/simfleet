import json
import asyncio

from loguru import logger
from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from simfleet.common.lib.transports.models.taxi import TaxiStrategyBehaviour
from simfleet.communications.protocol import (
    REQUEST_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination
)
from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_MOVING_TO_CUSTOMER, \
    TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_IN_CUSTOMER_PLACE, TRANSPORT_MOVING_TO_DESTINATION, \
    TRANSPORT_ARRIVED_AT_DESTINATION, CUSTOMER_IN_TRANSPORT, CUSTOMER_IN_DEST, TRANSPORT_WAITING_FOR_RETURN , \
    TRANSPORT_MOVING_TO_RETURN


################################################################
#                                                              #
#                        Taxi Strategy                         #
#                                                              #
################################################################

class ElectricTaxiWaitingState(TaxiStrategyBehaviour):
    """
        Represents the 'Waiting' state for the electric taxi. The taxi is waiting to receive a transport request.

        Methods:
            on_start(): Sets the initial state to 'TRANSPORT_WAITING' and logs the state.
            run(): Handles incoming messages, processes transport requests, and transitions to the next state.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING
        #logger.debug("Agent[{}]: in Transport Waiting State".format(self.agent.jid))

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING)
            return
        logger.debug("Agent[{}]: The agent received: {}".format(self.agent.jid, msg.body))
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")
        if performative == REQUEST_PERFORMATIVE:

            # New statistics
            # Event 1: Customer Request Reception
            self.agent.events_store.emit(
                event_type="customer_request_reception",
                details={}
            )

            # New statistics
            # Event 2: Transport Offer
            self.agent.events_store.emit(
                event_type="transport_offer",
                details={}
            )

            await self.send_proposal(content["customer_id"], {})
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return

        else:
            self.set_next_state(TRANSPORT_WAITING)
            return


class TaxiWaitingForApprovalState(TaxiStrategyBehaviour):
    """
        Represents the state where the taxi is waiting for approval from a customer or station.
        After making a transport offer, the taxi waits for a response (approval or refusal).

        Methods:
            on_start(): Logs the transition to 'Waiting For Approval'.
            run(): Handles incoming approval or refusal messages and transitions accordingly.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING_FOR_APPROVAL
        #logger.debug(
        #    "{} in Transport Waiting For Approval State".format(self.agent.jid)
        #)

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            # Handle acceptance by the customer or station
            try:
                logger.debug(
                    "Agent[{}]: The agent got accept from [{}]".format(
                        self.agent.name, content["customer_id"]
                    )
                )

                # New statistics
                # Event 3: Transport Offer Acceptance
                self.agent.events_store.emit(
                    event_type="transport_offer_acceptance",
                    details={}
                )

                await self.inform_customer(
                    customer_id=content["customer_id"], status=TRANSPORT_MOVING_TO_CUSTOMER
                )

               # await self.agent.add_assigned_taxicustomer(
               #     customer_id=content["customer_id"],
               #     origin=content["origin"], dest=content["dest"]
               # )

                await self.assigned_taxicustomer(
                    customer_id=content["customer_id"],
                    origin=content["origin"], dest=content["dest"]
                )

                # New statistics - TESTING
                path, distance, duration = await self.agent.request_path(
                    self.agent.get("current_pos"), content["origin"]
                )

                # New statistics
                # Event 4: Travel to Pickup
                self.agent.events_store.emit(
                    event_type="travel_to_pickup",
                    details={"distance": distance, "duration": duration}
                )

                await self.agent.move_to(content["origin"])

                self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER
                self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
                return


            except PathRequestException:
                logger.error(
                    "Agent[{}]: The agent could not get a path to customer [{}]. Cancelling...".format(
                        self.agent.name, content["customer_id"]
                    )
                )

                await self.cancel_proposal(content["customer_id"])
                self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING)
                return

            except AlreadyInDestination:

                await self.inform_customer(
                    customer_id=content["customer_id"], status=TRANSPORT_IN_CUSTOMER_PLACE
                )
                self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
                self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
                return

            except Exception as e:
                logger.error(
                    "Unexpected error in agent [{}]: {}".format(
                        self.agent.name, e
                    )
                )

                await self.cancel_proposal(content["customer_id"])
                self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING)
                return

        elif performative == REFUSE_PERFORMATIVE:
            logger.debug(
                "Agent[{}]: The agent got refusal from customer/station".format(self.agent.name)
            )
            self.set_next_state(TRANSPORT_WAITING)
            return

        else:
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return

class TaxiMovingToCustomerState(TaxiStrategyBehaviour):
    """
        Represents the state where the taxi is moving towards the customer to pick them up.

        Methods:
            on_start(): Logs the transition to 'Moving To Customer'.
            run(): Handles the movement to the customer and manages unexpected issues during the trip.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER
        logger.debug("{} in Transport Moving To Customer State".format(self.agent.jid))

    async def run(self):

        customers = self.get("assigned_customer")
        customer_id = next(iter(customers.items()))[0]

        try:

            if not self.agent.is_in_destination():

                msg = await self.receive(timeout=2)

                if msg:

                    performative = msg.get_metadata("performative")

                    if performative == REQUEST_PERFORMATIVE:
                        self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
                        return

                    elif performative == REFUSE_PERFORMATIVE:
                        logger.debug(
                            "Agent[{}]: The agent got refusal from customer/station".format(
                                self.agent.name
                            )
                        )

                        self.agent.remove_assigned_customer()
                        self.agent.status = TRANSPORT_WAITING
                        self.agent.set_available()
                        self.set_next_state(TRANSPORT_WAITING)
                        return

                else:
                    self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
            else:
                logger.info(
                    "Agent[{}]: The agent has arrived to destination. Status: {}".format(
                        self.agent.agent_id, self.agent.status
                    )
                )
                await self.inform_customer(
                    customer_id=customer_id, status=TRANSPORT_IN_CUSTOMER_PLACE
                )
                self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
                self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
                return

        except PathRequestException:
            logger.error(
                "Agent[{}]: The agent could not get a path to customer [{}]. Cancelling...".format(
                    self.agent.name, customer_id
                )
            )
            await self.cancel_proposal(customer_id)
            self.agent.remove_assigned_customer()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        except AlreadyInDestination:

            await self.inform_customer(
                customer_id=customer_id, status=TRANSPORT_IN_CUSTOMER_PLACE
            )
            self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
            return
        except Exception as e:
            logger.error(
                "Unexpected error in transport [{}]: {}".format(self.agent.name, e)
            )
            await self.cancel_proposal(customer_id)
            self.agent.remove_assigned_customer()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return


class TaxiArrivedAtCustomerState(TaxiStrategyBehaviour):
    """
        Represents the state where the taxi has arrived at the customer's location.

        Methods:
            on_start(): Logs the transition to 'Arrived At Customer'.
            run(): Handles the pickup of the customer and begins the journey to their destination.
        """

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
        #logger.debug("{} in Transport Arrived At Customer State".format(self.agent.jid))

    async def run(self):

        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
            return
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")

        if performative == INFORM_PERFORMATIVE:
            if "status" in content:
                status = content["status"]

                if status == CUSTOMER_IN_TRANSPORT:

                    customers = self.get("assigned_customer")
                    customer_id = next(iter(customers.items()))[0]
                    dest = next(iter(customers.items()))[1]["destination"]

                    try:
                        logger.debug(
                            "Agent[{}]: Customer [{}] in transport.".format(self.agent.name, customer_id)
                        )

                        self.agent.add_customer_in_transport(
                            customer_id=customer_id, dest=dest
                        )
                        #await self.agent.remove_assigned_taxicustomer()

                        await self.unassigned_taxicustomer()

                        logger.info(
                            "Agent[{}]: The agent on route to [{}] destination.".format(self.agent.name, customer_id)
                        )

                        # New statistics
                        # Event 5: Customer Pickup
                        self.agent.events_store.emit(
                            event_type="customer_pickup",
                            details={},
                        )

                        # New statistics - TESTING
                        path, distance, duration = await self.agent.request_path(
                            self.agent.get("current_pos"), dest
                        )

                        await self.agent.move_to(dest)

                        # New statistics
                        # Event 6: Travel to destination
                        self.agent.events_store.emit(
                            event_type="travel_to_destination",
                            details={"distance": distance},
                        )

                        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
                        self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)

                    except PathRequestException:

                        await self.cancel_customer(customer_id=customer_id)

                        if customer_id in self.agent.get("current_customer"):
                            self.agent.remove_customer_in_transport(customer_id)

                        self.agent.status = TRANSPORT_WAITING
                        self.agent.set_available()
                        self.set_next_state(TRANSPORT_WAITING)
                        return

                    except AlreadyInDestination:
                        self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION
                        self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
                        return

                    except Exception as e:

                        logger.error(
                            "Unexpected error in transport [{}]: {}".format(
                                self.agent.name, e
                            )
                        )

                        if customer_id in self.agent.get("current_customer"):
                            self.agent.remove_customer_in_transport(customer_id)

                        self.agent.status = TRANSPORT_WAITING
                        self.agent.set_available()
                        self.set_next_state(TRANSPORT_WAITING)
                        return


        elif performative == CANCEL_PERFORMATIVE:

            if self.agent.get("assigned_customer"):
                self.agent.remove_assigned_customer()

            current_customers = self.agent.get("current_customer")

            for customer_id in list(current_customers.keys()):
                self.agent.remove_customer_in_transport(customer_id)

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)

            return
        else:
            self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
            return

# MOD-STRATEGY-04 - New status
class TaxiMovingToCustomerDestState(TaxiStrategyBehaviour):
    """
        Represents the state where the taxi is transporting the customer to their destination.

        Methods:
            on_start(): Logs the transition to 'Moving To Destination'.
            run(): Manages the trip to the customer's destination.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
        #logger.debug("{} in Transport Moving To Customer Dest State".format(self.agent.jid))

    async def run(self):

        customers = self.get("current_customer")
        customer_id = next(iter(customers.items()))[0]

        try:

            if not self.agent.is_in_destination():
                await asyncio.sleep(1)
                self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)
            else:
                logger.info(
                    "Agent[{}]: The agent has arrived to destination. Status: {}".format(
                        self.agent.agent_id, self.agent.status
                    )
                )

                # New statistics
                # Event 6: Trip completion
                self.agent.events_store.emit(
                    event_type="trip_completion",
                    details={},
                )

                await self.inform_customer(
                    customer_id=customer_id, status=CUSTOMER_IN_DEST
                )
                self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION
                self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)

        except PathRequestException:
            logger.error(
                "Agent[{}]: The agent could not get a path to customer [{}]. Cancelling...".format(
                    self.agent.name, customer_id
                )
            )
            await self.cancel_proposal(customer_id)

            if customer_id in self.agent.get("current_customer"):
                self.agent.remove_customer_in_transport(customer_id)

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        except AlreadyInDestination:

            # New statistics
            # Event 6: Trip completion
            self.agent.events_store.emit(
                event_type="trip_completion",
                details={},
            )

            await self.inform_customer(
                customer_id=customer_id, status=CUSTOMER_IN_DEST
            )
            self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION
            self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
            return

        except Exception as e:
            logger.error(
                "Unexpected error in transport [{}]: {}".format(
                    self.agent.name, e
                )
            )

            await self.cancel_proposal(customer_id)

            if customer_id in self.agent.get("current_customer"):
                self.agent.remove_customer_in_transport(customer_id)

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

class TaxiArrivedAtCustomerDestState(TaxiStrategyBehaviour):
    """
        Represents the state where the taxi has arrived at the customer's destination.

        Methods:
            on_start(): Logs the transition to 'Arrived At Destination'.
            run(): Handles the process of dropping the customer off and resets the taxi to 'Waiting' state.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION
        #logger.debug("{} in Transport Arrived at Customer Dest State".format(self.agent.jid))

    async def run(self):

        customers = self.get("current_customer")
        customer_id = next(iter(customers.items()))[0]

        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
            return
        else:
            content = json.loads(msg.body)
            performative = msg.get_metadata("performative")

            if performative == INFORM_PERFORMATIVE:
                if "status" in content:
                    status = content["status"]

                    if status == CUSTOMER_IN_DEST:

                        self.agent.remove_customer_in_transport(customer_id)

                        self.agent.increment_completed_assignments()
                        #self.agent.set_available()
                        self.agent.set_busy()

                        logger.debug(
                            "Agent[{}]: The agent has completed the service for customer [{}].".format(
                                self.agent.agent_id, customer_id
                            )
                        )
                        self.agent.status = TRANSPORT_WAITING_FOR_RETURN
                        self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
                        return


            elif performative == CANCEL_PERFORMATIVE:

                if customer_id in self.agent.get("current_customer"):
                    self.agent.remove_customer_in_transport(customer_id)

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING)

                return
            else:
                self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
                return

class TaxiWaitingForReturnState(TaxiStrategyBehaviour):
    """
    Represents the state where the taxi waits for a return point
    assigned by the FleetManager.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_WAITING_FOR_RETURN
        self.return_requested = False

    async def run(self):

        if self.agent.get_return_position():
            self.set_next_state(TRANSPORT_MOVING_TO_RETURN)
            return

        if not self.return_requested:
            await self.request_return_position()
            self.return_requested = True

        msg = await self.receive(timeout=5)

        if not msg:
            self.return_requested = False
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        performative = msg.get_metadata("performative")

        if performative != INFORM_PERFORMATIVE:
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: Invalid return point response.".format(
                    self.agent.name
                )
            )

            self.return_requested = False
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        if content.get("request_type") != "taxi_return":
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        return_position = content.get("return_position")

        if return_position is None:
            self.return_requested = False
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        self.agent.set_return_position(
            return_position
        )

        logger.info(
            "Agent[{}]: Return position received: {}".format(
                self.agent.name,
                return_position
            )
        )

        try:
            await self.agent.move_to(
                return_position
            )

            self.agent.status = TRANSPORT_MOVING_TO_RETURN
            self.set_next_state(
                TRANSPORT_MOVING_TO_RETURN
            )
            return

        except AlreadyInDestination:
            logger.info(
                "Agent[{}]: The taxi is already at the return point {}.".format(
                    self.agent.name,
                    return_position
                )
            )

            self.agent.clear_return_position()
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        except PathRequestException:
            logger.error(
                "Agent[{}]: The taxi could not get a path to return point {}.".format(
                    self.agent.name,
                    return_position
                )
            )

            self.agent.clear_return_position()

            self.return_requested = False

            self.agent.status = TRANSPORT_WAITING_FOR_RETURN

            self.set_next_state(
                TRANSPORT_WAITING_FOR_RETURN
            )
            return

        except Exception as e:
            logger.error(
                "Unexpected error returning taxi [{}]: {}".format(
                    self.agent.name,
                    e
                )
            )

            self.agent.clear_return_position()
            self.return_requested = False
            self.agent.status = TRANSPORT_WAITING_FOR_RETURN

            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return


class TaxiMovingToReturnState(TaxiStrategyBehaviour):
    """
    Represents the state where the taxi moves to its assigned
    return point before becoming available again.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_MOVING_TO_RETURN

    async def run(self):

        return_position = self.agent.get_return_position()

        if return_position is None:
            logger.warning(
                "Agent[{}]: No return position available while returning.".format(
                    self.agent.name
                )
            )

            self.agent.status = TRANSPORT_WAITING_FOR_RETURN

            self.set_next_state(
                TRANSPORT_WAITING_FOR_RETURN
            )
            return

        if not self.agent.is_in_destination():

            await asyncio.sleep(1)

            self.set_next_state(
                TRANSPORT_MOVING_TO_RETURN
            )
            return

        logger.info(
            "Agent[{}]: The taxi has arrived at return point {}.".format(
                self.agent.name,
                return_position
            )
        )

        self.agent.clear_return_position()

        self.agent.status = TRANSPORT_WAITING
        self.agent.set_available()

        self.set_next_state(
            TRANSPORT_WAITING
        )
        return


class FSMTaxiBehaviour(FSMSimfleetBehaviour):
    """
    Represents the Finite State Machine (FSM) strategy for the electric taxi agent.
    This class manages the different states and transitions for the taxi based on its behavior,
    including waiting for customers, moving to charging stations, and traveling to destinations.

    Methods:
        setup(): Initializes all states and defines transitions between them.
    """

    def setup(self):
        """
        Sets up the FSM by adding states and defining transitions.
        This method creates the states the electric taxi can be in and
        specifies the valid transitions between these states.
        """

        # Add states to the FSM
        self.add_state(TRANSPORT_WAITING, ElectricTaxiWaitingState(), initial=True)
        self.add_state(TRANSPORT_WAITING_FOR_APPROVAL, TaxiWaitingForApprovalState())
        self.add_state(TRANSPORT_MOVING_TO_CUSTOMER, TaxiMovingToCustomerState())
        self.add_state(TRANSPORT_ARRIVED_AT_CUSTOMER, TaxiArrivedAtCustomerState())
        self.add_state(TRANSPORT_MOVING_TO_DESTINATION, TaxiMovingToCustomerDestState())
        self.add_state(TRANSPORT_ARRIVED_AT_DESTINATION, TaxiArrivedAtCustomerDestState())
        self.add_state(TRANSPORT_WAITING_FOR_RETURN, TaxiWaitingForReturnState())
        self.add_state(TRANSPORT_MOVING_TO_RETURN, TaxiMovingToReturnState())

        # Define transitions between states

        # Transitions related to the 'Waiting' state
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING)  # Remains in waiting if no new action
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING_FOR_APPROVAL)  # When a customer accepts a proposal

        # Transitions from 'Waiting For Approval' state
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_WAITING_FOR_APPROVAL)  # Keep waiting for approval
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_WAITING)  # If the proposal is refused
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_MOVING_TO_CUSTOMER)  # If the customer accepts
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_ARRIVED_AT_CUSTOMER)  # Direct arrival scenario

        # Transitions from 'Moving To Customer' state
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_MOVING_TO_CUSTOMER)  # Still moving
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_WAITING)  # Encounter an issue, go back to waiting
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_ARRIVED_AT_CUSTOMER)  # Successfully arrive

        # Transitions from 'Arrived At Customer' state
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_ARRIVED_AT_CUSTOMER)  # Waiting at customer's location
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_MOVING_TO_DESTINATION)  # Begin journey to destination
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_ARRIVED_AT_DESTINATION)  # Direct destination arrival
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_WAITING)  # Cancel and return to waiting

        # Transitions from 'Moving To Destination' state
        self.add_transition(TRANSPORT_MOVING_TO_DESTINATION, TRANSPORT_MOVING_TO_DESTINATION)  # Still moving to destination
        self.add_transition(TRANSPORT_MOVING_TO_DESTINATION, TRANSPORT_WAITING)  # An issue encountered, return to waiting
        self.add_transition(TRANSPORT_MOVING_TO_DESTINATION, TRANSPORT_ARRIVED_AT_DESTINATION)  # Arrival at destination

        # Transitions from 'Arrived At Destination' state
        self.add_transition(TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_ARRIVED_AT_DESTINATION)  # Stay at destination
        self.add_transition(TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_WAITING)  # Drop customer and return to waiting
        self.add_transition(TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_MOVING_TO_RETURN)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_MOVING_TO_RETURN)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_WAITING)


        # Additional transitions for customer movement and destination states
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_MOVING_TO_CUSTOMER)  # Still en route to customer
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_WAITING)  # Return to waiting if issue arises
