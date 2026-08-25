import json

from loguru import logger
from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from spade.behaviour import State
from spade.message import Message

#from simfleet.common.lib.transports.models.electrictaxi import ElectricTaxiStrategyBehaviour
from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    PROPOSE_PERFORMATIVE
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination
)
from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_MOVING_TO_CUSTOMER, \
    TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_IN_CUSTOMER_PLACE, TRANSPORT_MOVING_TO_DESTINATION, \
    TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_MOVING_TO_STATION, TRANSPORT_IN_STATION_PLACE, \
    TRANSPORT_IN_WAITING_LIST, TRANSPORT_NEEDS_CHARGING, TRANSPORT_CHARGING, CUSTOMER_IN_TRANSPORT, CUSTOMER_IN_DEST, \
    TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_MOVING_TO_RETURN


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class ElectricTaxiStrategyBehaviour(State):
    """
    Base class to define the transport strategy for an electric taxi.
    This class should be inherited and extended to create custom strategies.
    Subclasses must override the `run` coroutine to define specific behaviors.

    Methods:
        async on_start():
            Logs the beginning of the strategy execution.
        async on_end():
            Logs the end of the strategy execution.
        async go_to_the_station(station_id, dest):
            Directs the taxi to a specific station and updates autonomy based on distance.
        check_and_decrease_autonomy(customer_orig, customer_dest):
            Checks if there is enough autonomy for a trip and decreases it if possible.
        async drop_station():
            Resets the current station assignment for the taxi.
        async request_access_station(station_id, content):
            Sends a request to a station for access.
        async send_proposal(customer_id, content=None):
            Sends a transport proposal to a customer.
        async cancel_proposal(agent_id, content=None):
            Cancels a previously sent proposal to a customer.
        async run():
            Abstract method that must be implemented by subclasses.
    """

    async def on_start(self):
        """
                Logs the beginning of the strategy execution.
                """
        # await super().on_start()
        logger.debug(
            "Agent[{}]: Strategy {} started.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def on_end(self):
        """
                Logs the end of the strategy execution.
                """
        # await super().on_start()
        logger.debug(
            "Agent[{}]: Strategy {} finished.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def go_to_the_station(self, station_id, dest):
        """
                Directs the taxi to a specific station and updates autonomy based on the distance.

                Args:
                    station_id (str): The ID of the destination station.
                    dest (list): The coordinates of the station (x, y).
                """
        logger.info(
            "Agent[{}]: On route to station [{}]".format(
                self.agent.name,
                station_id
            )
        )

        self.agent.set_current_station(
            station_id
        )



    def check_and_decrease_autonomy(self, customer_orig, customer_dest):
        """
        Verifies if the ttransport has enough autonomy for a trip and decreases autonomy if possible.

        Args:
            customer_orig (list): The customer's origin coordinates (x, y).
            customer_dest (list): The customer's destination coordinates (x, y).

        Returns:
            bool: True if autonomy is sufficient and decreased, False otherwise.
        """

        if self.agent.has_enough_autonomy(customer_orig, customer_dest):
            autonomy = self.agent.get_autonomy()
            travel_km = self.agent.calculate_km_expense(
                self.agent.get_position(), customer_orig, customer_dest
            )
            self.agent.decrease_autonomy_km(travel_km)
            return True
        else:
            return False

    async def drop_station(self):
        """
        Resets the current station assignment for the transport.
        """

        logger.debug(
            "Agent[{}]: The agent has dropped the station [{}].".format(
                self.agent.agent_id,
                self.agent.get_current_station()
            )
        )

        self.agent.clear_current_station()
        self.agent.clear_nearby_station()

    async def request_access_station(self, station_id, content):

        """
                Sends a request to a station for access.

                Args:
                    station_id (str): The ID of the station to request access from.
                    content (dict): Additional information to include in the request.
                """

        if content is None:
            content = {}
        reply = Message()
        reply.to = station_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", REQUEST_PERFORMATIVE)
        reply.body = json.dumps(content)
        logger.debug(
            "Agent[{}]: The agent requesting access to [{}]".format(
                self.agent.name,
                station_id,
                reply.body
            )
        )
        await self.send(reply)

    async def send_proposal(self, customer_id, content=None):
        """
        Sends a proposal to a customer offering transport.

        Args:
            customer_id (str): The ID of the customer.
            content (dict, optional): Additional content for the proposal. Defaults to None.
        """
        if content is None:
            content = {}
        logger.info(
            "Agent[{}]: The agent sent proposal to agent [{}]".format(self.agent.name, customer_id)
        )
        reply = Message()
        reply.to = customer_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", PROPOSE_PERFORMATIVE)
        reply.body = json.dumps(content)
        await self.send(reply)

    async def cancel_proposal(self, agent_id, content=None):
        """
        Cancels a previously sent proposal.

        Args:
            agent_id (str): The ID of the customer.
            content (dict, optional): Additional content for the cancellation. Defaults to None.
        """
        if content is None:
            content = {}
        logger.info(
            "Agent[{}]: The agent sent cancel proposal to agent [{}]".format(
                self.agent.name, agent_id
            )
        )
        reply = Message()
        reply.to = agent_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", CANCEL_PERFORMATIVE)
        reply.body = json.dumps(content)
        await self.send(reply)

    async def inform_customer(self, customer_id, status, data=None):
        """
        Sends a message to inform the customer of the transport's new status.

        Args:
            customer_id (str): The ID of the customer.
            status (int): The new status code.
            data (dict, optional): Additional information about the status.
        """
        if data is None:
            data = {}
        msg = Message()
        msg.to = customer_id
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        data["status"] = status
        msg.body = json.dumps(data)
        await self.send(msg)

    async def cancel_customer(self, customer_id, data=None):
        """
        Cancels the assignment of a customer and informs them via a message.

        Args:
            customer_id (str): The ID of the customer.
            data (dict, optional): Additional cancellation-related information.
        """
        logger.error(
            "Agent[{}]: The agent could not get a path to customer [{}].".format(
                self.agent.agent_id, self.agent.get("current_customer")
            )
        )
        if data is None:
            data = {}
        reply = Message()
        reply.to = customer_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", CANCEL_PERFORMATIVE)
        reply.body = json.dumps(data)
        logger.debug(
            "Agent[{}]: The agent sent cancel proposal to customer [{}]".format(
                self.agent.agent_id, customer_id
            )
        )
        await self.send(reply)

    async def request_return_position(self):
        fleetmanager = self.agent.get_registration_fleet()

        if not fleetmanager:
            logger.warning(
                "Agent[{}]: No fleet manager configured for electric taxi return.".format(
                    self.agent.name
                )
            )
            return

        msg = Message()

        msg.to = str(fleetmanager)
        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        msg.set_metadata(
            "performative",
            REQUEST_PERFORMATIVE
        )

        msg.body = json.dumps(
            {
                "request_type": "taxi_return",
                "position": self.agent.get_position(),
            }
        )

        logger.debug(
            "Agent[{}]: Requesting return point from [{}] at position {}.".format(
                self.agent.name,
                fleetmanager,
                self.agent.get_position()
            )
        )

        await self.send(msg)

    async def run(self):
        raise NotImplementedError

# ==================================================================
# -------------------------End Behaviour----------------------------
# ==================================================================


################################################################
#                                                              #
#              Point Of Return Electric Taxi Strategy          #
#                                                              #
################################################################

class ElectricTaxiWaitingState(ElectricTaxiStrategyBehaviour):
    """
        Represents the 'Waiting' state for the electric taxi. The taxi is waiting to receive a transport request.

        Methods:
            on_start(): Sets the initial state to 'TRANSPORT_WAITING' and logs the state.
            run(): Handles incoming messages, processes transport requests, and transitions to the next state.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING

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

            if not self.agent.has_enough_autonomy_for_service(
                content["origin"],
                content["dest"]
            ):

                # New statistics
                # Event 1e: Need for Service
                self.agent.events_store.emit(
                    event_type="transport_need_for_service",
                    details={}
                )

                await self.cancel_proposal(content["customer_id"])
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return
            else:

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

class ElectricTaxiNeedsChargingState(ElectricTaxiStrategyBehaviour):
    """
        Represents the 'Needs Charging' state. The taxi is searching for or moving to a charging station.

        Methods:
            on_start(): Logs the transition to the 'Needs Charging' state.
            run(): Manages the behavior of the taxi while it finds a charging station and handles exceptions.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_NEEDS_CHARGING
        self.agent.set_busy()

    async def run(self):

        if (
            self.agent.get_stations() is None
            or self.agent.get_number_stations() < 1
        ):
            logger.info(
                "Agent[{}]: The agent looking for a station.".format(
                    self.agent.name
                )
            )

            # New
            stations = await self.agent.get_list_agent_position(
                self.agent.service_type,
                self.agent.get_stations()
            )
            self.agent.set_stations(stations)
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        else:
            nearby_station_dest = self.agent.nearst_agent(
                self.agent.get_stations(),
                self.agent.get_position()
            )

            if nearby_station_dest is None:
                logger.warning(
                    "Agent[{}]: No charging station available.".format(
                        self.agent.name
                    )
                )
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return

            self.agent.set_nearby_station(nearby_station_dest)
            station_id = self.agent.get_nearby_station_id()
            station_position = self.agent.get_nearby_station_position()

            logger.info(
                "Agent[{}]: The agent selected station [{}].".format(
                    self.agent.name,
                    station_id
                )
            )

            try:
                await self.go_to_the_station(
                    station_id,
                    station_position
                )

                travel_km = self.agent.calculate_distance_km(
                    self.agent.get_position(),
                    station_position
                )

                try:
                    (
                        distance,
                        osrm_duration,
                        _speed_based_duration,
                    ) = await self.agent.move_to(
                        station_position
                    )

                    self.agent.decrease_autonomy_km(
                        travel_km
                    )

                    self.agent.events_store.emit(
                        event_type="travel_to_station",
                        details={
                            "distance": distance,
                            "duration": _speed_based_duration,
                        },
                    )

                    self.agent.status = TRANSPORT_MOVING_TO_STATION
                    self.set_next_state(TRANSPORT_MOVING_TO_STATION)
                    return

                except AlreadyInDestination:
                    logger.debug(
                        "Agent[{}]: The agent is already in the stations' ({}) position. . .".format(
                            self.agent.name, self.agent.get_nearby_station_id()
                        )
                    )

                    arguments = {
                        "transport_need":
                            self.agent.max_autonomy_km
                            - self.agent.current_autonomy_km
                    }

                    content = {
                        "service_name": self.agent.service_type,
                        "object_type": "transport",
                        "args": arguments
                    }
                    await self.request_access_station(
                        station_id,
                        content
                    )

                    self.agent.events_store.emit(
                        event_type="arrival_at_station",
                        details={}
                    )

                    self.agent.status = TRANSPORT_IN_STATION_PLACE
                    self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                    return

                return


            except PathRequestException:
                logger.error(
                    "Agent[{}]: The agent could not get a path to station [{}].".format(
                        self.agent.name,
                        self.agent.get_nearby_station_id()
                    )
                )
                await self.drop_station()
                self.agent.status = TRANSPORT_NEEDS_CHARGING
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return
            except Exception as e:
                logger.error(
                    "Unexpected error in transport [{}]: {}".format(self.agent.name, e)
                )
                await self.drop_station()
                self.agent.status = TRANSPORT_NEEDS_CHARGING
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return


class ElectricTaxiMovingToStationState(ElectricTaxiStrategyBehaviour):
    """
        Represents the state where the taxi is moving towards the charging station.

        Methods:
            on_start(): Logs the transition to 'Moving to Station'.
            run(): Handles movement towards the charging station and transitions accordingly.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_STATION

    async def run(self):
        try:

            if not self.agent.is_in_destination():

                await self.agent.sleep(1)
                self.set_next_state(TRANSPORT_MOVING_TO_STATION)
            else:

                arguments = {
                    "transport_need":
                        self.agent.max_autonomy_km
                        - self.agent.current_autonomy_km
                }

                # New statistics
                # Event 3e: Arrival at Station
                self.agent.events_store.emit(
                    event_type="arrival_at_station",
                    details={}
                )

                content = {
                    "service_name": self.agent.service_type,
                    "object_type": "transport",
                    "args": arguments
                }
                await self.request_access_station(
                    self.agent.get_current_station(),
                    content
                )

                self.agent.status = TRANSPORT_IN_STATION_PLACE
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)

        except AlreadyInDestination:
            logger.warning(
                "Agent[{}]: The agent has arrived to destination.".format(
                    self.agent.agent_id
                )
            )

            arguments = {
                "transport_need":
                    self.agent.max_autonomy_km
                    - self.agent.current_autonomy_km
            }

            content = {
                "service_name": self.agent.service_type,
                "object_type": "transport",
                "args": arguments
            }

            await self.request_access_station(
                self.agent.get_current_station(),
                content
            )

            # New statistics
            # Event 3e: Arrival at Station
            self.agent.events_store.emit(
                event_type="arrival_at_station",
                details={}
            )

            self.agent.status = TRANSPORT_IN_STATION_PLACE
            self.set_next_state(TRANSPORT_IN_STATION_PLACE)
            return
        except PathRequestException:
            logger.error(
                "Agent[{}]: The agent could not get a path to customer. Cancelling...".format(
                    self.agent.name
                )
            )
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return


class ElectricTaxiInStationState(ElectricTaxiStrategyBehaviour):
    """
        Represents the state where the taxi is at the charging station, waiting for confirmation to begin charging.

        Methods:
            on_start(): Logs the transition to 'In Station Place'.
            run(): Handles the taxi's behavior while waiting in the station queue.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_IN_STATION_PLACE

    async def run(self):

        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_IN_STATION_PLACE)
            return
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            if content.get("station_id") is not None:
                logger.debug(
                    "Agent[{}]: The agent received a message with ACCEPT_PERFORMATIVE from [{}]".format(
                        self.agent.name, content["station_id"]
                    )
                )

                # New statistics
                # Event 4e: Wait for Service
                self.agent.events_store.emit(
                    event_type="wait_for_service",
                    details={}
                )

                self.agent.status = TRANSPORT_IN_WAITING_LIST
                self.set_next_state(TRANSPORT_IN_WAITING_LIST)

                return


        elif performative == REFUSE_PERFORMATIVE:
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

        else:

            self.set_next_state(TRANSPORT_IN_STATION_PLACE)
            return


class ElectricTaxiInWaitingListState(ElectricTaxiStrategyBehaviour):
    """
        Represents the state where the taxi is in the waiting list at the charging station.

        Methods:
            on_start(): Logs the transition to 'In Waiting List'.
            run(): Handles waiting for confirmation to start charging.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_IN_WAITING_LIST

    async def run(self):

        msg = await self.receive(timeout=5)
        if not msg:
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")

        if performative == INFORM_PERFORMATIVE:
            if content.get("station_id") is not None:
                logger.debug(
                    "Agent[{}]: The agent received a message with INFORM_PERFORMATIVE from [{}]".format(
                        self.agent.name, content["station_id"]
                    )
                )
                if content.get("serving") is not None and content.get("serving"):

                    # New statistics
                    # Event 5e: Service Start
                    self.agent.events_store.emit(
                        event_type="service_start",
                        details={}
                    )

                    self.agent.status = TRANSPORT_CHARGING
                    self.set_next_state(TRANSPORT_CHARGING)
                    return

        elif performative == REFUSE_PERFORMATIVE:
            if content.get("station_id") is not None:
                logger.debug(
                    "Agent[{}]: The agent received a message with REFUSE_PERFORMATIVE from [{}]".format(
                        self.agent.name, content["station_id"]
                    )
                )
                await self.drop_station()
                self.agent.status = TRANSPORT_NEEDS_CHARGING
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return

        else:
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return


class ElectricTaxiChargingState(ElectricTaxiStrategyBehaviour):
    """
        Represents the 'Charging' state. The taxi is currently charging in the station.

        Methods:
            on_start(): Logs the transition to 'Charging'.
            run(): Monitors the charging process and transitions back to 'Waiting' when charging completes.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_CHARGING

    async def run(self):

        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_CHARGING)
            return
        content = json.loads(msg.body)
        protocol = msg.get_metadata("protocol")
        performative = msg.get_metadata("performative")
        if protocol == REQUEST_PROTOCOL and performative == INFORM_PERFORMATIVE:
            if content["charged"]:
                self.agent.increase_full_autonomy_km()
                await self.drop_station()

                # New statistics
                # Event 6e: Service Completion
                self.agent.events_store.emit(
                    event_type="service_completion",
                    details={}
                )

                if self.agent.has_return_position():
                    logger.info(
                        "Agent[{}]: Charging completed. "
                        "Continuing pending return to {}.".format(
                            self.agent.name,
                            self.agent.get_return_position()
                        )
                    )
                    self.agent.set_busy()
                    self.agent.status = TRANSPORT_WAITING_FOR_RETURN
                    self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
                    return

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING)
                return

        else:
            self.set_next_state(TRANSPORT_CHARGING)
            return


class ElectricTaxiWaitingForApprovalState(ElectricTaxiStrategyBehaviour):
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
                if not self.check_and_decrease_autonomy(
                    content["origin"], content["dest"]
                ):
                    await self.cancel_proposal(content["customer_id"])
                    self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                    return
                else:

                    # New statistics
                    # Event 3: Transport Offer Acceptance
                    self.agent.events_store.emit(
                        event_type="transport_offer_acceptance",
                        details={}
                    )

                    await self.inform_customer(
                        customer_id=content["customer_id"], status=TRANSPORT_MOVING_TO_CUSTOMER
                    )

                    #await self.agent.add_assigned_taxicustomer(
                    #    customer_id=content["customer_id"],
                    #    origin=content["origin"], dest=content["dest"]
                    #)

                    self.agent.add_assigned_customer(
                        customer_id=content["customer_id"],
                        origin=content["origin"],
                        dest=content["dest"]
                    )

                    self.agent.set_busy()

                    (
                        distance,
                        osrm_duration,
                        _speed_based_duration,
                    ) = await self.agent.move_to(
                        content["origin"]
                    )

                    self.agent.events_store.emit(
                        event_type="travel_to_pickup",
                        details={
                            "distance": distance,
                            "duration": _speed_based_duration,
                        },
                    )

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
                    "Unexpected error in transport [{}]: {}".format(self.agent.name, e)
                )
                await self.cancel_proposal(content["customer_id"])
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

class ElectricTaxiMovingToCustomerState(ElectricTaxiStrategyBehaviour):
    """
        Represents the state where the taxi is moving towards the customer to pick them up.

        Methods:
            on_start(): Logs the transition to 'Moving To Customer'.
            run(): Handles the movement to the customer and manages unexpected issues during the trip.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER

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
                            "Agent[{}]: The agent got refusal from customer/station".format(self.agent.name)
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
            self.agent.status = TRANSPORT_WAITING
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
            self.agent.status = TRANSPORT_WAITING
            self.set_next_state(TRANSPORT_WAITING)
            return


class ElectricTaxiArrivedAtCustomerState(ElectricTaxiStrategyBehaviour):
    """
        Represents the state where the taxi has arrived at the customer's location.

        Methods:
            on_start(): Logs the transition to 'Arrived At Customer'.
            run(): Handles the pickup of the customer and begins the journey to their destination.
        """

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER

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
                        self.agent.remove_assigned_customer()

                        logger.info(
                            "Agent[{}]: The agent on route to [{}] destination".format(self.agent.name, customer_id)
                        )

                        # New statistics
                        # Event 5: Customer Pickup
                        self.agent.events_store.emit(
                            event_type="customer_pickup",
                            details={},
                        )

                        (
                            distance,
                            _osrm_duration,
                            _speed_based_duration,
                        ) = await self.agent.move_to(dest)

                        self.agent.events_store.emit(
                            event_type="travel_to_destination",
                            details={
                                "distance": distance,
                            },
                        )

                        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
                        self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)

                    except PathRequestException:
                        await self.cancel_customer(customer_id=customer_id)
                        self.agent.status = TRANSPORT_WAITING
                        self.set_next_state(TRANSPORT_WAITING)
                    except AlreadyInDestination:
                        self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)

                    except Exception as e:
                        logger.error(
                            "Unexpected error in transport [{}]: {}".format(self.agent.name, e)
                        )

        elif performative == CANCEL_PERFORMATIVE:
            self.agent.status = TRANSPORT_WAITING
            self.set_next_state(TRANSPORT_WAITING)
            return
        else:
            self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
            return

# MOD-STRATEGY-04 - New status
class ElectricTaxiMovingToCustomerDestState(ElectricTaxiStrategyBehaviour):
    """
        Represents the state where the taxi is transporting the customer to their destination.

        Methods:
            on_start(): Logs the transition to 'Moving To Destination'.
            run(): Manages the trip to the customer's destination.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION

    async def run(self):

        customers = self.get("current_customer")
        customer_id = next(iter(customers.items()))[0]

        try:

            if not self.agent.is_in_destination():
                await self.agent.sleep(1)
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
            self.agent.status = TRANSPORT_WAITING
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
                "Unexpected error in transport [{}]: {}".format(self.agent.name, e)
            )
            await self.cancel_proposal(customer_id)
            self.agent.status = TRANSPORT_WAITING
            self.set_next_state(TRANSPORT_WAITING)
            return

class ElectricTaxiArrivedAtCustomerDestState(ElectricTaxiStrategyBehaviour):
    """
        Represents the state where the taxi has arrived at the customer's destination.

        Methods:
            on_start(): Logs the transition to 'Arrived At Destination'.
            run(): Handles the process of dropping the customer off and resets the taxi to 'Waiting' state.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION

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
                        self.agent.set_busy()

                        logger.debug(
                            "Agent[{}]: The electric taxi has completed the service "
                            "for customer [{}].".format(
                                self.agent.agent_id,
                                customer_id
                            )
                        )

                        self.agent.status = TRANSPORT_WAITING_FOR_RETURN
                        self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
                        return

            elif performative == CANCEL_PERFORMATIVE:
                self.agent.status = TRANSPORT_WAITING
                self.set_next_state(TRANSPORT_WAITING)
                return
            else:
                self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
                return

class ElectricTaxiWaitingForReturnState(ElectricTaxiStrategyBehaviour):

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_WAITING_FOR_RETURN
        self.return_requested = False

    async def run(self):

        # 1. Obtain a return point only if there is no pending one
        if not self.agent.has_return_position():

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

            self.agent.set_return_position(return_position)

            logger.info(
                "Agent[{}]: Return position received: {}".format(
                    self.agent.name,
                    return_position
                )
            )

        # 2. From here on, a return point already exists.
        #    This block is also executed after charging.
        return_position = self.agent.get_return_position()

        if return_position is None:
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        return_km = self.agent.calculate_distance_km(
            self.agent.get_position(),
            return_position
        )

        # 3. Option A: charge only if the return cannot be completed
        if not self.agent.has_enough_autonomy_km(return_km):

            logger.info(
                "Agent[{}]: Not enough autonomy to reach return point {}. "
                "Charging is required.".format(
                    self.agent.name,
                    return_position
                )
            )

            self.agent.set_busy()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

        # 4. Enough autonomy: complete the pending return
        try:
            await self.agent.move_to(return_position)

            self.agent.decrease_autonomy_km(return_km)

            self.agent.status = TRANSPORT_MOVING_TO_RETURN
            self.set_next_state(TRANSPORT_MOVING_TO_RETURN)
            return

        except AlreadyInDestination:

            logger.info(
                "Agent[{}]: Electric taxi is already at return point {}.".format(
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
                "Agent[{}]: Could not get a path to return point {}.".format(
                    self.agent.name,
                    return_position
                )
            )

            await self.agent.sleep(1)

            # Keep the same return point and retry
            self.agent.status = TRANSPORT_WAITING_FOR_RETURN
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        except Exception as e:

            logger.error(
                "Unexpected error returning electric taxi [{}]: {}".format(
                    self.agent.name,
                    e
                )
            )

            self.agent.status = TRANSPORT_WAITING_FOR_RETURN
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return


class ElectricTaxiMovingToReturnState(ElectricTaxiStrategyBehaviour):

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
            self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
            return

        if not self.agent.is_in_destination():
            await self.agent.sleep(1)
            self.set_next_state(TRANSPORT_MOVING_TO_RETURN)
            return

        logger.info(
            "Agent[{}]: Electric taxi arrived at return point {}.".format(
                self.agent.name,
                return_position
            )
        )

        self.agent.clear_return_position()
        self.agent.status = TRANSPORT_WAITING
        self.agent.set_available()
        self.set_next_state(TRANSPORT_WAITING)
        return

class FSMElectricTaxiBehaviour(FSMSimfleetBehaviour):
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
        self.add_state(TRANSPORT_NEEDS_CHARGING, ElectricTaxiNeedsChargingState())
        self.add_state(TRANSPORT_WAITING_FOR_APPROVAL, ElectricTaxiWaitingForApprovalState())
        self.add_state(TRANSPORT_MOVING_TO_CUSTOMER, ElectricTaxiMovingToCustomerState())
        self.add_state(TRANSPORT_ARRIVED_AT_CUSTOMER, ElectricTaxiArrivedAtCustomerState())
        self.add_state(TRANSPORT_MOVING_TO_DESTINATION, ElectricTaxiMovingToCustomerDestState())
        self.add_state(TRANSPORT_ARRIVED_AT_DESTINATION, ElectricTaxiArrivedAtCustomerDestState())
        self.add_state(TRANSPORT_MOVING_TO_STATION, ElectricTaxiMovingToStationState())
        self.add_state(TRANSPORT_IN_STATION_PLACE, ElectricTaxiInStationState())
        self.add_state(TRANSPORT_IN_WAITING_LIST, ElectricTaxiInWaitingListState())
        self.add_state(TRANSPORT_CHARGING, ElectricTaxiChargingState())
        self.add_state(TRANSPORT_WAITING_FOR_RETURN, ElectricTaxiWaitingForReturnState())
        self.add_state(TRANSPORT_MOVING_TO_RETURN,ElectricTaxiMovingToReturnState())

        # Define transitions between states

        # Transitions related to the 'Waiting' state
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING)  # Remains in waiting if no new action
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING_FOR_APPROVAL)  # When a customer accepts a proposal
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_NEEDS_CHARGING)  # If the taxi needs charging

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

        # Transitions related to the 'Needs Charging' state
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_NEEDS_CHARGING)  # Continue searching for a station
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_WAITING)  # Issue finding station, return to waiting
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_MOVING_TO_STATION)  # Successfully heading to station
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_IN_STATION_PLACE)  # Arrives at the station

        # Transitions from 'Moving To Station' state
        self.add_transition(TRANSPORT_MOVING_TO_STATION, TRANSPORT_MOVING_TO_STATION)  # Still heading to the station
        self.add_transition(TRANSPORT_MOVING_TO_STATION, TRANSPORT_IN_STATION_PLACE)  # Arrives at station
        self.add_transition(TRANSPORT_MOVING_TO_STATION, TRANSPORT_NEEDS_CHARGING)

        # Transitions from 'In Station Place' state
        self.add_transition(TRANSPORT_IN_STATION_PLACE, TRANSPORT_IN_STATION_PLACE)  # Waiting in station queue
        self.add_transition(TRANSPORT_IN_STATION_PLACE, TRANSPORT_NEEDS_CHARGING)  # Transition if refused service
        self.add_transition(TRANSPORT_IN_STATION_PLACE, TRANSPORT_IN_WAITING_LIST)  # Moved to waiting list for service

        # Transitions from 'In Waiting List' state
        self.add_transition(TRANSPORT_IN_WAITING_LIST, TRANSPORT_IN_WAITING_LIST)  # Remain in queue
        self.add_transition(TRANSPORT_IN_WAITING_LIST, TRANSPORT_CHARGING)  # Begin charging process
        self.add_transition(TRANSPORT_IN_WAITING_LIST, TRANSPORT_NEEDS_CHARGING)

        # Transitions from 'Charging' state
        self.add_transition(TRANSPORT_CHARGING, TRANSPORT_CHARGING)  # Continue charging
        self.add_transition(TRANSPORT_CHARGING, TRANSPORT_WAITING)  # Finish charging and return to waiting

        # Additional transitions for customer movement and destination states
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_MOVING_TO_CUSTOMER)  # Still en route to customer
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_WAITING)  # Return to waiting if issue arises

        self.add_transition(TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_WAITING_FOR_RETURN)

        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_MOVING_TO_RETURN)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_NEEDS_CHARGING)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_WAITING)

        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_MOVING_TO_RETURN)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_WAITING)

        self.add_transition(TRANSPORT_CHARGING, TRANSPORT_WAITING_FOR_RETURN)


################################################################
#                                                              #
#           NO Point Of Return Electric Taxi Strategy          #
#                                                              #
################################################################

class NRPElectricTaxiWaitingState(ElectricTaxiStrategyBehaviour):
    """
        Represents the 'Waiting' state for the electric taxi. The taxi is waiting to receive a transport request.

        Methods:
            on_start(): Sets the initial state to 'TRANSPORT_WAITING' and logs the state.
            run(): Handles incoming messages, processes transport requests, and transitions to the next state.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING

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

            if not self.agent.has_enough_autonomy_for_service(
                content["origin"],
                content["dest"]
            ):

                # New statistics
                # Event 1e: Need for Service
                self.agent.events_store.emit(
                    event_type="transport_need_for_service",
                    details={}
                )

                await self.cancel_proposal(content["customer_id"])
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return
            else:

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


class NPRElectricTaxiWaitingForApprovalState(ElectricTaxiStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING_FOR_APPROVAL

    async def run(self):

        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return

        performative = msg.get_metadata("performative")

        if performative == ACCEPT_PERFORMATIVE:

            customer_id = content["customer_id"]
            origin = content["origin"]
            dest = content["dest"]

            travel_km = self.agent.calculate_service_km(
                origin,
                dest
            )

            if not self.agent.has_enough_autonomy_km(
                travel_km
            ):
                await self.cancel_proposal(customer_id)

                self.agent.set_busy()
                self.agent.status = TRANSPORT_NEEDS_CHARGING
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return

            try:

                self.agent.add_assigned_customer(
                    customer_id=customer_id,
                    origin=origin,
                    dest=dest
                )

                (
                    distance,
                    osrm_duration,
                    _speed_based_duration,
                ) = await self.agent.move_to(origin)

                self.agent.decrease_autonomy_km(
                    travel_km
                )

                self.agent.set_busy()

                self.agent.events_store.emit(
                    event_type="transport_offer_acceptance",
                    details={}
                )

                self.agent.events_store.emit(
                    event_type="travel_to_pickup",
                    details={
                        "distance": distance,
                        "duration": _speed_based_duration
                    }
                )

                await self.inform_customer(
                    customer_id=customer_id,
                    status=TRANSPORT_MOVING_TO_CUSTOMER
                )

                self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER
                self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
                return

            except AlreadyInDestination:

                self.agent.set_busy()

                self.agent.events_store.emit(
                    event_type="transport_offer_acceptance",
                    details={}
                )

                await self.inform_customer(
                    customer_id=customer_id,
                    status=TRANSPORT_IN_CUSTOMER_PLACE
                )

                self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
                self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
                return

            except PathRequestException:

                logger.error(
                    "Agent[{}]: Could not get a path to customer [{}].".format(
                        self.agent.name,
                        customer_id
                    )
                )

                await self.cancel_proposal(customer_id)

                self.agent.remove_assigned_customer()

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()

                self.set_next_state(TRANSPORT_WAITING)
                return

            except Exception as e:

                logger.error(
                    "Unexpected error in transport [{}]: {}".format(
                        self.agent.name,
                        e
                    )
                )

                await self.cancel_proposal(customer_id)

                self.agent.remove_assigned_customer()

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()

                self.set_next_state(TRANSPORT_WAITING)
                return

        elif performative == REFUSE_PERFORMATIVE:

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()

            self.set_next_state(TRANSPORT_WAITING)
            return

        self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)



class NPRElectricTaxiMovingToCustomerState(ElectricTaxiStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER

    async def run(self):

        customers = self.get("assigned_customer")

        if not customers:
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        customer_id = next(iter(customers.items()))[0]

        try:

            if not self.agent.is_in_destination():

                msg = await self.receive(timeout=2)

                if msg:
                    performative = msg.get_metadata("performative")

                    if performative == REQUEST_PERFORMATIVE:
                        self.set_next_state(
                            TRANSPORT_MOVING_TO_CUSTOMER
                        )
                        return

                    elif performative == REFUSE_PERFORMATIVE:

                        await self.cancel_proposal(customer_id)

                        self.agent.remove_assigned_customer()

                        self.agent.status = TRANSPORT_WAITING
                        self.agent.set_available()

                        self.set_next_state(
                            TRANSPORT_WAITING
                        )
                        return

                self.set_next_state(
                    TRANSPORT_MOVING_TO_CUSTOMER
                )
                return

            logger.info(
                "Agent[{}]: The agent has arrived to customer [{}].".format(
                    self.agent.name,
                    customer_id
                )
            )

            await self.inform_customer(
                customer_id=customer_id,
                status=TRANSPORT_IN_CUSTOMER_PLACE
            )

            self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER

            self.set_next_state(
                TRANSPORT_ARRIVED_AT_CUSTOMER
            )
            return

        except AlreadyInDestination:

            await self.inform_customer(
                customer_id=customer_id,
                status=TRANSPORT_IN_CUSTOMER_PLACE
            )

            self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER

            self.set_next_state(
                TRANSPORT_ARRIVED_AT_CUSTOMER
            )
            return

        except PathRequestException:

            logger.error(
                "Agent[{}]: Could not get a path to customer [{}].".format(
                    self.agent.name,
                    customer_id
                )
            )

            await self.cancel_proposal(customer_id)

            self.agent.remove_assigned_customer()

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()

            self.set_next_state(
                TRANSPORT_WAITING
            )
            return

        except Exception as e:

            logger.error(
                "Unexpected error in transport [{}]: {}".format(
                    self.agent.name,
                    e
                )
            )

            await self.cancel_proposal(customer_id)

            self.agent.remove_assigned_customer()

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()

            self.set_next_state(
                TRANSPORT_WAITING
            )
            return


class NPRElectricTaxiArrivedAtCustomerState(ElectricTaxiStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER

    async def run(self):

        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(
                TRANSPORT_ARRIVED_AT_CUSTOMER
            )
            return

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            self.set_next_state(
                TRANSPORT_ARRIVED_AT_CUSTOMER
            )
            return

        performative = msg.get_metadata("performative")

        if performative == INFORM_PERFORMATIVE:

            if content.get("status") != CUSTOMER_IN_TRANSPORT:
                self.set_next_state(
                    TRANSPORT_ARRIVED_AT_CUSTOMER
                )
                return

            customers = self.get("assigned_customer")

            if not customers:
                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING)
                return

            customer_id = next(iter(customers.items()))[0]
            dest = next(iter(customers.items()))[1]["destination"]

            try:

                logger.debug(
                    "Agent[{}]: Customer [{}] in transport.".format(
                        self.agent.name,
                        customer_id
                    )
                )

                self.agent.add_customer_in_transport(
                    customer_id=customer_id,
                    dest=dest
                )

                self.agent.remove_assigned_customer()

                self.agent.events_store.emit(
                    event_type="customer_pickup",
                    details={}
                )

                (
                    distance,
                    osrm_duration,
                    _speed_based_duration,
                ) = await self.agent.move_to(dest)

                self.agent.events_store.emit(
                    event_type="travel_to_destination",
                    details={
                        "distance": distance,
                        "duration": _speed_based_duration,
                    },
                )

                self.agent.status = TRANSPORT_MOVING_TO_DESTINATION

                self.set_next_state(
                    TRANSPORT_MOVING_TO_DESTINATION
                )
                return

            except AlreadyInDestination:

                self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION

                self.set_next_state(
                    TRANSPORT_ARRIVED_AT_DESTINATION
                )
                return

            except PathRequestException:

                logger.error(
                    "Agent[{}]: Could not get a path to destination "
                    "for customer [{}].".format(
                        self.agent.name,
                        customer_id
                    )
                )

                await self.cancel_customer(
                    customer_id=customer_id
                )

                self.agent.remove_customer_in_transport(
                    customer_id
                )

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()

                self.set_next_state(
                    TRANSPORT_WAITING
                )
                return

            except Exception as e:

                logger.error(
                    "Unexpected error in transport [{}]: {}".format(
                        self.agent.name,
                        e
                    )
                )

                await self.cancel_customer(
                    customer_id=customer_id
                )

                self.agent.remove_customer_in_transport(
                    customer_id
                )

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()

                self.set_next_state(
                    TRANSPORT_WAITING
                )
                return

        elif performative == CANCEL_PERFORMATIVE:

            customers = self.get("assigned_customer")

            if customers:
                self.agent.remove_assigned_customer()

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()

            self.set_next_state(
                TRANSPORT_WAITING
            )
            return

        self.set_next_state(
            TRANSPORT_ARRIVED_AT_CUSTOMER
        )


class NPRElectricTaxiMovingToCustomerDestState(ElectricTaxiStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION

    async def run(self):

        customers = self.get("current_customer")

        if not customers:
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        customer_id = next(iter(customers.items()))[0]

        try:

            if not self.agent.is_in_destination():

                await self.agent.sleep(1)

                self.set_next_state(
                    TRANSPORT_MOVING_TO_DESTINATION
                )
                return

            logger.info(
                "Agent[{}]: The agent has arrived to destination "
                "with customer [{}].".format(
                    self.agent.name,
                    customer_id
                )
            )

            self.agent.events_store.emit(
                event_type="trip_completion",
                details={}
            )

            await self.inform_customer(
                customer_id=customer_id,
                status=CUSTOMER_IN_DEST
            )

            self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION

            self.set_next_state(
                TRANSPORT_ARRIVED_AT_DESTINATION
            )
            return

        except AlreadyInDestination:

            self.agent.events_store.emit(
                event_type="trip_completion",
                details={}
            )

            await self.inform_customer(
                customer_id=customer_id,
                status=CUSTOMER_IN_DEST
            )

            self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION

            self.set_next_state(
                TRANSPORT_ARRIVED_AT_DESTINATION
            )
            return

        except PathRequestException:

            logger.error(
                "Agent[{}]: Could not complete the route "
                "for customer [{}].".format(
                    self.agent.name,
                    customer_id
                )
            )

            await self.cancel_customer(
                customer_id=customer_id
            )

            self.agent.remove_customer_in_transport(
                customer_id
            )

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()

            self.set_next_state(
                TRANSPORT_WAITING
            )
            return

        except Exception as e:

            logger.error(
                "Unexpected error in transport [{}]: {}".format(
                    self.agent.name,
                    e
                )
            )

            await self.cancel_customer(
                customer_id=customer_id
            )

            self.agent.remove_customer_in_transport(
                customer_id
            )

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()

            self.set_next_state(
                TRANSPORT_WAITING
            )
            return


class NPRElectricTaxiArrivedAtCustomerDestState(ElectricTaxiStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION

    async def run(self):

        customers = self.get("current_customer")

        if not customers:
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return

        customer_id = next(iter(customers.items()))[0]

        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(
                TRANSPORT_ARRIVED_AT_DESTINATION
            )
            return

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            self.set_next_state(
                TRANSPORT_ARRIVED_AT_DESTINATION
            )
            return

        performative = msg.get_metadata("performative")

        if performative == INFORM_PERFORMATIVE:

            if content.get("status") != CUSTOMER_IN_DEST:
                self.set_next_state(
                    TRANSPORT_ARRIVED_AT_DESTINATION
                )
                return

            self.agent.remove_customer_in_transport(
                customer_id
            )

            self.agent.increment_completed_assignments()

            logger.info(
                "Agent[{}]: Service for customer [{}] completed.".format(
                    self.agent.name,
                    customer_id
                )
            )

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()

            self.set_next_state(
                TRANSPORT_WAITING
            )
            return

        elif performative == CANCEL_PERFORMATIVE:

            self.agent.remove_customer_in_transport(
                customer_id
            )

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()

            self.set_next_state(
                TRANSPORT_WAITING
            )
            return

        self.set_next_state(
            TRANSPORT_ARRIVED_AT_DESTINATION
        )


class NPRElectricTaxiNeedsChargingState(ElectricTaxiStrategyBehaviour):

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_NEEDS_CHARGING
        self.agent.set_busy()

    async def run(self):

        if (
            self.agent.get_stations() is None
            or self.agent.get_number_stations() < 1
        ):

            logger.info(
                "Agent[{}]: Looking for a charging station.".format(
                    self.agent.name
                )
            )

            stations = await self.agent.get_list_agent_position(
                self.agent.service_type,
                self.agent.get_stations()
            )

            self.agent.set_stations(stations)

            self.set_next_state(
                TRANSPORT_NEEDS_CHARGING
            )
            return

        nearby_station = self.agent.nearst_agent(
            self.agent.get_stations(),
            self.agent.get_position()
        )

        if nearby_station is None:
            logger.warning(
                "Agent[{}]: No charging station available.".format(
                    self.agent.name
                )
            )

            self.set_next_state(
                TRANSPORT_NEEDS_CHARGING
            )
            return

        self.agent.set_nearby_station(
            nearby_station
        )

        station_id = self.agent.get_nearby_station_id()
        station_position = self.agent.get_nearby_station_position()

        logger.info(
            "Agent[{}]: Selected charging station [{}].".format(
                self.agent.name,
                station_id
            )
        )

        try:

            await self.go_to_the_station(
                station_id,
                station_position
            )

            travel_km = self.agent.calculate_distance_km(
                self.agent.get_position(),
                station_position
            )

            (
                distance,
                osrm_duration,
                _speed_based_duration,
            ) = await self.agent.move_to(
                station_position
            )

            self.agent.decrease_autonomy_km(
                travel_km
            )

            self.agent.events_store.emit(
                event_type="travel_to_station",
                details={
                    "distance": distance,
                    "duration": _speed_based_duration,
                },
            )

            self.agent.status = TRANSPORT_MOVING_TO_STATION

            self.set_next_state(
                TRANSPORT_MOVING_TO_STATION
            )
            return

        except AlreadyInDestination:

            logger.info(
                "Agent[{}]: Already at charging station [{}].".format(
                    self.agent.name,
                    station_id
                )
            )

            arguments = {
                "transport_need":
                    self.agent.max_autonomy_km
                    - self.agent.current_autonomy_km
            }

            content = {
                "service_name": self.agent.service_type,
                "object_type": "transport",
                "args": arguments
            }

            await self.request_access_station(
                self.agent.get_current_station(),
                content
            )

            self.agent.events_store.emit(
                event_type="arrival_at_station",
                details={}
            )

            self.agent.status = TRANSPORT_IN_STATION_PLACE

            self.set_next_state(
                TRANSPORT_IN_STATION_PLACE
            )
            return

        except PathRequestException:

            logger.error(
                "Agent[{}]: Could not get a path to charging "
                "station [{}].".format(
                    self.agent.name,
                    station_id
                )
            )

            await self.drop_station()

            self.agent.status = TRANSPORT_NEEDS_CHARGING

            self.set_next_state(
                TRANSPORT_NEEDS_CHARGING
            )
            return

        except Exception as e:

            logger.error(
                "Unexpected error in electric taxi [{}]: {}".format(
                    self.agent.name,
                    e
                )
            )

            await self.drop_station()

            self.agent.status = TRANSPORT_NEEDS_CHARGING

            self.set_next_state(
                TRANSPORT_NEEDS_CHARGING
            )
            return


class NPRElectricTaxiMovingToStationState(ElectricTaxiStrategyBehaviour):

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_MOVING_TO_STATION

    async def run(self):

        station_id = self.agent.get_current_station()

        try:

            if not self.agent.is_in_destination():

                await self.agent.sleep(1)

                self.set_next_state(
                    TRANSPORT_MOVING_TO_STATION
                )
                return

            logger.info(
                "Agent[{}]: Arrived at charging station [{}].".format(
                    self.agent.name,
                    station_id
                )
            )

            arguments = {
                "transport_need":
                    self.agent.max_autonomy_km
                    - self.agent.current_autonomy_km
            }

            content = {
                "service_name": self.agent.service_type,
                "object_type": "transport",
                "args": arguments
            }

            await self.request_access_station(
                station_id,
                content
            )

            self.agent.events_store.emit(
                event_type="arrival_at_station",
                details={}
            )

            self.agent.status = TRANSPORT_IN_STATION_PLACE

            self.set_next_state(
                TRANSPORT_IN_STATION_PLACE
            )
            return

        except AlreadyInDestination:

            logger.info(
                "Agent[{}]: Already at charging station [{}].".format(
                    self.agent.name,
                    station_id
                )
            )

            arguments = {
                "transport_need":
                    self.agent.max_autonomy_km
                    - self.agent.current_autonomy_km
            }

            content = {
                "service_name": self.agent.service_type,
                "object_type": "transport",
                "args": arguments
            }

            await self.request_access_station(
                station_id,
                content
            )

            self.agent.events_store.emit(
                event_type="arrival_at_station",
                details={}
            )

            self.agent.status = TRANSPORT_IN_STATION_PLACE

            self.set_next_state(
                TRANSPORT_IN_STATION_PLACE
            )
            return

        except PathRequestException:

            logger.error(
                "Agent[{}]: Could not complete route "
                "to charging station [{}].".format(
                    self.agent.name,
                    station_id
                )
            )

            await self.drop_station()

            self.agent.status = TRANSPORT_NEEDS_CHARGING

            self.set_next_state(
                TRANSPORT_NEEDS_CHARGING
            )
            return

        except Exception as e:

            logger.error(
                "Unexpected error in electric taxi [{}]: {}".format(
                    self.agent.name,
                    e
                )
            )

            await self.drop_station()

            self.agent.status = TRANSPORT_NEEDS_CHARGING

            self.set_next_state(
                TRANSPORT_NEEDS_CHARGING
            )
            return


class NPRElectricTaxiInStationState(ElectricTaxiStrategyBehaviour):

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_IN_STATION_PLACE

    async def run(self):

        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(
                TRANSPORT_IN_STATION_PLACE
            )
            return

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            self.set_next_state(
                TRANSPORT_IN_STATION_PLACE
            )
            return

        performative = msg.get_metadata(
            "performative"
        )

        if performative == ACCEPT_PERFORMATIVE:

            if content.get("station_id") is None:
                self.set_next_state(
                    TRANSPORT_IN_STATION_PLACE
                )
                return

            logger.debug(
                "Agent[{}]: ACCEPT received from station [{}].".format(
                    self.agent.name,
                    content["station_id"]
                )
            )

            self.agent.events_store.emit(
                event_type="wait_for_service",
                details={}
            )

            self.agent.status = TRANSPORT_IN_WAITING_LIST

            self.set_next_state(
                TRANSPORT_IN_WAITING_LIST
            )
            return

        elif performative == REFUSE_PERFORMATIVE:

            logger.info(
                "Agent[{}]: Charging station [{}] refused the request.".format(
                    self.agent.name,
                    self.agent.get_current_station()
                )
            )

            await self.drop_station()

            self.agent.status = TRANSPORT_NEEDS_CHARGING

            self.set_next_state(
                TRANSPORT_NEEDS_CHARGING
            )
            return

        self.set_next_state(
            TRANSPORT_IN_STATION_PLACE
        )


class NPRElectricTaxiInWaitingListState(ElectricTaxiStrategyBehaviour):

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_IN_WAITING_LIST

    async def run(self):

        msg = await self.receive(timeout=5)

        if not msg:
            self.set_next_state(
                TRANSPORT_IN_WAITING_LIST
            )
            return

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            self.set_next_state(
                TRANSPORT_IN_WAITING_LIST
            )
            return

        performative = msg.get_metadata(
            "performative"
        )

        if performative == INFORM_PERFORMATIVE:

            if content.get("station_id") is None:
                self.set_next_state(
                    TRANSPORT_IN_WAITING_LIST
                )
                return

            if content.get("serving"):

                logger.debug(
                    "Agent[{}]: Charging service started at station [{}].".format(
                        self.agent.name,
                        content["station_id"]
                    )
                )

                self.agent.events_store.emit(
                    event_type="service_start",
                    details={}
                )

                self.agent.status = TRANSPORT_CHARGING

                self.set_next_state(
                    TRANSPORT_CHARGING
                )
                return

            self.set_next_state(
                TRANSPORT_IN_WAITING_LIST
            )
            return

        elif performative == REFUSE_PERFORMATIVE:

            logger.info(
                "Agent[{}]: Charging service refused by station [{}].".format(
                    self.agent.name,
                    self.agent.get_current_station()
                )
            )

            await self.drop_station()

            self.agent.status = TRANSPORT_NEEDS_CHARGING

            self.set_next_state(
                TRANSPORT_NEEDS_CHARGING
            )
            return

        self.set_next_state(
            TRANSPORT_IN_WAITING_LIST
        )


class NPRElectricTaxiChargingState(ElectricTaxiStrategyBehaviour):

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_CHARGING

    async def run(self):

        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(
                TRANSPORT_CHARGING
            )
            return

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            self.set_next_state(
                TRANSPORT_CHARGING
            )
            return

        protocol = msg.get_metadata(
            "protocol"
        )

        performative = msg.get_metadata(
            "performative"
        )

        if (
            protocol == REQUEST_PROTOCOL
            and performative == INFORM_PERFORMATIVE
        ):

            if content.get("charged"):

                self.agent.increase_full_autonomy_km()

                await self.drop_station()

                self.agent.events_store.emit(
                    event_type="service_completion",
                    details={}
                )

                logger.info(
                    "Agent[{}]: Charging completed. "
                    "Current autonomy: {} km.".format(
                        self.agent.name,
                        self.agent.get_autonomy()
                    )
                )

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()

                self.set_next_state(
                    TRANSPORT_WAITING
                )
                return

        self.set_next_state(
            TRANSPORT_CHARGING
        )


class FSMNPRElectricTaxiBehaviour(FSMSimfleetBehaviour):

    def setup(self):

        # ============================================================
        # States
        # ============================================================

        self.add_state(
            TRANSPORT_WAITING,
            NRPElectricTaxiWaitingState(),
            initial=True
        )

        self.add_state(
            TRANSPORT_WAITING_FOR_APPROVAL,
            NPRElectricTaxiWaitingForApprovalState()
        )

        self.add_state(
            TRANSPORT_MOVING_TO_CUSTOMER,
            NPRElectricTaxiMovingToCustomerState()
        )

        self.add_state(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            NPRElectricTaxiArrivedAtCustomerState()
        )

        self.add_state(
            TRANSPORT_MOVING_TO_DESTINATION,
            NPRElectricTaxiMovingToCustomerDestState()
        )

        self.add_state(
            TRANSPORT_ARRIVED_AT_DESTINATION,
            NPRElectricTaxiArrivedAtCustomerDestState()
        )

        self.add_state(
            TRANSPORT_NEEDS_CHARGING,
            NPRElectricTaxiNeedsChargingState()
        )

        self.add_state(
            TRANSPORT_MOVING_TO_STATION,
            NPRElectricTaxiMovingToStationState()
        )

        self.add_state(
            TRANSPORT_IN_STATION_PLACE,
            NPRElectricTaxiInStationState()
        )

        self.add_state(
            TRANSPORT_IN_WAITING_LIST,
            NPRElectricTaxiInWaitingListState()
        )

        self.add_state(
            TRANSPORT_CHARGING,
            NPRElectricTaxiChargingState()
        )

        # ============================================================
        # WAITING
        # ============================================================

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING_FOR_APPROVAL
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # WAITING FOR APPROVAL
        # ============================================================

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_WAITING_FOR_APPROVAL
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_NEEDS_CHARGING
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_MOVING_TO_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_ARRIVED_AT_CUSTOMER
        )

        # ============================================================
        # MOVING TO CUSTOMER
        # ============================================================

        self.add_transition(
            TRANSPORT_MOVING_TO_CUSTOMER,
            TRANSPORT_MOVING_TO_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_CUSTOMER,
            TRANSPORT_ARRIVED_AT_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_CUSTOMER,
            TRANSPORT_WAITING
        )

        # ============================================================
        # ARRIVED AT CUSTOMER
        # ============================================================

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_ARRIVED_AT_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_ARRIVED_AT_DESTINATION
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_WAITING
        )

        # ============================================================
        # MOVING TO DESTINATION
        # ============================================================

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_ARRIVED_AT_DESTINATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_WAITING
        )

        # ============================================================
        # ARRIVED AT DESTINATION
        # ============================================================

        self.add_transition(
            TRANSPORT_ARRIVED_AT_DESTINATION,
            TRANSPORT_ARRIVED_AT_DESTINATION
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_DESTINATION,
            TRANSPORT_WAITING
        )

        # ============================================================
        # NEEDS CHARGING
        # ============================================================

        self.add_transition(
            TRANSPORT_NEEDS_CHARGING,
            TRANSPORT_NEEDS_CHARGING
        )

        self.add_transition(
            TRANSPORT_NEEDS_CHARGING,
            TRANSPORT_MOVING_TO_STATION
        )

        self.add_transition(
            TRANSPORT_NEEDS_CHARGING,
            TRANSPORT_IN_STATION_PLACE
        )

        # ============================================================
        # MOVING TO STATION
        # ============================================================

        self.add_transition(
            TRANSPORT_MOVING_TO_STATION,
            TRANSPORT_MOVING_TO_STATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_STATION,
            TRANSPORT_IN_STATION_PLACE
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_STATION,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # IN STATION PLACE
        # ============================================================

        self.add_transition(
            TRANSPORT_IN_STATION_PLACE,
            TRANSPORT_IN_STATION_PLACE
        )

        self.add_transition(
            TRANSPORT_IN_STATION_PLACE,
            TRANSPORT_IN_WAITING_LIST
        )

        self.add_transition(
            TRANSPORT_IN_STATION_PLACE,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # IN WAITING LIST
        # ============================================================

        self.add_transition(
            TRANSPORT_IN_WAITING_LIST,
            TRANSPORT_IN_WAITING_LIST
        )

        self.add_transition(
            TRANSPORT_IN_WAITING_LIST,
            TRANSPORT_CHARGING
        )

        self.add_transition(
            TRANSPORT_IN_WAITING_LIST,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # CHARGING
        # ============================================================

        self.add_transition(
            TRANSPORT_CHARGING,
            TRANSPORT_CHARGING
        )

        self.add_transition(
            TRANSPORT_CHARGING,
            TRANSPORT_WAITING
        )
