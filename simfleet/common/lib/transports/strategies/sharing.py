import asyncio
import json

from loguru import logger
from spade.message import Message
from spade.behaviour import State

from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_BOOKED, TRANSPORT_MOVING_TO_DESTINATION, \
    TRANSPORT_IN_DEST, TRANSPORT_IN_CUSTOMER_PLACE, CUSTOMER_IN_DEST, CUSTOMER_IN_TRANSPORT

from simfleet.communications.protocol import (
    REQUEST_PERFORMATIVE,
    PROPOSE_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    REQUEST_PROTOCOL,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination,
    distance_in_meters
)


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class SharingStrategyBehaviour(State):

    async def on_start(self):
        logger.debug(
            "Strategy {} started in transport {}".format(
                type(self).__name__,
                self.agent.name
            )
        )


    # async def send_status_fleetmanager(self):
    #     msg = Message()
    #     msg.to = str(self.agent.fleetmanager_id)
    #     msg.set_metadata("protocol", REQUEST_PROTOCOL)
    #     msg.set_metadata("performative", INFORM_PERFORMATIVE)
    #     msg.body = json.dumps({
    #         "name": self.agent.name,
    #         "jid": str(self.agent.jid),
    #         "status": self.agent.status,
    #         "position": self.agent.get_position()
    #     })
    #     await self.send(msg)

    async def pick_up_customer(self, customer_id, origin, dest):
        # Save customer attributes and travel destination
        #self.set("current_customer", customer_id)
        #self.agent.current_customer_orig = origin
        #self.agent.current_customer_dest = dest

        self.agent.add_customer_in_transport(
            customer_id=customer_id, origin=origin, dest=dest
        )

        if not self.agent.is_customer_in_transport():
            try:
                # try to pick up the customer and move towards its destination
                #self.set("customer_in_transport", self.get("current_customer"))
                self.set("customer_in_transport", customer_id)
                #await self.agent.move_to(self.agent.current_customer_dest)
                await self.agent.move_to(dest)
                #self.agent.num_assignments += 1
            except PathRequestException:
                # if there is no path to customer's destination, cancel it
                await self.cancel_customer()
                self.agent.status = TRANSPORT_WAITING
            #except AlreadyInDestination:
                # if the transport is already in the customer's destination, drop the customer off
                #logger.error("++++++++++ transport {} is already in customers destination {}".format(
                #    self.agent.name, self.agent.current_customer_dest))
            #    logger.error("++++++++++ transport {} is already in customers destination {}".format(
            #        self.agent.name, dest))
            #    await self.agent.drop_customer()
            else:
                # if there is no error moving to the destination,
                # inform the customer that it has been picked up
                #await self.agent.inform_customer(self.get("current_customer"), TRANSPORT_IN_CUSTOMER_PLACE)
                await self.inform_customer(customer_id, TRANSPORT_IN_CUSTOMER_PLACE)
                self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
                #logger.info("Transport {} has picked up the customer {}.".format(
                #    self.agent.agent_id, self.get("current_customer")))
                logger.info("Transport {} has picked up the customer {}.".format(
                    self.agent.agent_id, customer_id))

    #
    # async def pick_up_customer_in_station(self, customer_id, origin, dest):
    #     # Save customer attributes and travel destination
    #     #self.set("current_customer", customer_id)
    #     #self.agent.current_customer_orig = origin
    #     #self.agent.current_customer_dest = dest
    #
    #     self.agent.add_customer_in_transport(
    #         customer_id=customer_id, origin=origin, dest=dest
    #     )
    #
    #     if not self.agent.is_customer_in_transport():
    #         try:
    #             # try to pick up the customer and move towards its destination
    #             #self.set("customer_in_transport", self.get("current_customer"))
    #             self.set("customer_in_transport", customer_id)
    #             #await self.agent.move_to(self.agent.current_customer_dest)
    #             await self.agent.move_to(dest)
    #             #self.agent.num_assignments += 1
    #         except PathRequestException:
    #             # if there is no path to customer's destination, cancel it
    #             await self.cancel_customer()
    #             self.agent.set_registration(status=False)  # Registro esta a FALSE para que se registre nuevamente en la estación.
    #             self.agent.status = TRANSPORT_WAITING
    #         else:
    #             logger.info("Transport {} has picked up the customer {}.".format(
    #                 self.agent.agent_id, customer_id))

    async def accept_customer(self, customer_id):

        reply = Message()

        reply.to = str(customer_id)
        reply.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        reply.set_metadata(
            "performative",
            ACCEPT_PERFORMATIVE
        )

        reply.body = json.dumps(
            {
                "transport_id": str(self.agent.jid),
                "position": self.agent.get_position()
            }
        )

        await self.send(reply)

        logger.info(
            "Transport {} accepted booking from customer {}".format(
                self.agent.name,
                customer_id
            )
        )

    async def refuse_customer(self, customer_id):

        reply = Message()

        reply.to = str(customer_id)
        reply.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        reply.set_metadata(
            "performative",
            REFUSE_PERFORMATIVE
        )

        reply.body = json.dumps(
            {
                "transport_id": str(self.agent.jid),
                "position": self.agent.get_position()
            }
        )

        await self.send(reply)

        logger.info(
            "Transport {} refused booking from customer {}".format(
                self.agent.name,
                customer_id
            )
        )

    async def inform_customer(
        self,
        customer_id,
        status,
        data=None
    ):

        if data is None:
            data = {}

        msg = Message()

        msg.to = str(customer_id)
        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        msg.set_metadata(
            "performative",
            INFORM_PERFORMATIVE
        )

        data["status"] = status

        msg.body = json.dumps(data)

        await self.send(msg)

    async def cancel_customer(
        self,
        customer_id,
        data=None
    ):

        if data is None:
            data = {}

        msg = Message()

        msg.to = str(customer_id)
        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        msg.set_metadata(
            "performative",
            CANCEL_PERFORMATIVE
        )

        msg.body = json.dumps(data)

        await self.send(msg)

        logger.debug(
            "Agent[{}]: Cancelled booking with customer [{}].".format(
                self.agent.agent_id,
                customer_id
            )
        )
    #
    # async def inform_station(self, station_id, content):
    #     """
    #         Inform to station
    #
    #         Args:
    #             content (dict, optional): Information needed for registration.
    #     """
    #     if content is None:
    #         content = {}
    #     msg = Message()
    #     msg.to = station_id
    #     msg.set_metadata("protocol", REQUEST_PROTOCOL)
    #     msg.set_metadata("performative", INFORM_PERFORMATIVE)
    #     msg.body = json.dumps(content)
    #     await self.send(msg)

    async def deassign_customer(self):
        """
        Triggered when, by any reason, a customer cancels their already accepted booking
        """
        # Delete saved values (destination, etc.) belonging to booked customer
        self.agent.set("current_customer", None)
        self.agent.current_customer_orig = None
        self.agent.current_customer_dest = None

    async def run(self):
        raise NotImplementedError


# ==================================================================
# -------------------------End Behaviour----------------------------
# ==================================================================


################################################################
#                                                              #
#               Distributed Transport Strategy                 #
#                                                              #
################################################################
class SharingWaitingState(SharingStrategyBehaviour):
    """
    Represents the state where the sharing transport is available
    and waiting for a booking request.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_WAITING

        logger.debug(
            "Agent[{}]: The sharing transport is waiting.".format(
                self.agent.name
            )
        )

    async def run(self):

        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(TRANSPORT_WAITING)
            return

        logger.debug(
            "Agent[{}]: The agent received: {}".format(
                self.agent.jid,
                msg.body
            )
        )

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: Invalid booking request.".format(
                    self.agent.name
                )
            )

            self.set_next_state(TRANSPORT_WAITING)
            return

        performative = msg.get_metadata(
            "performative"
        )

        if performative != PROPOSE_PERFORMATIVE:
            self.set_next_state(TRANSPORT_WAITING)
            return

        customer_id = content.get("customer_id")
        dest = content.get("dest")

        if customer_id is None or dest is None:

            logger.warning(
                "Agent[{}]: Booking request without customer or destination.".format(
                    self.agent.name
                )
            )

            if customer_id is not None:
                await self.refuse_customer(
                    customer_id
                )

            self.set_next_state(TRANSPORT_WAITING)
            return

        origin = self.agent.get_position()

        self.agent.add_assigned_customer(
            customer_id=customer_id,
            origin=origin,
            dest=dest
        )

        self.agent.set_busy()

        await self.accept_customer(
            customer_id
        )

        logger.info(
            "Agent[{}]: Sharing transport booked by customer [{}].".format(
                self.agent.name,
                customer_id
            )
        )

        self.agent.status = TRANSPORT_BOOKED
        self.set_next_state(TRANSPORT_BOOKED)
        return


class SharingBookedState(SharingStrategyBehaviour):
    """
    Represents the state where the sharing transport is booked
    by a customer and waits for the customer to reach it.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_BOOKED

        logger.debug(
            "Agent[{}]: The sharing transport is booked.".format(
                self.agent.name
            )
        )

    async def run(self):

        msg = await self.receive(timeout=60)

        # The booking remains active while the customer walks
        # towards the sharing transport.
        if not msg:
            self.set_next_state(TRANSPORT_BOOKED)
            return

        logger.debug(
            "Agent[{}]: The agent received: {}".format(
                self.agent.jid,
                msg.body
            )
        )

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: Invalid message received while booked.".format(
                    self.agent.name
                )
            )

            self.set_next_state(TRANSPORT_BOOKED)
            return

        performative = msg.get_metadata(
            "performative"
        )

        customer_id = content.get(
            "customer_id"
        )

        assigned_customers = (
            self.agent.get("assigned_customer")
            or {}
        )

        # Another customer tries to book the transport.
        if performative == PROPOSE_PERFORMATIVE:

            if customer_id is not None:
                await self.refuse_customer(
                    customer_id
                )

            self.set_next_state(TRANSPORT_BOOKED)
            return

        # The customer cancels the booking before starting the trip.
        elif performative == CANCEL_PERFORMATIVE:

            if (
                customer_id is None
                or str(customer_id) not in assigned_customers
            ):
                self.set_next_state(TRANSPORT_BOOKED)
                return

            logger.info(
                "Agent[{}]: Customer [{}] cancelled the booking.".format(
                    self.agent.name,
                    customer_id
                )
            )

            self.agent.remove_assigned_customer()

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()

            self.set_next_state(TRANSPORT_WAITING)
            return

        # The customer informs that they have reached the transport.
        elif performative == INFORM_PERFORMATIVE:

            if (
                customer_id is None
                or str(customer_id) not in assigned_customers
            ):
                logger.warning(
                    "Agent[{}]: Arrival notification received "
                    "from an unassigned customer [{}].".format(
                        self.agent.name,
                        customer_id
                    )
                )

                self.set_next_state(TRANSPORT_BOOKED)
                return

            customer_data = assigned_customers[
                str(customer_id)
            ]

            origin = customer_data.get(
                "origin"
            )

            dest = customer_data.get(
                "destination"
            )

            if dest is None:
                logger.warning(
                    "Agent[{}]: Booking for customer [{}] "
                    "has no destination.".format(
                        self.agent.name,
                        customer_id
                    )
                )

                self.agent.remove_assigned_customer()

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()

                self.set_next_state(TRANSPORT_WAITING)
                return

            # The booking becomes an active trip.
            self.agent.add_customer_in_transport(
                customer_id=customer_id,
                origin=origin,
                dest=dest
            )

            self.agent.remove_assigned_customer()

            try:
                await self.agent.move_to(
                    dest
                )

            except AlreadyInDestination:

                logger.info(
                    "Agent[{}]: The sharing transport is already "
                    "at customer [{}] destination.".format(
                        self.agent.name,
                        customer_id
                    )
                )

                await self.inform_customer(
                    customer_id=customer_id,
                    status=CUSTOMER_IN_DEST
                )

                self.agent.remove_customer_in_transport(
                    customer_id
                )

                self.agent.increment_completed_assignments()

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()

                self.set_next_state(TRANSPORT_WAITING)
                return

            except PathRequestException:

                logger.error(
                    "Agent[{}]: Could not get a path to "
                    "customer [{}] destination.".format(
                        self.agent.name,
                        customer_id
                    )
                )

                await self.cancel_customer(
                    customer_id
                )

                self.agent.remove_customer_in_transport(
                    customer_id
                )

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()

                self.set_next_state(TRANSPORT_WAITING)
                return

            except Exception as e:

                logger.error(
                    "Unexpected error in sharing transport [{}]: {}".format(
                        self.agent.name,
                        e
                    )
                )

                await self.cancel_customer(
                    customer_id
                )

                self.agent.remove_customer_in_transport(
                    customer_id
                )

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()

                self.set_next_state(TRANSPORT_WAITING)
                return

            await self.inform_customer(
                customer_id=customer_id,
                status=CUSTOMER_IN_TRANSPORT
            )

            self.agent.status = TRANSPORT_MOVING_TO_DESTINATION

            self.set_next_state(
                TRANSPORT_MOVING_TO_DESTINATION
            )
            return

        self.set_next_state(TRANSPORT_BOOKED)
        return


class SharingMovingToDestinationState(SharingStrategyBehaviour):
    """
    Represents the state where the sharing transport is moving
    with the customer towards the destination.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION

        logger.debug(
            "Agent[{}]: The sharing transport is moving "
            "to the customer destination.".format(
                self.agent.name
            )
        )

    async def run(self):

        customers = (
            self.agent.get("current_customer")
            or {}
        )

        if not customers:
            logger.warning(
                "Agent[{}]: The sharing transport is moving "
                "without a current customer.".format(
                    self.agent.name
                )
            )

            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()

            self.set_next_state(
                TRANSPORT_WAITING
            )
            return

        customer_id = next(
            iter(customers.keys())
        )

        if not self.agent.is_in_destination():

            msg = await self.receive(timeout=1)

            if msg:

                try:
                    content = json.loads(msg.body)

                except (json.JSONDecodeError, TypeError):
                    content = {}

                performative = msg.get_metadata(
                    "performative"
                )

                # A customer may try to book the transport using
                # an outdated candidate list.
                if performative == PROPOSE_PERFORMATIVE:

                    new_customer_id = content.get(
                        "customer_id"
                    )

                    if new_customer_id is not None:

                        await self.refuse_customer(
                            new_customer_id
                        )

            self.set_next_state(
                TRANSPORT_MOVING_TO_DESTINATION
            )
            return

        logger.info(
            "Agent[{}]: The sharing transport reached "
            "the destination with customer [{}].".format(
                self.agent.name,
                customer_id
            )
        )

        await self.inform_customer(
            customer_id=customer_id,
            status=CUSTOMER_IN_DEST
        )

        self.agent.remove_customer_in_transport(
            customer_id
        )

        self.agent.increment_completed_assignments()

        self.agent.status = TRANSPORT_WAITING

        self.agent.set_available()

        self.set_next_state(
            TRANSPORT_WAITING
        )
        return

class FSMSharingStrategyBehaviour(FSMSimfleetBehaviour):
    """
    Finite State Machine behaviour for a free-floating sharing transport.
    """

    def setup(self):

        # States
        self.add_state(
            TRANSPORT_WAITING,
            SharingWaitingState(),
            initial=True
        )

        self.add_state(
            TRANSPORT_BOOKED,
            SharingBookedState()
        )

        self.add_state(
            TRANSPORT_MOVING_TO_DESTINATION,
            SharingMovingToDestinationState()
        )

        # Waiting
        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_BOOKED
        )

        # Booked
        self.add_transition(
            TRANSPORT_BOOKED,
            TRANSPORT_BOOKED
        )

        self.add_transition(
            TRANSPORT_BOOKED,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_BOOKED,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        # Moving to destination
        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_WAITING
        )



