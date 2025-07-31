import asyncio
import json

from loguru import logger
from spade.behaviour import State
from spade.template import Template
from spade.message import Message

from simfleet.common.agents.transport import TransportAgent

from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_IN_CUSTOMER_PLACE, TRANSPORT_MOVING_TO_STATION, TRANSPORT_MOVING_TO_DESTINATION, CUSTOMER_IN_DEST, CUSTOMER_LOCATION

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    INFORM_PERFORMATIVE,
    ACCEPT_PERFORMATIVE
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination
)

class SharingAgent(TransportAgent):

    def __init__(self, agentjid, password, **kwargs):
        super().__init__(agentjid, password)

        self.fleetmanager_id = kwargs.get('fleet', None)
        self.current_customer_orig = None
        self.current_customer_dest = None


    async def setup(self):
        self.set_type("transport")
        self.set_status()

        await super().setup()

        #try:
        #    template = Template()
        #    template.set_metadata("protocol", REGISTER_PROTOCOL)
        #    register_behaviour = RegistrationBehaviour()
        #    self.add_behaviour(register_behaviour, template)
        #    while not self.has_behaviour(register_behaviour):
        #        logger.warning("Transport {} could not create RegisterBehaviour. Retrying...".format(self.agent_id))
        #        self.add_behaviour(register_behaviour, template)
        #    self.ready = True
        #except Exception as e:
        #    logger.error("EXCEPTION creating RegisterBehaviour in Transport {}: {}".format(self.agent_id, e))

    #Analizar si quitarlo
    def set_type(self, transport_type):  # new
        self.transport_type = transport_type

    def set_status(self, state=TRANSPORT_WAITING):  # new
        self.status = state

    def is_customer_in_transport(self):
        return self.get("customer_in_transport") is not None

    async def drop_customer(self):
        """
        Drops the customer that the transport is carring in the current location.
        """
        customers = self.get("current_customer")
        customer_id = next(iter(customers.items()))[0]

        #await self.inform_customer(customer_id, CUSTOMER_IN_DEST)
        self.status = TRANSPORT_WAITING
        logger.info("Transport {} has dropped the customer {} in destination.".format(self.agent_id,
                                                                                      customer_id))

        self.remove_customer_in_transport(customer_id)

        #self.set("current_customer", None)
        #self.set("current_customer", {})
        self.set("customer_in_transport", None)

    async def set_position(self, coords=None):
        """
        Sets the position of the transport. If no position is provided it is located in a random position.

        Args:
            coords (list): a list coordinates (longitude and latitude)
        """

        await super().set_position(coords)
        self.set("current_pos", coords)

        # if the transport has arrived to its self.dest position
        if self.is_in_destination():
            logger.info("Transport {} has arrived to destination. Status: {}".format(self.agent_id, self.status))
            await self.drop_customer()



class SharingStrategyBehaviour(State):
    """
    Class from which to inherit to create a transport strategy.
    You must overload the ```run`` coroutine

    Helper functions:
        * ``pick_up_customer``
        * ``send_proposal``
        * ``cancel_proposal``
    """

    async def on_start(self):
        logger.debug("Strategy {} started in transport {}".format(type(self).__name__, self.agent.name))
        self.agent.total_waiting_time = 0.0

    async def send_status_fleetmanager(self):
        msg = Message()
        msg.to = str(self.agent.fleetmanager_id)
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        msg.body = json.dumps({
            "name": self.agent.name,
            "jid": str(self.agent.jid),
            "status": self.agent.status,
            "position": self.agent.get_position()
        })
        await self.send(msg)

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


    async def pick_up_customer_in_station(self, customer_id, origin, dest):
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
                self.agent.set_registration(status=False)  # Registro esta a FALSE para que se registre nuevamente en la estación.
                self.agent.status = TRANSPORT_WAITING
            else:
                logger.info("Transport {} has picked up the customer {}.".format(
                    self.agent.agent_id, customer_id))

    async def accept_customer(self, customer_id):
        """
        Sends a ``spade.message.Message`` to a customer to accept a booking.
        It uses the REQUEST_PROTOCOL and the ACCEPT_PERFORMATIVE.

        Args:
            customer_id (str): The Agent JID of the transport
        """
        # COPIED FROM customer.py AND MODIFIED, MIGHT REQUIRE FURTHER MODIFICATION
        reply = Message()
        reply.to = str(customer_id)
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", ACCEPT_PERFORMATIVE)
        content = {
            "transport_id": str(self.agent.jid),
            "position": self.agent.get("current_pos")
        }
        reply.body = json.dumps(content)
        await self.send(reply)
        self.agent.set("current_costumer", customer_id)
        logger.info("Transport {} accepted booking from customer {}".format(self.agent.name, customer_id))

    async def refuse_customer(self, customer_id):
        """
        Sends an ``spade.message.Message`` to a customer to refuse a booking.
        It uses the REQUEST_PROTOCOL and the REFUSE_PERFORMATIVE.

        Args:
            customer_id (str): The Agent JID of the transport
        """
        # COPIED FROM customer.py AND MODIFIED, MIGHT REQUIRE FURTHER MODIFICATION
        reply = Message()
        reply.to = str(customer_id)
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", REFUSE_PERFORMATIVE)
        content = {
            "transport_id": str(self.agent.jid),
            "position": self.agent.get("current_pos")
        }
        reply.body = json.dumps(content)

        await self.send(reply)
        logger.info("Transport {} refused booking from customer {}".format(self.agent.name,
                                                                           customer_id))

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
