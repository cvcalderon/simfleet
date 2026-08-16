import json
#import ast
from loguru import logger
#from rich.status import Status

from spade.behaviour import State
from spade.message import Message

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    PROPOSE_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    REQUEST_PERFORMATIVE,
#    ACCEPT_PERFORMATIVE,
#    QUERY_PROTOCOL,
)

from simfleet.common.agents.transport import TransportAgent

#from spade.presence import PresenceManager
#from spade.presence import PresenceType, PresenceShow, PresenceInfo

class TaxiAgent(TransportAgent):
    """
        Represents a taxi agent, inheriting from `TransportAgent`. This class provides
        functionalities specific to taxis, such as managing assigned customers.

        Attributes:
            fleetmanager_id (str): The ID of the fleet manager the taxi belongs to.
            assigned_customer (dict): A dictionary storing the currently assigned customer's
                                      ID, origin, and destination.

        Methods:
            async add_assigned_taxicustomer(customer_id, origin=None, dest=None):
                Adds a customer to the taxi's assigned customer list.
            async remove_assigned_taxicustomer():
                Removes all assigned customers from the taxi's customer list.
        """
   # OLD
    def __init__(self, agentjid, password, **kwargs):
        super().__init__(agentjid, password)

        self.initial_position = None
        self.return_position = None
    #    self.set("assigned_customer", {})
        #OLD
        #self.fleetmanager_id = kwargs.get('fleet', None)
        # Presence variable
        #self.status_info = None

    async def setup(self):

        await super().setup()

        #OLD
        # Presence
        # -------------------------
        #self.presence.on_subscribe = self.on_subscribe
        #self.presence.on_subscribed = self.on_subscribed
        #self.presence.on_available = self.on_available
        # -------------------------

        #if self.status_info is None:
        #    self.status_info = (self.get("current_pos"), 0)

        #if self.fleetmanager_id:
        #    self.presence.subscribe(self.fleetmanager_id)

        #self.publish_presence()

    # New implementation v1

    def set_initial_position(self, coords=None):
        super().set_initial_position(coords)

        position = self.get_position()

        if position:
            self.initial_position = list(position)

    def get_initial_position(self):
        return self.initial_position

    def set_return_position(self, position):
        if position:
            self.return_position = list(position)
        else:
            self.return_position = None

    def get_return_position(self):
        return self.return_position

    def clear_return_position(self):
        self.return_position = None

    # ---------------------

    # Presence
    # -------------------------
    # def on_available(self, peer_jid, presence_info, last_presence):
    #     """Marca un transporte como 'available' cuando se vuelve activo."""
    #     logger.info(f"[{self.name}] Agent {peer_jid.split('@')[0]} is {presence_info.show.value}")
    #
    # def on_subscribed(self, peer_jid):
    #     logger.info(f"[{self.name}] Agent {peer_jid.split('@')[0]} has accepted the subscription")
    #     contacts = self.presence.get_contacts()
    #     logger.info(f"[{self.name}] Contacts List: {contacts}")
    #
    #     self.publish_presence()
    #     # self.presence.subscribe(str(peer_jid))
    #
    # def on_subscribe(self, peer_jid):
    #     logger.info(f"[{self.name}] Agent {peer_jid.split('@')[0]} asked for subscription. Let's approve it")
    #     self.presence.approve_subscription(peer_jid)
    #     #self.presence.subscribe(peer_jid)

    # -------------------------


    # def publish_presence(self):
    #     self.presence.set_presence(
    #         presence_type=PresenceType.AVAILABLE,
    #         show=PresenceShow.CHAT,
    #         status=str(self.status_info),
    #     )


    # async def add_assigned_taxicustomer(self, customer_id, origin=None, dest=None):
    #     customers = self.get("assigned_customer")
    #     customers[customer_id] = {"origin": origin, "destination": dest}
    #     self.set("assigned_customer", customers)
    #
    # async def remove_assigned_taxicustomer(self):
    #     self.set("assigned_customer", {})


    # Using Presence
    #-------------------------

    # async def update_presence_info(self, type: str, status: str):
    #
    #     if type == "available":
    #         self.presence.set_presence(
    #             presence_type=PresenceType.AVAILABLE,
    #             show=PresenceShow.CHAT,
    #             status=status,
    #         )
    #
    #     elif type == "busy":
    #         self.presence.set_presence(
    #             presence_type=PresenceType.UNAVAILABLE,
    #             show=PresenceShow.CHAT,
    #             status=status,
    #         )



class TaxiStrategyBehaviour(State):
    """
    Base class to define the transport strategy for a taxi.
    This class should be inherited and extended to create custom strategies.
    Subclasses must override the `run` coroutine to define specific behaviors.

    Methods:
        async on_start():
            Logs the beginning of the strategy execution.
        async on_end():
            Logs the end of the strategy execution.
        async send_proposal(customer_id, content=None):
            Sends a transport proposal to a customer.
        async cancel_proposal(agent_id, content=None):
            Cancels a previously sent proposal to a customer.
        async run():
            Abstract method that must be implemented by subclasses.
    """

    async def on_start(self):
        # await super().on_start()
        logger.debug(
            "Agent[{}]: Strategy {} started.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def on_end(self):
        # await super().on_start()
        logger.debug(
            "Agent[{}]: Strategy {} finished.".format(
                self.agent.name, type(self).__name__
            )
        )

#    async def assigned_taxicustomer(self, customer_id, origin=None, dest=None):

#        await self.agent.add_assigned_taxicustomer(customer_id, origin, dest)

#        await self.agent.update_presence_info(type="busy", status=str(self.agent.status_info))

    async def assigned_taxicustomer(
        self,
        customer_id,
        origin=None,
        dest=None
    ):
        self.agent.add_assigned_customer(
            customer_id,
            origin,
            dest
        )

        self.agent.set_busy()

    async def unassigned_taxicustomer(self):
        self.agent.remove_assigned_customer()

    # async def unassigned_taxicustomer(self):
    #
    #     await self.agent.remove_assigned_taxicustomer()
    #
    #     self.prepare_status_info()
    #
    #     await self.agent.update_presence_info(type="available", status=str(self.agent.status_info))
    #
    # def prepare_status_info(self):
    #
    #     position, value = self.agent.status_info
    #     value += 1
    #
    #     new_info = (self.agent.get("current_pos"), value)
    #
    #     self.agent.status_info = new_info


    async def send_proposal(self, customer_id, content=None):
        """
        Send a ``spade.message.Message`` with a proposal to a customer to pick up him.
        If the content is empty the proposal is sent without content.

        Args:
            customer_id (str): the id of the customer
            content (dict, optional): the optional content of the message
        """
        if content is None:
            content = {}
        logger.info(
            "Agent[{}]: The agent sent proposal to [{}]".format(self.agent.name, customer_id)
        )
        reply = Message()
        reply.to = customer_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", PROPOSE_PERFORMATIVE)
        reply.body = json.dumps(content)
        await self.send(reply)

    async def cancel_proposal(self, agent_id, content=None):
        """
        Send a ``spade.message.Message`` to cancel a proposal.
        If the content is empty the proposal is sent without content.

        Args:
            agent_id (str): the id of the customer
            content (dict, optional): the optional content of the message
        """
        if content is None:
            content = {}
        logger.info(
            "Agent[{}]: The agent sent cancel proposal to [{}]".format(
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
                "Agent[{}]: No fleet manager configured for taxi return.".format(
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
