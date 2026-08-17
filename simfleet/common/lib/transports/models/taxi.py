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

    def has_return_position(self):
        return self.return_position is not None

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




