import json
from loguru import logger
from spade.behaviour import CyclicBehaviour
from spade.message import Message
from spade.template import Template

from simfleet.utils.abstractstrategies import StrategyBehaviour
from simfleet.common.agents.fleetmanager import FleetManagerAgent

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REGISTER_PROTOCOL,
    ACCEPT_PERFORMATIVE,
    REQUEST_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    INFORM_PERFORMATIVE
)

from spade.presence import PresenceManager
from spade.presence import PresenceType, PresenceShow, PresenceInfo

class LogisticFleetManagerAgent(FleetManagerAgent):
    """
        The VehicleAgent class represents a vehicle in the system. It inherits from both MovableMixin and GeoLocatedAgent,
        combining the functionality of movement and geolocation. This agent can register with a fleet manager, move to a
        destination, and execute strategies defined by specific behaviors.

        Attributes:
            fleetmanager_id (str): The ID of the fleet manager the vehicle is registered with.
    """
    def __init__(self, agentjid, password):
        """
            Initializes the VehicleAgent with its unique JID and password. The vehicle agent also has attributes
            to store the fleet manager's ID and manages its own state regarding its location and registration.

            Args:
                agentjid (str): The Jabber ID of the agent.
                password (str): The password used for agent authentication.
        """
        super().__init__(agentjid, password)

        self.fleetmanager_id = None
        self.vehicle_dest = None

    async def setup(self):
        """
            Sets up the vehicle agent, registers it with the fleet manager, and ensures that
            the agent has the required behaviors for communication.
        """
        await super().setup()

        logger.info("LogisticFleetManager agent {} running".format(self.name))

        # Presence
        # -------------------------
        self.presence.on_subscribe = self.on_subscribe
        self.presence.on_subscribed = self.on_subscribed
        self.presence.on_available = self.on_available
        # -------------------------



    # Presence
    # -------------------------
    def on_available(self, peer_jid, presence_info, last_presence):
        """Marca un transporte como 'available' cuando se vuelve activo."""
        logger.info(f"[{self.name}] Agent {peer_jid.split('@')[0]} is {presence_info.show.value}")

    def on_subscribed(self, peer_jid):
        logger.info(f"[{self.name}] Agent {peer_jid.split('@')[0]} has accepted the subscription")
        contacts = self.presence.get_contacts()
        logger.info(f"[{self.name}] Contacts List: {contacts}")

        self.presence.set_presence(
            presence_type=PresenceType.AVAILABLE,
            show=PresenceShow.CHAT,
            status="FleetManager ready",
        )
        # self.presence.subscribe(str(peer_jid))

    def on_subscribe(self, peer_jid):
        logger.info(f"[{self.name}] Agent {peer_jid.split('@')[0]} asked for subscription. Let's approve it")
        self.presence.approve_subscription(peer_jid)
        self.presence.subscribe(peer_jid)

    # -------------------------

    def _is_available(self, presence_type):

        if presence_type is None:
            return False

        try:
            if presence_type == PresenceType.AVAILABLE:
                return True
        except Exception:
            pass

class LogisticFleetManagerStrategyBehaviour(StrategyBehaviour):
    """
    The FleetManagerStrategyBehaviour class defines the main strategy for coordinating customer and transport
    agents in the fleet. This behavior needs to implement a `_process` method for custom strategies.
    """

    async def on_start(self):
        """
            Logs that the strategy has started in the Fleet Manager.
        """
        logger.debug("Strategy {} started in manager".format(type(self).__name__))

    def get_transport_agents(self):
        """
        Returns the list of transport agents currently registered with the FleetManager.

        Returns:
            list: A list of transport agents.
        """
        return self.get("transport_agents")


    async def send_registration(self):
        """
        Sends a registration request to the directory service to register the FleetManager.
        """
        logger.info(
            "Manager {} sent proposal to register to directory {}".format(
                self.agent.name, self.agent.directory_id
            )
        )
        content = {"jid": str(self.agent.jid), "type": self.agent.fleet_type}
        msg = Message()
        msg.to = str(self.agent.directory_id)
        msg.set_metadata("protocol", REGISTER_PROTOCOL)
        msg.set_metadata("performative", REQUEST_PERFORMATIVE)
        msg.body = json.dumps(content)
        await self.send(msg)

    async def inform_customer(self, customer_id, data=None):
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
        msg.body = json.dumps(data)
        await self.send(msg)

    async def run(self):
        """
            A placeholder method that needs to be implemented by any subclass defining specific fleet management strategies.
        """
        raise NotImplementedError
