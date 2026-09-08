import json

from loguru import logger
from spade.message import Message
from spade.template import Template

from simfleet.utils.helpers import new_random_position

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    INFORM_PERFORMATIVE,
)

from simfleet.common.agents.customer import CustomerAgent
from simfleet.utils.abstractstrategies import StrategyBehaviour

class TaxiCustomerAgent(CustomerAgent):

    """
    Customer model for on-demand taxi services.

    TaxiCustomerAgent extends the common customer state with the transport
    currently assigned to the active taxi service.

    FleetManager discovery, customer destination, position updates, and
    generic travel handling remain responsibilities of CustomerAgent.

    Taxi request negotiation and service progression are implemented by the
    configured taxi customer strategy.
    """

    def __init__(self, agentjid, password):
        """
        Initialize the common customer infrastructure and Taxi capability.

        Args:
            agentjid (str): XMPP JID used by the customer.
            password (str): XMPP authentication password.
        """
        CustomerAgent.__init__(self, agentjid, password)
        self._init_taxi_state()

    def _init_taxi_state(self):
        """
        Initialize state owned exclusively by the Taxi customer capability.

        The helper is intentionally separate from ``__init__`` so
        MultiModalCustomerAgent can initialize Taxi state without executing the
        complete TaxiCustomerAgent constructor through multiple inheritance.
        """
        self.transport_assigned = None


    def set_transport_assigned(self, transport_id):
        """
        Store the taxi transport assigned to the current service.

        Args:
            transport_id (str): Assigned transport JID.
        """
        self.transport_assigned = transport_id

    def clear_transport_assigned(self):
        """
        Clear the taxi transport assigned to the current service.
        """
        self.transport_assigned = None

    def reset_taxi_context(self):
        """
        Reset transient state owned by the completed Taxi service.

        The generic customer destination, physical position, FleetManagers, and
        accumulated metrics are intentionally not modified here.
        """
        self.clear_transport_assigned()


    def run_strategy(self):
        """
        Start the configured Taxi customer strategy once.

        Taxi service negotiation uses REQUEST_PROTOCOL. Generic
        TRAVEL_PROTOCOL position updates remain handled by the inherited
        TravelBehaviour.

        ``running_strategy`` prevents duplicate strategy instances.
        """
        if not self.running_strategy:
            template1 = Template()
            template1.set_metadata("protocol", REQUEST_PROTOCOL)
            self.add_behaviour(self.strategy(), template1)
            self.running_strategy = True


class TaxiCustomerStrategyBehaviour(StrategyBehaviour):
    """
    Represents the strategy behavior for the TaxiCustomerAgent.
    It defines the communication protocol and decision-making processes for requesting,
    accepting, and managing transport services.

    Methods:
        async send_request(content=None):
            Sends a transport request to the fleet manager(s).
        async accept_transport(transport_id):
            Accepts a transport proposal from a transport agent.
        async refuse_transport(transport_id):
            Refuses a transport proposal from a transport agent.
        async inform_transport(transport_id, status, data=None):
            Sends a message to a transport agent to inform about a status update.
        async run():
            Abstract method that must be implemented in a subclass to define behavior.
    """

    async def on_start(self):
        await super().on_start()
        logger.debug(
            "Agent[{}]: Strategy {} started.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def on_end(self):
        """
        Finalize the Taxi strategy and notify customer orchestration.
        """
        await super().on_end()

        logger.debug(
            "Agent[{}]: Strategy {} finished.".format(
                self.agent.name,
                type(self).__name__,
            )
        )

        self.agent.notify_modal_completion()

    async def send_request(self, content=None):
        """
        Sends a transport request to the fleet manager(s).
        Uses the REQUEST_PROTOCOL and REQUEST_PERFORMATIVE.

        Args:
            content (dict): Optional dictionary containing request details.
                            If not provided, a default content with customer ID,
                            origin, and destination will be used.
        """
        if not self.agent.customer_dest:
            self.agent.customer_dest = new_random_position(self.agent.boundingbox, self.agent.route_host, self.route_profile)

        if content is None or len(content) == 0:
            content = {
                "customer_id": str(self.agent.jid),
                "origin": self.agent.get("current_pos"),
                "dest": self.agent.customer_dest,
            }

        if self.agent.get_fleetmanagers() is not None:
            for (
                fleetmanager
            ) in self.agent.fleetmanagers.keys():
                msg = Message()
                msg.to = str(fleetmanager)
                msg.set_metadata("protocol", REQUEST_PROTOCOL)
                msg.set_metadata("performative", REQUEST_PERFORMATIVE)
                msg.body = json.dumps(content)
                await self.send(msg)
            logger.info(
                "Agent[{}]: The agent asked for a transport to ({}).".format(
                    self.agent.name, self.agent.customer_dest
                )
            )
        else:
            logger.warning("Agent[{}]: The agent has no fleet managers.".format(self.agent.name))

    async def accept_transport(self, transport_id):
        """
        Sends a message to a transport agent to accept a travel proposal.
        Uses the REQUEST_PROTOCOL and ACCEPT_PERFORMATIVE.

        Args:
            transport_id (str): The JID of the transport agent to accept.
        """
        reply = Message()
        reply.to = str(transport_id)
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", ACCEPT_PERFORMATIVE)
        content = {
            "customer_id": str(self.agent.jid),
            "origin": self.agent.get("current_pos"),
            "dest": self.agent.customer_dest,
        }
        reply.body = json.dumps(content)
        await self.send(reply)
        self.agent.set_transport_assigned(str(transport_id))
        logger.info(
            "Agent[{}]: The agent accepted proposal from transport [{}]".format(
                self.agent.name, transport_id
            )
        )

    async def refuse_transport(self, transport_id):
        """
        Sends a message to a transport agent to refuse a travel proposal.
        Uses the REQUEST_PROTOCOL and REFUSE_PERFORMATIVE.

        Args:
            transport_id (str): The JID of the transport agent to refuse.
        """
        reply = Message()
        reply.to = str(transport_id)
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", REFUSE_PERFORMATIVE)
        content = {
            "customer_id": str(self.agent.jid),
            "origin": self.agent.get("current_pos"),
            "dest": self.agent.customer_dest,
        }
        reply.body = json.dumps(content)

        await self.send(reply)
        logger.info(
            "Agent[{}]: The agent refused proposal from transport [{}]".format(
                self.agent.name, transport_id
            )
        )

    async def inform_transport(self, transport_id, status, data=None):
        """
        Sends a message to a transport agent to inform it of a status update.
        Uses the REQUEST_PROTOCOL and INFORM_PERFORMATIVE.

        Args:
            transport_id (str): The JID of the transport agent.
            status (str): The status to be informed.
            data (dict): Optional additional data to be included in the message.
        """
        if data is None:
            data = {}
        reply = Message()
        reply.to = str(transport_id)
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", INFORM_PERFORMATIVE)
        data["status"] = status
        reply.body = json.dumps(data)
        await self.send(reply)
        #self.agent.transport_assigned = str(transport_id)
        if status != "CUSTOMER_IN_DEST":
            self.agent.set_transport_assigned(str(transport_id))
        else:
            self.agent.clear_transport_assigned()
        logger.info(
            "Agent[{}]: The agent informs the transport [{}]".format(
                self.agent.name, transport_id
            )
        )

    async def run(self):
        """
                Abstract method to define the strategy's behavior.
                This method must be implemented in the child class.
        """
        raise NotImplementedError
