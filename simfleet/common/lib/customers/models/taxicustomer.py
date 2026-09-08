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
    Base operational strategy for TaxiCustomerAgent.

    The behaviour provides the common REQUEST_PROTOCOL messaging primitives
    used by taxi-customer strategies:

    - request transport service from configured FleetManagers;
    - accept a transport proposal;
    - refuse a transport proposal;
    - inform the selected transport of customer status changes.

    Concrete subclasses implement the proposal-selection and service
    progression policy in ``run()``.

    The behaviour also participates in multimodal orchestration. When the
    complete Taxi customer strategy terminates, ``on_end()`` invokes the
    generic customer completion hook. Legacy TaxiCustomerAgent treats that
    hook as a no-op, while MultiModalCustomerAgent converts it into explicit
    modal completion signalling.
    """

    async def on_start(self):
        """
        Start the Taxi customer strategy lifecycle.

        The generic StrategyBehaviour hook runs first, emitting
        ``initial_event``, after which Taxi-specific strategy startup is logged.
        """
        await super().on_start()
        logger.debug(
            "Agent[{}]: Strategy {} started.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def on_end(self):
        """
        Finalize Taxi customer strategy execution.

        The generic StrategyBehaviour end hook emits ``final_event`` first. The
        Taxi customer completion hook is then invoked so multimodal customers can
        notify their orchestration FSM that the current modal strategy has ended.

        For legacy TaxiCustomerAgent instances, the completion hook remains a
        no-op.
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
        Send one Taxi service request to every configured FleetManager.

        REQUEST_PROTOCOL / REQUEST_PERFORMATIVE is used for each FleetManager.

        When no explicit payload is supplied, the default request contains:

        - customer JID;
        - current customer position as origin;
        - current customer destination.

        If the customer has no destination, the current implementation attempts
        to generate a random routable destination before creating the request.

        Args:
            content (dict | None): Optional request payload. None or an empty
                mapping causes the default Taxi request payload to be built.
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
        Accept one Taxi transport proposal.

        An ACCEPT_PERFORMATIVE message is sent through REQUEST_PROTOCOL containing
        the customer identifier, current origin, and requested destination.

        After sending the acceptance, the selected transport JID is stored in the
        Taxi customer's transient assignment context.

        Args:
            transport_id (str): Transport JID whose proposal is accepted.
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
        Refuse one Taxi transport proposal.

        A REFUSE_PERFORMATIVE message is sent through REQUEST_PROTOCOL containing
        the same customer, origin, and destination context used for acceptance.

        Refusing a proposal does not change the currently stored Taxi transport
        assignment.

        Args:
            transport_id (str): Transport JID whose proposal is refused.
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
        Inform the selected Taxi transport of a customer status update.

        The message uses REQUEST_PROTOCOL / INFORM_PERFORMATIVE. ``status`` is
        inserted into the supplied payload before serialization.

        Any status other than ``CUSTOMER_IN_DEST`` keeps or updates the transient
        Taxi assignment to ``transport_id``. Destination completion clears that
        assignment.

        Args:
            transport_id (str): Transport JID receiving the update.
            status (str): Customer status included in the message.
            data (dict | None): Optional additional payload fields.
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
        Execute one iteration of the concrete Taxi customer strategy.

        Subclasses implement proposal handling, customer state progression, and
        interaction with the assigned transport.

        Raises:
            NotImplementedError: When no concrete Taxi customer strategy is
                implemented.
        """
        raise NotImplementedError
