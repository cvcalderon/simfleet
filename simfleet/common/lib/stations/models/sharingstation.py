import json

from loguru import logger
from asyncio import CancelledError
from spade.message import Message
from spade.template import Template
from spade.behaviour import CyclicBehaviour


from simfleet.communications.protocol import (
        ACCEPT_PERFORMATIVE,
        INFORM_PERFORMATIVE,
        REGISTER_PROTOCOL,
        REQUEST_PERFORMATIVE,
        REQUEST_PROTOCOL
)

from simfleet.common.agents.station.servicestationagent import ServiceStationAgent
from simfleet.utils.status import TRANSPORT_IN_CUSTOMER_PLACE


class SharingStationAgent(ServiceStationAgent):
    """
        Represents a bus stop agent that manages customer registrations,
        informs customers about bus arrivals, and handles the bus stop status.

        Methods:
            setup(): Initializes the bus stop agent and sets up its behavior templates.
            set_name(name): Sets the name of the bus stop.
            set_status(state): Sets the status of the bus stop.
            set_type(station_type): Sets the type of the bus stop.
            set_lines(lines): Sets the bus lines that serve the stop.
            to_json(): Returns the bus stop information in JSON format.
            run_strategy(): Sets the strategy behavior for the stop.
    """
    def __init__(self, agentjid, password, **kwargs):
        ServiceStationAgent.__init__(agentjid, password)

        # Bus stop attributes
        self.station_name = None
        self.service_name = None

        # Atribut afegit perque funcione
        self.type = None

    async def setup(self):
        """
            Sets up the bus stop agent by defining its type and behaviors.
        """
        await super().setup()
        logger.info("Stop agent {} running".format(self.name))
        self.set_type("sharing-station")
        #self.set_type("stop")
        self.set_service_name("sharing-station")
        try:
            template1 = Template()
            template1.set_metadata("protocol", REGISTER_PROTOCOL)
            register_behaviour = RegistrationBehaviour()

            template2 = Template()
            template2.set_metadata("protocol", REQUEST_PROTOCOL)
            template2.set_metadata("performative", INFORM_PERFORMATIVE)
            sharing_station_strategy_behaviour = SharingStationStrategyBehaviour()

            self.add_behaviour(register_behaviour, template1)
            self.add_behaviour(sharing_station_strategy_behaviour, template2)
            while not self.has_behaviour(register_behaviour):
                logger.warning(
                    "Station {} could not create RegisterBehaviour. Retrying...".format(
                        self.agent_id
                    )
                )
                self.add_behaviour(register_behaviour, template1)


        except Exception as e:
            logger.error(
                "EXCEPTION creating RegisterBehaviour in Station {}: {}".format(
                    self.agent_id, e
                )
            )
        self.ready = True

    #Cambiar en mas lugares
    def set_name(self, name):
        """
            Sets the name of the bus stop.
        """
        self.station_name = name

    def set_service_name(self, name=None):
        """
        Sets the service_name of the sharing station.
        """
        self.service_name = name

    def set_type(self, station_type):
        """
        Sets the type of the bus stop.
        """
        self.type = station_type


    def to_json(self):
        return {
            "id": self.agent_id,
            "name": self.station_name,
            "position": self.get("current_pos"),
            "icon": self.icon,
        }

    def run_strategy(self):
        """
        Sets the strategy for the stop agent.
        """
        if not self.running_strategy:
            self.running_strategy = True


class RegistrationBehaviour(CyclicBehaviour):
    """
        Handles the registration behavior of the bus stop, allowing it to register with the directory agent.

        Methods:
            on_start(): Initializes the behavior and sets up the logger.
            send_registration(): Sends a registration message to the directory.
            run(): Manages the registration logic and response handling.
    """
    async def on_start(self):
        logger.debug("Strategy {} started in directory".format(type(self).__name__))

    def set_registration(self, decision):
        """
            Sets the registration status of the agent.

            Args:
                decision (bool): Registration decision.
        """
        self.agent.registration = decision

    async def send_registration(self):
        """
        Sends a registration message to the directory agent with the bus stop's information.
        """
        logger.info(
            "Sharing Station {} sent proposal to register to directory {}".format(
                self.agent.name, self.agent.directory_id
            )
        )

        service = self.agent.services_list.get(self.agent.service_name, {})


        content = {
            "jid": str(self.agent.jid),
            "type": "sharing-station",
            "station_name": self.agent.station_name,
            "position": self.get("current_pos"),
            "max_transports": service.get("max_agents"),
            "available_transports": self.agent.available_agents(self.agent.service_name)
        }
        msg = Message()
        msg.to = str(self.agent.directory_id)
        msg.set_metadata("protocol", REGISTER_PROTOCOL)
        msg.set_metadata("performative", REQUEST_PERFORMATIVE)
        msg.body = json.dumps(content)
        await self.send(msg)

    async def run(self):
        try:
            if not self.agent.registration:
                await self.send_registration()
            msg = await self.receive(timeout=10)
            if msg:
                performative = msg.get_metadata("performative")
                if performative == ACCEPT_PERFORMATIVE:
                    self.set_registration(True)
                    logger.debug("Registration in the directory")
        except CancelledError:
            logger.debug("Cancelling async tasks...")
        except Exception as e:
            logger.error(
                "EXCEPTION in RegisterBehaviour of Station {}: {}".format(
                    self.agent.name, e
                )
            )

class SharingStationStrategyBehaviour(CyclicBehaviour):
    """
    Strategy behavior for managing the bus stop operations, including handling customer notifications.

    Methods:
        on_start(): Initializes the behavior and sets up the logger.
        dequeue_customers(): Manages the dequeueing of customers based on their destinations.
        inform_customers(): Informs customers that their transport has arrived.
        run(): Main loop handling incoming messages and customer notifications.
    """

    async def on_start(self):
        logger.debug(
            "Strategy {} started in transport {}".format(
                type(self).__name__, self.agent.name
            )
        )

    async def check_available_place(self, service_name, agent):

        if not self.agent.is_station_full(service_name):
            await self.inform_customer(agent, True)
            logger.debug(f"Agent[{self.name}]: Inform to the agent [{agent}] that the station is not full.")
            return True
        else:
            await self.inform_customer(agent, False)
            logger.warning(f"Agent[{self.name}]: Inform to the agent [{agent}] that the station is full.")
            return False


    async def inform_customer(self, inform_to, available):
        """
            Informs the specified customers that there is a place available.

            Args:
                inform_to (str): Customer to be informed.
                available (bool): Place available for the bike.
        """
        data = {"available_place": available}
        msg = Message()
        msg.to = inform_to
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        msg.body = json.dumps(data)
        await self.send(msg)


    async def run(self):
        """
            Main loop that handles incoming messages and manages customer notifications.
        """
        msg = await self.receive(timeout=30)
        logger.debug("Sharing Station {} received message: {}".format(self.agent.jid, msg))
        if msg:
            sender = msg.sender
            performative = msg.get_metadata("performative")
            content = json.loads(msg.body)
            service_name = content["service_name"]

            if performative == INFORM_PERFORMATIVE:
                await self.check_available_place(service_name, sender)
