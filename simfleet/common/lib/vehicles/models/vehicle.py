import json

from loguru import logger
from asyncio import CancelledError

from spade.message import Message
from spade.template import Template
from spade.behaviour import CyclicBehaviour, State
from spade.presence import PresenceType, PresenceShow

from simfleet.common.mixins.movable import MovableMixin
from simfleet.common.geolocatedagent import GeoLocatedAgent
from simfleet.utils.helpers import new_random_position
#

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REGISTER_PROTOCOL,
    ACCEPT_PERFORMATIVE,
    REQUEST_PERFORMATIVE,
    REFUSE_PERFORMATIVE
)

from simfleet.utils.helpers import AlreadyInDestination

class VehicleAgent(MovableMixin, GeoLocatedAgent):
    """
        The VehicleAgent class represents a vehicle in the system. It inherits from both MovableMixin and GeoLocatedAgent,
        combining the functionality of movement and geolocation. This agent can register with a fleet manager, move to a
        destination, and execute strategies defined by specific behaviors.
    """
    def __init__(self, agentjid, password):
        """
            Initializes the VehicleAgent with its unique JID and password. The vehicle agent also has attributes
            to store the fleet manager's ID and manages its own state regarding its location and registration.

            Args:
                agentjid (str): The Jabber ID of the agent.
                password (str): The password used for agent authentication.
        """
        GeoLocatedAgent.__init__(self, agentjid, password)
        MovableMixin.__init__(self)

        self.vehicle_dest = None

    async def setup(self):
        """
            Sets up the vehicle agent, registers it with the fleet manager, and ensures that
            the agent has the required behaviors for communication.
        """

        await super().setup()

        if not self.get_registration_fleet():
            self.ready = True
            return

        try:
            template = Template()
            template.set_metadata("protocol", REGISTER_PROTOCOL)
            register_behaviour = RegistrationBehaviour()
            self.add_behaviour(register_behaviour, template)
            while not self.has_behaviour(register_behaviour):
                logger.warning(
                    "Agent[{}]: The agent could not create RegisterBehaviour. Retrying...".format(
                        self.agent_id
                    )
                )
                self.add_behaviour(register_behaviour, template)

        except Exception as e:
            logger.error(
                "EXCEPTION creating RegisterBehaviour in agent [{}]: {}".format(
                    self.agent_id, e
                )
            )

    def get_registration_content(self):

        return {
            "name":
                self.name,

            "jid":
                str(self.jid),

            "fleet_type":
                self.fleet_type,
        }


    def set_target_position(self, coords=None):
        """
        Sets the target position of the customer (i.e., its destination).
        If no position is provided, the destination is set to a random position.

        Args:
            coords (list): A list of coordinates (longitude and latitude) for the destination.
        """
        if coords:
            self.vehicle_dest = coords
        else:
            self.vehicle_dest = new_random_position(self.boundingbox, self.route_host, self.route_profile)
        logger.debug(
            "Agent[{}]: The agent target position is ({})".format(self.agent_id, self.vehicle_dest)
        )

    def run_strategy(self):
        """
        Runs the strategy for the vehicle agent. It initializes the behavior associated with the vehicle's operations
        and begins executing its assigned strategy.
        """
        if not self.running_strategy:
            template = Template()
            template.set_metadata("protocol", REQUEST_PROTOCOL)
            self.add_behaviour(self.strategy(), template)
            self.running_strategy = True


    async def set_position(self, coords=None):
        """
        Sets the vehicle's position. If no position is provided, the vehicle will be assigned a random position.

        Args:
            coords (list): A list of coordinates representing the vehicle's longitude and latitude.
        """

        super().set_position(coords)
        self.set("current_pos", coords)

    def get_presence_status(self):
        return {
            "p": self.get_position(),
            "st": self.status,
        }

    def publish_presence(
        self,
        presence_type=PresenceType.AVAILABLE,
        show=PresenceShow.CHAT,
        priority=0
    ):
        status = json.dumps(self.get_presence_status())

        self.set_agent_presence(
            status=status,
            presence_type=presence_type,
            show=show,
            priority=priority
        )

    def set_available(self):
        if not self.get_registration_presence():
            return

        self.publish_presence(
            presence_type=PresenceType.AVAILABLE,
            show=PresenceShow.CHAT
        )

    def set_busy(self):
        if not self.get_registration_presence():
            return

        self.publish_presence(
            presence_type=PresenceType.AVAILABLE,
            show=PresenceShow.DND
        )


    def to_json(self):
        data = super().to_json()
        data.update({
            "dest": [float("{0:.6f}".format(coord)) for coord in self.dest]
            if self.dest
            else None,
            "distance": "{0:.2f}".format(self.total_route_distance),
            "speed": float("{0:.2f}".format(self.animation_speed)) if self.animation_speed else None,
            "path": self.get("path"),
        })
        return data


class RegistrationBehaviour(CyclicBehaviour):
    async def on_start(self):
        logger.debug("Strategy {} started in transport".format(type(self).__name__))

    async def send_registration(self):
        """
        Send a ``spade.message.Message`` with a proposal to manager to register.
        """

        registration_fleet = self.agent.get_registration_fleet()

        logger.debug(
            "Agent[{}]: The agent sent proposal to register to manager [{}]".format(
                #self.agent.name, self.agent.fleetmanager_id
                self.agent.name,
                registration_fleet
            )
        )

        content = (
            self.agent.get_registration_content()
        )

        msg = Message()
        msg.to = str(registration_fleet)
        msg.set_metadata("protocol", REGISTER_PROTOCOL)
        msg.set_metadata("performative", REQUEST_PERFORMATIVE)
        msg.body = json.dumps(content)
        await self.send(msg)


    async def run(self):

        try:

            if not self.agent.registration and self.agent.get_registration_fleet():
                await self.send_registration()


            msg = await self.receive(timeout=10)
            if msg:
                performative = msg.get_metadata("performative")
                if performative == ACCEPT_PERFORMATIVE:
                    content = json.loads(msg.body)
                    self.agent.set_registration(True, content)
                    self.agent.ready = True
                    self.agent.set_available()
                    logger.info(
                        "Agent[{}]: Registration in agent [{}] accepted.".format(
                            self.agent.name, self.agent.get_registration_fleet()
                        )
                    )
                    self.kill(exit_code="Fleet Registration Accepted")
                elif performative == REFUSE_PERFORMATIVE:
                    logger.warning(
                        "Registration in agent was rejected (check fleet type)."
                    )
                    self.kill(exit_code="Fleet Registration Rejected")
        except CancelledError:
            logger.debug("Cancelling async tasks...")
        except Exception as e:
            logger.error(
                "EXCEPTION in RegisterBehaviour of agent [{}]: {}".format(
                    self.agent.name, e
                )
            )

class VehicleStrategyBehaviour(State):
    """
    This class defines the vehicle's behavior strategy. It is designed to be extended to implement
    custom strategies for vehicle operations.

    Key Methods:
        - on_start(): Logs the initialization of the strategy.
        - planned_trip(): Defines how the vehicle should move to its destination.
    """

    async def on_start(self):
        """
            Logs the start of the vehicle's strategy behavior.
        """
        logger.debug("Strategy {} started in vehicle".format(type(self).__name__))

    async def planned_trip(self, dest=None):
        """
        Initiates the process for the vehicle to travel to the specified destination. The vehicle moves along the
        path, updating its position until it reaches its destination.

        Args:
            dest (list): The coordinates of the vehicle's destination.
        """
        logger.info(
            "Agent[{}]: The agent on route to destination ({})".format(self.agent.name, dest)
        )
        try:
            logger.debug("Agent[{}]: The agent move_to destination ({})".format(self.agent.name, dest))
            await self.agent.move_to(dest)
        except AlreadyInDestination:
            logger.debug(
                "Agent[{}]: The agent is already in the destination' ({}) position. . .".format(
                    self.agent.name, dest
                )
            )

    async def run(self):
        """
            Abstract method that should be implemented in subclasses. This is where the specific strategy of the
            vehicle will be executed.
        """
        raise NotImplementedError
