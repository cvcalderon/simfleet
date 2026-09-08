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
    Base model for movable fleet resources.

    VehicleAgent combines geolocation and route-based movement with the
    registration contract used by fleet-managed resources. Concrete vehicle
    models extend this class with modality-specific state and strategies.

    The model is responsible for:

    - movement and target-position state;
    - registration with an optional FleetManager;
    - publication of resource availability through Presence;
    - launching the configured operational strategy;
    - serializing common vehicle movement information.

    Operational decisions belong to the configured strategy rather than to
    this model.
    """
    def __init__(self, agentjid, password):
        """
        Initialize the common runtime state of a vehicle.

        Args:
            agentjid (str): XMPP JID used by the agent.
            password (str): XMPP authentication password.
        """
        GeoLocatedAgent.__init__(self, agentjid, password)
        MovableMixin.__init__(self)

        self.vehicle_dest = None

    async def setup(self):
        """
        Initialize the vehicle and its fleet-registration behaviour.

        Vehicles without a configured registration fleet become ready
        immediately. Otherwise, a RegistrationBehaviour is installed and
        readiness is completed after the FleetManager accepts registration.
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
        """
        Build the common payload sent when registering the vehicle.

        Returns:
            dict: Vehicle identity and fleet type advertised to the
            FleetManager.
        """

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
        Set the vehicle target position.

        When no coordinates are provided, a valid random position is generated
        inside the configured simulation bounding box.

        Args:
            coords (list | None): Target coordinates as longitude and latitude.
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
        Start the configured operational vehicle strategy once.

        The strategy receives messages using REQUEST_PROTOCOL. The
        ``running_strategy`` flag prevents duplicate strategy instances.
        """
        if not self.running_strategy:
            template = Template()
            template.set_metadata("protocol", REQUEST_PROTOCOL)
            self.add_behaviour(self.strategy(), template)
            self.running_strategy = True


    async def set_position(self, coords=None):
        """
        Update the physical position of the vehicle.

        The geolocation state and the vehicle ``current_pos`` value are kept
        synchronized.

        Args:
            coords (list | None): New longitude/latitude coordinates.
        """

        super().set_position(coords)
        self.set("current_pos", coords)

    def get_presence_status(self):
        """
        Build the application payload published through vehicle Presence.

        Returns:
            dict: Current position and operational status.
        """
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
        """
        Publish the current vehicle state through the generic Presence contract.

        Args:
            presence_type: XMPP Presence availability type.
            show: XMPP Presence show value.
            priority (int): XMPP Presence priority.
        """

        status = json.dumps(self.get_presence_status())

        self.set_agent_presence(
            status=status,
            presence_type=presence_type,
            show=show,
            priority=priority
        )

    def set_available(self):
        """
        Advertise the vehicle as available when Presence registration is enabled.
        """

        if not self.get_registration_presence():
            return

        self.publish_presence(
            presence_type=PresenceType.AVAILABLE,
            show=PresenceShow.CHAT
        )

    def set_busy(self):
        """
        Advertise the vehicle as busy when Presence registration is enabled.
        """

        if not self.get_registration_presence():
            return

        self.publish_presence(
            presence_type=PresenceType.AVAILABLE,
            show=PresenceShow.DND
        )


    def to_json(self):
        """
        Serialize the vehicle state used by the simulator and frontend.

        Extends the geolocated-agent representation with destination, travelled
        distance, animation speed, and current route path.

        Returns:
            dict: Serializable vehicle state.
        """
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
    """
    Register a VehicleAgent with its configured FleetManager.

    Registration requests are retried until an acceptance is received.
    Successful registration stores the returned FleetManager information,
    marks the vehicle as ready, and publishes its initial availability.
    """
    async def on_start(self):
        logger.debug("Strategy {} started in transport".format(type(self).__name__))

    async def send_registration(self):
        """
        Send the vehicle registration payload to the configured FleetManager.
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
        """
        Execute one registration cycle and process the FleetManager response.

        ACCEPT completes vehicle registration. REFUSE leaves the behaviour
        active so registration may be retried.
        """
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
    Base SPADE State shared by generic VehicleAgent FSM strategies.

    This class is not a complete FSM behaviour. Concrete vehicle states inherit
    from it to reuse common state startup logging and route initiation through
    ``planned_trip()``.

    The surrounding FSM defines state registration, transitions, and complete
    strategy lifecycle instrumentation through FSMSimfleetBehaviour.

    Concrete state subclasses implement ``run()``.
    """

    async def on_start(self):
        """
        Log entry into a concrete Vehicle FSM state.

        Generic FSM ``initial_event`` instrumentation belongs to the surrounding
        FSMSimfleetBehaviour, not to individual Vehicle states.
        """
        logger.debug("Strategy {} started in vehicle".format(type(self).__name__))

    async def planned_trip(self, dest=None):
        """
        Start MovableMixin route execution toward a vehicle destination.

        The helper delegates physical route planning and movement setup to the
        owning VehicleAgent through ``move_to()``.

        ``AlreadyInDestination`` is handled locally and logged, so callers normally
        receive no exception when the requested destination already equals the
        current position.

        Other movement exceptions, including PathRequestException, are not handled
        here and may be processed by the concrete state.

        Args:
            dest (list | None): Destination coordinates passed to
                VehicleAgent.move_to().
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
        Execute the concrete Vehicle FSM state.

        State subclasses implement their transition and movement decisions.

        Raises:
            NotImplementedError: When the state does not provide an
                implementation.
        """
        raise NotImplementedError
