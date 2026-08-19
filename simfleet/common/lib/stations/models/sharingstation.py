import json

from asyncio import CancelledError

from loguru import logger
from spade.behaviour import CyclicBehaviour
from spade.message import Message
from spade.presence import PresenceType, PresenceShow
from spade.template import Template

from simfleet.communications.protocol import (
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    REGISTER_PROTOCOL,
    REQUEST_PERFORMATIVE,
)

from simfleet.common.agents.station.servicestationagent import (
    ServiceStationAgent
)

from spade.presence import (
    PresenceType,
    PresenceShow,
)

class SharingStationAgent(ServiceStationAgent):
    """
    Represents a station-based sharing station.

    The station manages a dynamic inventory of shared transports
    and publishes its operational state to its FleetManager.

    Shared transports register directly with the station, while
    the station registers with its FleetManager.
    """

    def __init__(self, agentjid, password):
        super().__init__(agentjid, password)

    async def setup(self):
        await super().setup()

        logger.info(
            "Agent[{}]: Sharing station running".format(
                self.name
            )
        )

        try:
            template = Template()
            template.set_metadata(
                "protocol",
                REGISTER_PROTOCOL
            )

            register_behaviour = (
                SharingStationRegistrationBehaviour()
            )

            self.add_behaviour(
                register_behaviour,
                template
            )

            while not self.has_behaviour(
                register_behaviour
            ):
                logger.warning(
                    "Agent[{}]: Could not create "
                    "SharingStationRegistrationBehaviour. "
                    "Retrying...".format(
                        self.name
                    )
                )

                self.add_behaviour(
                    register_behaviour,
                    template
                )

        except Exception as e:
            logger.error(
                "EXCEPTION creating registration behaviour "
                "in Sharing Station [{}]: {}".format(
                    self.name,
                    e
                )
            )

    def get_capacity(self):
        """
        Returns the physical capacity of the station.
        """
        service = self.services_list.get(
            self.fleet_type,
            {}
        )

        if service.get("mode") != "agent":
            return 0

        return service.get(
            "max_agents",
            0
        )

    def get_available_bikes(self):
        """
        Returns the number of bikes currently available
        at the station.
        """
        service = self.services_list.get(
            self.fleet_type,
            {}
        )

        if service.get("mode") != "agent":
            return 0

        return len(
            service.get(
                "agents",
                []
            )
        )

    def get_available_docks(self):
        """
        Returns the number of currently available docks.
        """
        capacity = self.get_capacity()
        available_bikes = self.get_available_bikes()

        return max(
            capacity - available_bikes,
            0
        )

    def has_available_bikes(self):
        return self.get_available_bikes() > 0

    def has_available_docks(self):
        return self.get_available_docks() > 0

    def is_bike_registered(self, bike_jid):
        """
        Checks whether a bike currently belongs to this station.
        """
        service = self.services_list.get(
            self.fleet_type,
            {}
        )

        if service.get("mode") != "agent":
            return False

        bike_jid = str(
            bike_jid
        )

        for registered_bike in service.get(
            "agents",
            []
        ):
            if str(registered_bike) == bike_jid:
                return True

        return False

    def register_bike(self, bike_jid):
        """
        Registers a bike in the station inventory.

        Returns True when the bike belongs to the station after
        the operation and False when the station cannot accept it.
        """
        if self.is_bike_registered(
            bike_jid
        ):
            return True

        if not self.has_available_docks():
            return False

        self.register_agent(
            self.fleet_type,
            str(bike_jid)
        )

        return self.is_bike_registered(
            bike_jid
        )

    def assign_bike(self):
        """
        Removes and returns one available bike from the station.
        """
        return self.assign_agent(
            self.fleet_type
        )

    def get_presence_status(self):
        """
        Returns the dynamic station information published
        through Presence.
        """
        return {
            "p": self.get_position(),
            "b": self.get_available_bikes(),
            "d": self.get_available_docks(),
            "c": self.get_capacity(),
        }

    def publish_station_presence(self):

        if not self.registration:
            return

        if not self.get_registration_presence():
            return

        self.set_agent_presence(
            status=json.dumps(
                self.get_presence_status()
            ),
            presence_type=PresenceType.AVAILABLE,
            show=PresenceShow.CHAT,
        )

    def on_agent_assigned(
        self,
        service_name,
        agent_jid
    ):

        if service_name != self.fleet_type:
            return

        self.publish_station_presence()

    def to_json(self):
        data = super().to_json()

        data.update(
            {
                "available_bikes": self.get_available_bikes(),
                "available_docks": self.get_available_docks(),
                "capacity": self.get_capacity(),
            }
        )

        return data

    def run_strategy(self):
        if not self.running_strategy:
            self.running_strategy = True




class SharingStationRegistrationBehaviour(CyclicBehaviour):

    async def on_start(self):
        logger.debug(
            "Strategy {} started in sharing station [{}]".format(
                type(self).__name__,
                self.agent.name
            )
        )

    async def send_station_registration(self):
        fleetmanager_id = (
            self.agent.get_registration_fleet()
        )

        if fleetmanager_id is None:
            return

        logger.debug(
            "Agent[{}]: Sending registration to "
            "FleetManager [{}].".format(
                self.agent.name,
                fleetmanager_id
            )
        )

        content = {
            "name": self.agent.name,
            "jid": str(self.agent.jid),
            "fleet_type": self.agent.fleet_type,
        }

        msg = Message()
        msg.to = str(
            fleetmanager_id
        )

        msg.set_metadata(
            "protocol",
            REGISTER_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            REQUEST_PERFORMATIVE
        )

        msg.body = json.dumps(
            content
        )

        await self.send(
            msg
        )

    async def accept_bike_registration(self, bike_jid):
        content = {
            "icon": self.agent.icon,
            "fleet_type": self.agent.fleet_type,
        }

        reply = Message()
        reply.to = str(
            bike_jid
        )

        reply.set_metadata(
            "protocol",
            REGISTER_PROTOCOL
        )

        reply.set_metadata(
            "performative",
            ACCEPT_PERFORMATIVE
        )

        reply.body = json.dumps(
            content
        )

        await self.send(
            reply
        )

    async def reject_bike_registration(self, bike_jid):
        reply = Message()
        reply.to = str(
            bike_jid
        )

        reply.set_metadata(
            "protocol",
            REGISTER_PROTOCOL
        )

        reply.set_metadata(
            "performative",
            REFUSE_PERFORMATIVE
        )

        reply.body = json.dumps(
            {
                "fleet_type": self.agent.fleet_type
            }
        )

        await self.send(
            reply
        )

    # def publish_station_presence(self):
    #     if not self.agent.get_registration_presence():
    #         return
    #
    #     self.agent.set_agent_presence(
    #         status=json.dumps(
    #             self.agent.get_presence_status()
    #         ),
    #         presence_type=PresenceType.AVAILABLE,
    #         show=PresenceShow.CHAT,
    #     )

    async def process_bike_registration(self, msg):
        try:
            content = json.loads(
                msg.body
            )
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: Invalid bike registration "
                "request from [{}].".format(
                    self.agent.name,
                    msg.sender
                )
            )

            await self.reject_bike_registration(
                msg.sender
            )

            return

        bike_jid = content.get(
            "jid"
        )

        fleet_type = content.get(
            "fleet_type"
        )

        if bike_jid is None:
            await self.reject_bike_registration(
                msg.sender
            )
            return

        if fleet_type != self.agent.fleet_type:
            logger.warning(
                "Agent[{}]: Bike [{}] requested registration "
                "with invalid fleet type [{}].".format(
                    self.agent.name,
                    bike_jid,
                    fleet_type
                )
            )

            await self.reject_bike_registration(
                msg.sender
            )

            return

        if str(msg.sender) != str(bike_jid):
            logger.warning(
                "Agent[{}]: Bike registration sender [{}] "
                "does not match jid [{}].".format(
                    self.agent.name,
                    msg.sender,
                    bike_jid
                )
            )

            await self.reject_bike_registration(
                msg.sender
            )

            return

        registered = self.agent.register_bike(
            bike_jid
        )

        if not registered:
            logger.warning(
                "Agent[{}]: Bike [{}] registration refused. "
                "Station is full.".format(
                    self.agent.name,
                    bike_jid
                )
            )

            await self.reject_bike_registration(
                msg.sender
            )

            return

        await self.accept_bike_registration(
            msg.sender
        )

        self.agent.publish_station_presence()

        logger.info(
            "Agent[{}]: Bike [{}] registered. "
            "Available bikes: {}. "
            "Available docks: {}.".format(
                self.agent.name,
                bike_jid,
                self.agent.get_available_bikes(),
                self.agent.get_available_docks()
            )
        )

    async def process_fleetmanager_accept(self, msg):
        fleetmanager_id = (
            self.agent.get_registration_fleet()
        )

        if fleetmanager_id is None:
            return

        if not self.agent.is_same_jid(
            msg.sender,
            fleetmanager_id
        ):
            return

        self.agent.set_registration(
            True
        )

        self.agent.publish_station_presence()

        self.agent.ready = True

        logger.info(
            "Agent[{}]: Registration in FleetManager "
            "[{}] accepted.".format(
                self.agent.name,
                fleetmanager_id
            )
        )

    async def process_fleetmanager_refuse(self, msg):
        fleetmanager_id = (
            self.agent.get_registration_fleet()
        )

        if fleetmanager_id is None:
            return

        if not self.agent.is_same_jid(
            msg.sender,
            fleetmanager_id
        ):
            return

        logger.warning(
            "Agent[{}]: Registration in FleetManager "
            "[{}] refused.".format(
                self.agent.name,
                fleetmanager_id
            )
        )

    async def run(self):
        try:
            if (
                not self.agent.registration
                and self.agent.get_registration_fleet()
                is not None
            ):
                await self.send_station_registration()

            msg = await self.receive(
                timeout=10
            )

            if not msg:
                return

            performative = msg.get_metadata(
                "performative"
            )

            if (
                performative
                == REQUEST_PERFORMATIVE
            ):
                await self.process_bike_registration(
                    msg
                )

            elif (
                performative
                == ACCEPT_PERFORMATIVE
            ):
                await self.process_fleetmanager_accept(
                    msg
                )

            elif (
                performative
                == REFUSE_PERFORMATIVE
            ):
                await self.process_fleetmanager_refuse(
                    msg
                )

        except CancelledError:
            logger.debug(
                "Cancelling async tasks..."
            )

        except Exception as e:
            logger.error(
                "EXCEPTION in "
                "SharingStationRegistrationBehaviour "
                "of agent [{}]: {}".format(
                    self.agent.name,
                    e
                )
            )



