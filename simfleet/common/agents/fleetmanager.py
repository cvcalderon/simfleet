import json
from asyncio import CancelledError

from loguru import logger
from spade.behaviour import CyclicBehaviour
from spade.message import Message
from spade.template import Template
from spade.presence import ContactNotFound, PresenceNotFound, PresenceType, PresenceShow

from simfleet.common.simfleetagent import SimfleetAgent

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REGISTER_PROTOCOL,
    ACCEPT_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    REQUEST_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
)
from simfleet.utils.abstractstrategies import StrategyBehaviour


class FleetManagerAgent(SimfleetAgent):
    """
    The FleetManagerAgent is responsible for managing the fleet of vehicle agents. It registers vehicle agents
    into its fleet and coordinates the requests between vehicles and customers.

    Attributes:
        vehicles_in_fleet (int): The number of vehicles currently registered in the fleet.
        fleet_icon (str): The icon representing the fleet in visual representations.
    """

    def __init__(self, agentjid, password):
        """
            Initializes the FleetManager agent with the given JID (Jabber ID) and password. It also initializes
            its internal structures to track the vehicle agents within the fleet.
        """

        super().__init__(agentjid, password)

        self.vehicles_in_fleet = 0
        self.fleet_icon = None
        self.clear_agents()


    def clear_agents(self):
        """
        Clears the stored set of vehicle agents and resets the simulation clock. This method is useful for
        resetting the fleet manager state between simulations or sessions.
        """
        self.set("vehicle_agents", {})

    async def setup(self):
        """
            Sets up the FleetManager agent by registering a behavior that handles the registration of vehicle agents.
            This method is called automatically when the agent is started.
        """
        await super().setup()
        logger.info("FleetManager agent {} running".format(self.name))
        try:
            template = Template()
            template.set_metadata("protocol", REGISTER_PROTOCOL)
            register_behaviour = VehicleRegistrationForFleetBehaviour()
            self.add_behaviour(register_behaviour, template)
            while not self.has_behaviour(register_behaviour):
                logger.warning(
                    "Manager {} could not create RegisterBehaviour. Retrying...".format(
                        self.agent_id
                    )
                )
                self.add_behaviour(register_behaviour, template)
            self.ready = True
        except Exception as e:
            logger.error(
                "EXCEPTION creating RegisterBehaviour in Manager {}: {}".format(
                    self.agent_id, e
                )
            )

    # New implementation v1

    def can_accept_presence_subscription(self, peer_jid):
        return self.is_registered_vehicle(peer_jid)

    def is_registered_vehicle(self, vehicle_jid):
        for vehicle in self.get_vehicle_agents().values():
            if self.is_same_jid(vehicle.get("jid"), vehicle_jid):
                return True

        return False

    def should_subscribe_back(self, peer_jid):
        return self.is_registered_vehicle(peer_jid)

    def get_vehicle_presence(self, vehicle_jid):
        try:
            return self.presence.get_contact_presence(vehicle_jid)

        except (ContactNotFound, PresenceNotFound):
            logger.debug(
                "Agent[{}]: No presence information for vehicle [{}].".format(
                    self.name, vehicle_jid
                )
            )
            return None

    def get_vehicle_presence_data(self, presence):
        if presence is None or not presence.status:
            return None

        try:
            data = json.loads(presence.status)

        except (json.JSONDecodeError, TypeError):
            logger.debug(
                "Agent[{}]: Invalid vehicle presence status: {!r}.".format(
                    self.name, presence.status
                )
            )
            return None

        if not isinstance(data, dict):
            return None

        return data

    def is_vehicle_presence_mirror_available(self, presence):
        if not presence:
            return False

        return (
            presence.get("type") == PresenceType.AVAILABLE.value
            and presence.get("show") == PresenceShow.CHAT.value
        )

    def get_vehicle_presence_mirror_data(self, presence):
        if not presence or not presence.get("status"):
            return None

        try:
            data = json.loads(presence["status"])

        except (json.JSONDecodeError, TypeError):
            logger.debug(
                "Agent[{}]: Invalid mirrored vehicle presence status: {!r}.".format(
                    self.name, presence.get("status")
                )
            )
            return None

        if not isinstance(data, dict):
            return None

        return data

    def is_vehicle_available(self, presence):
        if presence is None:
            return False

        return (
            presence.type == PresenceType.AVAILABLE
            and presence.show == PresenceShow.CHAT
        )

    def get_available_vehicles(self):
        available_vehicles = []

        for vehicle in self.get_vehicle_agents().values():
            vehicle_jid = vehicle.get("jid")

            if not vehicle_jid:
                continue

            presence = self.get_vehicle_presence(vehicle_jid)

            if self.is_vehicle_available(presence):
                data = self.get_vehicle_presence_data(presence)

            else:
                presence = vehicle.get("presence")

                if not self.is_vehicle_presence_mirror_available(presence):
                    continue

                data = self.get_vehicle_presence_mirror_data(presence)

            if data is None:
                continue

            available_vehicles.append(
                {
                    "vehicle": vehicle,
                    "presence": presence,
                    "data": data,
                }
            )

        return available_vehicles

    # ---------------------

    def get_vehicle_agents(self):
        """
        Returns the list of vehicle agents currently registered with the FleetManager.

        Returns:
            list: A list of vehicle agents.
        """
        return self.get("vehicle_agents")

    def set_id(self, agent_id):
        """
        Sets the ID for the agent.

        Args:
            agent_id (str): The new ID for the agent.
        """
        self.agent_id = agent_id

    def set_icon(self, icon):
        """
            Sets the fleet icon for visual representation.

            Args:
                icon (str): The icon identifier for the fleet.
        """
        self.fleet_icon = icon

    def run_strategy(self):
        """
        Runs the fleet management strategy, registering a behavior for handling vehicle and customer requests.
        """
        if not self.running_strategy:
            template = Template()
            template.set_metadata("protocol", REQUEST_PROTOCOL)
            self.add_behaviour(self.strategy(), template)
            self.running_strategy = True


class VehicleRegistrationForFleetBehaviour(CyclicBehaviour):
    """
        This behavior manages the registration of new vehicle agents in the fleet. It receives requests from
        vehicle agents and registers them if their fleet type matches the FleetManager's type.
    """
    async def on_start(self):
        logger.debug("Strategy {} started in manager".format(type(self).__name__))

    def add_vehicle(self, agent):
        """
        Adds a new vehicle agent to the fleet's internal store.

        Args:
            agent (dict): The details of the vehicle agent to be added.
        """
        vehicles = self.get("vehicle_agents")

        if agent["name"] not in vehicles:
            self.agent.vehicles_in_fleet += 1

        vehicles[agent["name"]] = agent

    def remove_vehicle(self, key):
        """
        Removes a vehicle agent from the fleet's internal store by its key.

        Args:
            key (str): The unique key representing the vehicle agent.
        """
        if key in self.get("vehicle_agents"):
            del self.get("vehicle_agents")[key]
            logger.debug(
                "Deregistration of the VehicleAgent {}".format(key)
            )
            self.agent.vehicles_in_fleet -= 1
        else:
            logger.debug(
                "Cancelation of the registration in the Fleet"
            )

    def update_vehicle_presence(self, content):
        vehicle_jid = content.get("jid")

        for vehicle in self.get("vehicle_agents").values():
            if self.agent.is_same_jid(vehicle.get("jid"), vehicle_jid):
                vehicle["presence"] = content
                return True

        return False

    async def accept_registration(self, agent_id):
        """
        Sends an acceptance message to a vehicle agent, confirming its registration in the fleet.

        Args:
            agent_id (str): The ID of the vehicle agent to be accepted.
        """
        reply = Message()
        content = {"icon": self.agent.fleet_icon, "fleet_type": self.agent.fleet_type}
        reply.to = str(agent_id)
        reply.set_metadata("protocol", REGISTER_PROTOCOL)
        reply.set_metadata("performative", ACCEPT_PERFORMATIVE)
        reply.body = json.dumps(content)
        await self.send(reply)

    async def reject_registration(self, agent_id):
        """
        Sends a rejection message to a vehicle agent, declining its registration request.

        Args:
            agent_id (str): The ID of the vehicle agent to be rejected.
        """
        reply = Message()
        reply.to = str(agent_id)
        reply.set_metadata("protocol", REGISTER_PROTOCOL)
        reply.set_metadata("performative", REFUSE_PERFORMATIVE)
        reply.body = ""
        await self.send(reply)

    async def run(self):
        """
            Listens for registration requests from vehicle agents and processes them by accepting or rejecting
            them based on the fleet type.
        """
        try:
            msg = await self.receive(timeout=5)
            if msg:
                performative = msg.get_metadata("performative")
                if performative == REQUEST_PERFORMATIVE:
                    content = json.loads(msg.body)
                    if content["fleet_type"] == self.agent.fleet_type:
                        self.add_vehicle(content)
                        self.agent.subscribe_to_presence(content["jid"])
                        await self.accept_registration(msg.sender)
                        logger.debug(
                            "Registration in the {} fleet to {}".format(self.agent.name,content.get("name"))
                        )
                    else:
                        await self.reject_registration(msg.sender)

                if performative == ACCEPT_PERFORMATIVE:
                    self.agent.set_registration(True)
                    logger.info("Registration in the dictionary of services")

                if performative == INFORM_PERFORMATIVE:
                    content = json.loads(msg.body)

                    if self.update_vehicle_presence(content):
                        logger.debug(
                            "Agent[{}]: updated mirrored presence for [{}].".format(
                                self.agent.name,
                                content.get("jid")
                            )
                        )
        except CancelledError:
            logger.debug("Cancelling async tasks...")
        except Exception as e:
            logger.error(
                "EXCEPTION in RegisterBehaviour of Manager {}: {}".format(
                    self.agent.name, e
                )
            )


class FleetManagerStrategyBehaviour(StrategyBehaviour):
    """
    The FleetManagerStrategyBehaviour class defines the main strategy for coordinating customer and vehicle
    agents in the fleet. This behavior needs to implement a `_process` method for custom strategies.
    """

    async def on_start(self):
        """
            Logs that the strategy has started in the Fleet Manager.
        """
        logger.debug("Strategy {} started in manager".format(type(self).__name__))

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

    async def run(self):
        """
            A placeholder method that needs to be implemented by any subclass defining specific fleet management strategies.
        """
        raise NotImplementedError
