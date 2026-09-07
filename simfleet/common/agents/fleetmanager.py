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
    The FleetManagerAgent is responsible for managing fleet resources. Resources may be transports,
    vehicles, stations, or other infrastructure agents depending on the fleet strategy.

    Attributes:
        resources_in_fleet (int): The number of resources currently registered in the fleet.
        fleet_icon (str): The icon representing the fleet in visual representations.
    """

    def __init__(self, agentjid, password):
        """
            Initializes the FleetManager agent with the given JID (Jabber ID) and password. It also initializes
            its internal structures to track the resources within the fleet.
        """

        super().__init__(agentjid, password)

        self.resources_in_fleet = 0
        self.fleet_icon = None
        self.clear_agents()


    def clear_agents(self):
        """
        Clears the stored set of resources and resets the simulation clock. This method is useful for
        resetting the fleet manager state between simulations or sessions.
        """
        self.set("fleet_resources", {})

    async def setup(self):
        """
            Sets up the FleetManager agent by registering a behavior that handles the registration of resources.
            This method is called automatically when the agent is started.
        """
        await super().setup()
        logger.info("FleetManager agent {} running".format(self.name))
        try:
            template = Template()
            template.set_metadata("protocol", REGISTER_PROTOCOL)
            register_behaviour = ResourceRegistrationForFleetBehaviour()
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

    def can_accept_presence_subscription(self, peer_jid):
        """
        Accept Presence subscriptions only from registered fleet resources.
        """
        return self.is_registered_resource(peer_jid)

    def is_registered_resource(self, resource_jid):
        """
        Check whether a JID belongs to a resource registered in this fleet.

        Args:
            resource_jid: Resource XMPP identifier.

        Returns:
            bool: True when the resource belongs to the fleet.
        """

        for resource in self.get_fleet_resources().values():
            if self.is_same_jid(resource.get("jid"), resource_jid):
                return True

        return False

    def should_subscribe_back(self, peer_jid):
        """
        Request reciprocal Presence for registered fleet resources.
        """
        return self.is_registered_resource(peer_jid)

    def get_resource_presence(self, resource_jid):
        """
        Return the live XMPP Presence for a registered resource.

        Missing contacts or resources without a Presence entry are treated as
        normal conditions and return None.

        Args:
            resource_jid: Resource XMPP identifier.

        Returns:
            PresenceInfo | None: Live Presence when available.
        """

        try:
            return self.presence.get_contact_presence(resource_jid)

        except (ContactNotFound, PresenceNotFound):
            logger.debug(
                "Agent[{}]: No presence information for resource [{}].".format(
                    self.name, resource_jid
                )
            )
            return None

    def get_resource_presence_data(self, presence):
        """
        Decode the JSON application payload stored in a live Presence status.

        Args:
            presence: XMPP Presence information.

        Returns:
            dict | None: Decoded payload when valid.
        """

        if presence is None or not presence.status:
            return None

        try:
            data = json.loads(presence.status)

        except (json.JSONDecodeError, TypeError):
            logger.debug(
                "Agent[{}]: Invalid resource presence status: {!r}.".format(
                    self.name, presence.status
                )
            )
            return None

        if not isinstance(data, dict):
            return None

        return data

    def is_resource_presence_mirror_available(self, presence):
        """
        Return whether a message-based Presence mirror represents an available resource.
        """

        if not presence:
            return False

        return (
            presence.get("type") == PresenceType.AVAILABLE.value
            and presence.get("show") == PresenceShow.CHAT.value
        )

    def get_resource_presence_mirror_data(self, presence):
        """
        Decode the JSON payload stored in a message-based Presence mirror.

        Returns:
            dict | None: Decoded Presence payload when valid.
        """

        if not presence or not presence.get("status"):
            return None

        try:
            data = json.loads(presence["status"])

        except (json.JSONDecodeError, TypeError):
            logger.debug(
                "Agent[{}]: Invalid mirrored resource presence status: {!r}.".format(
                    self.name, presence.get("status")
                )
            )
            return None

        if not isinstance(data, dict):
            return None

        return data

    def is_resource_available(self, presence):
        """
        Return whether live XMPP Presence represents an available resource.
        """

        if presence is None:
            return False

        return (
            presence.type == PresenceType.AVAILABLE
            and presence.show == PresenceShow.CHAT
        )

    def get_available_resources(self):
        """
        Return fleet resources currently advertised as available.

        Live XMPP Presence is the primary source of truth. When live Presence
        is missing or unavailable, the FleetManager falls back to the
        message-based Presence mirror stored with the resource registration.

        Only resources with valid Presence payloads are returned.

        Returns:
            list[dict]: Available resources together with their Presence
            representation and decoded application data.
        """

        available_resources = []

        for resource in self.get_fleet_resources().values():
            resource_jid = resource.get("jid")

            if not resource_jid:
                continue

            presence = self.get_resource_presence(resource_jid)

            if self.is_resource_available(presence):
                data = self.get_resource_presence_data(presence)

            else:
                presence = resource.get("presence")

                if not self.is_resource_presence_mirror_available(presence):
                    continue

                data = self.get_resource_presence_mirror_data(presence)

            if data is None:
                continue

            available_resources.append(
                {
                    "resource": resource,
                    "presence": presence,
                    "data": data,
                }
            )

        return available_resources

    def get_fleet_resources(self):
        """
        Returns the resources currently registered with the FleetManager.

        Returns:
            dict: Registered fleet resources indexed by name.
        """
        return self.get("fleet_resources")

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
        Runs the fleet management strategy, registering a behavior for handling resource and customer requests.
        """
        if not self.running_strategy:
            template = Template()
            template.set_metadata("protocol", REQUEST_PROTOCOL)
            self.add_behaviour(self.strategy(), template)
            self.running_strategy = True

    def dependencies_ready(self):

        if not super().dependencies_ready():
            return False

        return bool(
            self.registration
        )


class ResourceRegistrationForFleetBehaviour(CyclicBehaviour):
    """
        This behavior manages the registration of new resources in the fleet. It receives requests from
        agents and registers them if their fleet type matches the FleetManager's type.
    """
    async def on_start(self):
        logger.debug("Strategy {} started in manager".format(type(self).__name__))

    def add_resource(self, agent):
        """
        Adds a new resource agent to the fleet's internal store.

        Args:
            agent (dict): The details of the resource agent to be added.
        """
        resources = self.get("fleet_resources")

        if agent["name"] not in resources:
            self.agent.resources_in_fleet += 1

        resources[agent["name"]] = agent

    def remove_resource(self, key):
        """
        Removes a resource agent from the fleet's internal store by its key.

        Args:
            key (str): The unique key representing the resource agent.
        """
        if key in self.get("fleet_resources"):
            del self.get("fleet_resources")[key]
            logger.debug(
                "Deregistration of the fleet resource {}".format(key)
            )
            self.agent.resources_in_fleet -= 1
        else:
            logger.debug(
                "Cancelation of the registration in the Fleet"
            )

    def update_resource_presence(self, content):
        """
        Update the stored message-based Presence mirror for a fleet resource.

        Args:
            content: Presence update containing at least the resource JID.

        Returns:
            bool: True when a matching registered resource was updated.
        """

        resource_jid = content.get("jid")

        for resource in self.get("fleet_resources").values():
            if self.agent.is_same_jid(resource.get("jid"), resource_jid):
                resource["presence"] = content
                return True

        return False

    async def accept_registration(self, agent_id):
        """
        Sends an acceptance message to a resource agent, confirming its registration in the fleet.

        Args:
            agent_id (str): The ID of the resource agent to be accepted.
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
        Sends a rejection message to a resource agent, declining its registration request.

        Args:
            agent_id (str): The ID of the resource agent to be rejected.
        """
        reply = Message()
        reply.to = str(agent_id)
        reply.set_metadata("protocol", REGISTER_PROTOCOL)
        reply.set_metadata("performative", REFUSE_PERFORMATIVE)
        reply.body = ""
        await self.send(reply)

    async def send_registration(self):

        if self.agent.directory_id is None:
            return

        logger.info(
            "Manager {} sent proposal to register "
            "to directory {}".format(
                self.agent.name,
                self.agent.directory_id
            )
        )

        content = {
            "jid": str(self.agent.jid),
            "type": self.agent.fleet_type,
        }

        msg = Message()
        msg.to = str(self.agent.directory_id)
        msg.set_metadata("protocol", REGISTER_PROTOCOL)
        msg.set_metadata("performative", REQUEST_PERFORMATIVE)
        msg.body = json.dumps(content)

        await self.send(msg)

    async def run(self):
        """
            Listens for registration requests from resources and processes them by accepting or rejecting
            them based on the fleet type.
        """

        if (not self.agent.registration
            and self.agent.directory_id is not None):
            await self.send_registration()

        try:
            msg = await self.receive(timeout=5)
            if msg:
                performative = msg.get_metadata("performative")
                if performative == REQUEST_PERFORMATIVE:
                    content = json.loads(msg.body)
                    if content["fleet_type"] == self.agent.fleet_type:
                        self.add_resource(content)
                        self.agent.subscribe_to_presence(content["jid"])
                        await self.accept_registration(msg.sender)
                        logger.debug(
                            "Registration in the {} fleet to {}".format(self.agent.name,content.get("name"))
                        )
                    else:
                        await self.reject_registration(msg.sender)

                if performative == ACCEPT_PERFORMATIVE:

                    if (self.agent.directory_id is not None
                        and self.agent.is_same_jid(msg.sender, self.agent.directory_id)
                    ):
                        self.agent.set_registration(True)

                        logger.info(
                            "Agent[{}]: Registration in Directory "
                            "[{}] accepted.".format(
                                self.agent.name,
                                self.agent.directory_id
                            )
                        )

                if performative == INFORM_PERFORMATIVE:
                    content = json.loads(msg.body)

                    if self.update_resource_presence(content):
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
    The FleetManagerStrategyBehaviour class defines the main strategy for coordinating customer and resource
    agents in the fleet. This behavior needs to implement a `_process` method for custom strategies.
    """

    async def on_start(self):
        """
            Logs that the strategy has started in the Fleet Manager.
        """
        logger.debug("Strategy {} started in manager".format(type(self).__name__))

    # async def send_registration(self):
    #     """
    #     Sends a registration request to the directory service to register the FleetManager.
    #     """
    #     logger.info(
    #         "Manager {} sent proposal to register to directory {}".format(
    #             self.agent.name, self.agent.directory_id
    #         )
    #     )
    #     content = {"jid": str(self.agent.jid), "type": self.agent.fleet_type}
    #     msg = Message()
    #     msg.to = str(self.agent.directory_id)
    #     msg.set_metadata("protocol", REGISTER_PROTOCOL)
    #     msg.set_metadata("performative", REQUEST_PERFORMATIVE)
    #     msg.body = json.dumps(content)
    #     await self.send(msg)

    async def run(self):
        """
            A placeholder method that needs to be implemented by any subclass defining specific fleet management strategies.
        """
        raise NotImplementedError
