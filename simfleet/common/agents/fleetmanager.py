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
    Base agent responsible for managing resources belonging to one fleet.

    Fleet resources register through REGISTER_PROTOCOL and are stored in
    ``fleet_resources``. A resource may represent a transport, vehicle,
    station, stop, or another fleet-specific infrastructure element.

    The FleetManager also maintains resource availability through two
    Presence sources:

    - live XMPP Presence, used as the primary runtime source;
    - a message-based Presence mirror stored with the registered resource,
      used as fallback when live Presence is unavailable.

    FleetManagerAgent may itself register with a DirectoryAgent. Concrete
    fleet managers extend this model with modality-specific resource and
    customer coordination.
    """

    def __init__(self, agentjid, password):
        """
        Initialize fleet resource storage and common manager state.

        Args:
            agentjid (str): XMPP JID used by the FleetManager.
            password (str): XMPP authentication password.
        """

        super().__init__(agentjid, password)

        self.resources_in_fleet = 0
        self.fleet_icon = None
        self.clear_agents()


    def clear_agents(self):
        """
        Clear all resources currently registered with this FleetManager.

        This resets the ``fleet_resources`` mapping but does not modify the
        FleetManager identity, strategy, fleet type, or Directory registration.
        """
        self.set("fleet_resources", {})

    async def setup(self):
        """
        Initialize fleet-resource registration handling.

        ResourceRegistrationForFleetBehaviour listens to REGISTER_PROTOCOL and
        manages both resource registration and this FleetManager's registration
        with an optional DirectoryAgent.

        Local setup is marked ready once that behaviour has been installed.
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
        Return resources currently registered with the FleetManager.

        Returns:
            dict: Registered resources indexed by their resource name.
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
        Configure the icon advertised to registered fleet resources.

        Args:
            icon (str): Fleet icon identifier.
        """
        self.fleet_icon = icon

    def run_strategy(self):
        """
        Start the configured fleet-management strategy once.

        The strategy receives REQUEST_PROTOCOL messages used for fleet-specific
        customer and resource coordination. ``running_strategy`` prevents
        duplicate strategy instances.
        """
        if not self.running_strategy:
            template = Template()
            template.set_metadata("protocol", REQUEST_PROTOCOL)
            self.add_behaviour(self.strategy(), template)
            self.running_strategy = True

    def dependencies_ready(self):
        """
        Return whether external FleetManager dependencies are satisfied.

        In addition to the generic SimfleetAgent dependencies, the FleetManager
        must have completed its configured Directory registration.

        Returns:
            bool: True when all required dependencies are ready.
        """
        if not super().dependencies_ready():
            return False

        return bool(
            self.registration
        )


class ResourceRegistrationForFleetBehaviour(CyclicBehaviour):
    """
    Manage REGISTER_PROTOCOL relationships owned by a FleetManager.

    The behaviour handles three responsibilities:

    - register this FleetManager with its optional DirectoryAgent;
    - accept or reject resources requesting membership in the fleet;
    - receive message-based Presence mirrors from registered resources.

    A resource is accepted only when its declared fleet type matches the
    FleetManager fleet type. Accepted resources are stored in
    ``fleet_resources`` and subscribed through XMPP Presence.
    """
    async def on_start(self):
        """Log the start of fleet-resource registration handling."""
        logger.debug("Strategy {} started in manager".format(type(self).__name__))

    def add_resource(self, agent):
        """
        Add or update a resource in the FleetManager registry.

        ``resources_in_fleet`` is incremented only for a previously unknown
        resource name.

        Args:
            agent (dict): Resource registration payload.
        """
        resources = self.get("fleet_resources")

        if agent["name"] not in resources:
            self.agent.resources_in_fleet += 1

        resources[agent["name"]] = agent

    def remove_resource(self, key):
        """
        Remove a registered fleet resource by its registry key.

        Args:
            key (str): Resource key stored in ``fleet_resources``.
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
        Update the message-based Presence mirror of a registered resource.

        Resource matching uses bare-JID equivalence rather than exact resource
        components.

        Args:
            content (dict): Presence mirror containing the resource JID.

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
        Accept a resource into the fleet.

        The response advertises the FleetManager icon and fleet type.

        Args:
            agent_id: Resource JID receiving the acceptance.
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
        Refuse a resource registration request.

        Args:
            agent_id: Resource JID receiving the refusal.
        """
        reply = Message()
        reply.to = str(agent_id)
        reply.set_metadata("protocol", REGISTER_PROTOCOL)
        reply.set_metadata("performative", REFUSE_PERFORMATIVE)
        reply.body = ""
        await self.send(reply)

    async def send_registration(self):
        """
        Request registration of this FleetManager with its DirectoryAgent.

        No message is sent when no DirectoryAgent has been configured.
        """

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
        Execute one FleetManager registration cycle.

        Before processing incoming messages, an unregistered FleetManager may
        retry registration with its configured DirectoryAgent.

        REGISTER_PROTOCOL messages are then interpreted as follows:

        ``REQUEST_PERFORMATIVE``
            A fleet resource requests registration. Matching fleet types are
            stored, subscribed through Presence, and accepted.

        ``ACCEPT_PERFORMATIVE``
            The DirectoryAgent confirms registration of this FleetManager.

        ``INFORM_PERFORMATIVE``
            A registered resource supplies a message-based Presence mirror.

        Unexpected errors are logged without terminating the cyclic behaviour.
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


    async def run(self):
        """
            A placeholder method that needs to be implemented by any subclass defining specific fleet management strategies.
        """
        raise NotImplementedError
