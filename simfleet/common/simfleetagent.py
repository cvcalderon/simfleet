import json
import time
import asyncio
import geopy.distance

from loguru import logger
from spade.agent import Agent
from collections import defaultdict
from spade.message import Message
from slixmpp import JID

from simfleet.utils.helpers import distance_in_meters

from simfleet.utils.statistics import StatisticsStore
from spade.presence import PresenceInfo, PresenceType, PresenceShow
from simfleet.communications.protocol import REGISTER_PROTOCOL, INFORM_PERFORMATIVE

class SimfleetAgent(Agent):
    """
        SimfleetAgent is the base class for all agents in the SimFleet framework. It provides essential functionalities for managing
        registration, messaging, and event observation for agents that interact with the transportation fleet and customer system.

        Attributes:
            __observers (dict): A dictionary of observer callbacks to monitor changes in agent properties.
            agent_id (str): The identifier for the agent.
            strategy (function): The current strategy assigned to the agent.
            running_strategy (bool): Indicates if the strategy is currently running.
            port (int): The port used by the agent.
            stopped (bool): Indicates if the agent has stopped.
            ready (bool): Flag to check if the agent is ready to operate.
            is_launched (bool): Flag indicating if the agent has been launched.
            directory_id (str): The JID of the directory service for agent registration.
            status (str): The current status of the agent.
            fleet_type (str): The type of fleet to which the agent belongs.
            registration (bool): Indicates whether the agent is registered or not.
            init_time (float): The start time of the agent's lifecycle.
            end_time (float): The end time of the agent's lifecycle.
            events_store (StatisticsStore): A storage mechanism for agent events and statistics.
    """
    def __init__(self, agentjid, password):
        super().__init__(agentjid, password)
        self.__observers = defaultdict(list)
        self.agent_id = None
        self.agent_name = None
        self.strategy = None
        self.running_strategy = False
        self.port = None
        self.stopped = False #True
        self.ready = False
        self.is_launched = False
        self.directory_id = None
        self.status = None
        self.fleet_type = None

        self.init_time = None   #Change
        self.end_time = None    #Change

        self.registration = False

        self.registration_fleet = None
        self.registration_presence = False
        self.registration_presence_ready = False

        self.events_store = StatisticsStore(agent_name=str(agentjid), class_type=type(self))

    async def setup(self):
        await super().setup()

        self.presence.on_subscribe = self.on_subscribe
        self.presence.on_subscribed = self.on_subscribed
        self.presence.on_available = self.on_available
        self.presence.on_unavailable = self.on_unavailable

    # Presence callbacks
    @staticmethod
    def bare_jid(jid):
        """
        Return the bare JID associated with an XMPP identifier.

        Resource components are removed so Presence subscriptions and
        registration checks compare agents by their stable account JID.

        Args:
            jid: JID instance or string representation of an XMPP identifier.

        Returns:
            str | None: Bare JID, or None when no identifier is provided.
        """

        if jid is None:
            return None

        try:
            return str(JID(str(jid)).bare)
        except Exception:
            return str(jid).split("/")[0]

    def is_same_jid(self, jid_a, jid_b):
        """
        Compare two XMPP identifiers using their bare JIDs.

        Args:
            jid_a: First JID.
            jid_b: Second JID.

        Returns:
            bool: True when both identifiers refer to the same XMPP account.
        """

        return self.bare_jid(jid_a) == self.bare_jid(jid_b)

    def on_subscribe(self, peer_jid):
        """
        Handle an incoming XMPP Presence subscription request.

        The request is approved only when
        ``can_accept_presence_subscription()`` authorizes the peer. Agents
        may optionally subscribe back and mark their registration Presence
        as ready once the expected fleet relationship is established.

        Args:
            peer_jid: JID requesting the Presence subscription.
        """

        logger.debug(
            "Agent[{}]: Agent {} requested presence subscription".format(
                self.name,
                peer_jid
            )
        )

        if self.can_accept_presence_subscription(peer_jid):
            self.approve_presence_subscription(peer_jid)

            logger.debug(
                "Agent[{}]: Presence subscription approved for {}".format(
                    self.name,
                    peer_jid
                )
            )

            if (
                self.registration_presence
                and self.registration_fleet
                and self.is_same_jid(
                peer_jid,
                self.registration_fleet)
            ):
                self.set_registration_presence_ready(True)


            if self.should_subscribe_back(peer_jid):
                self.subscribe_to_presence(peer_jid)

            if (
                self.registration_presence
                and self.registration_fleet
                and self.is_same_jid(peer_jid, self.registration_fleet)
                and hasattr(self, "set_available")
            ):
                self.set_available()

        else:
            logger.debug(
                "Agent[{}]: Presence subscription not approved for {}".format(
                    self.name,
                    peer_jid
                )
            )

    def on_subscribed(self, peer_jid):
        """
        Handle confirmation that a Presence subscription was accepted.

        When the peer is the configured registration fleet, the agent marks
        its Presence registration as ready and publishes availability when
        the concrete agent supports ``set_available()``.

        Args:
            peer_jid: JID that accepted the subscription.
        """

        logger.debug(
            "Agent[{}]: Agent {} accepted presence subscription".format(
                self.name,
                peer_jid
            )
        )

        if (
            self.registration_presence
            and self.registration_fleet
            and self.is_same_jid(peer_jid, self.registration_fleet)
        ):
            self.set_registration_presence_ready(True)

            if hasattr(self, "set_available"):
                self.set_available()

    def on_available(self, peer_jid, presence_info, last_presence):
        """Handle notification that a subscribed peer became available."""
        logger.debug(
            "Agent[{}]: Agent {} is available".format(
                self.name,
                peer_jid
            )
        )

    def on_unavailable(self, peer_jid, presence_info, last_presence):
        """Handle notification that a subscribed peer became unavailable."""
        logger.debug(
            "Agent[{}]: Agent {} is unavailable".format(
                self.name,
                peer_jid
            )
        )

    #Authorization
    def can_accept_presence_subscription(self, peer_jid):
        """
        Decide whether an incoming Presence subscription may be accepted.

        The base policy only accepts the configured registration fleet.
        Specialized agents such as FleetManagerAgent may override this
        policy.

        Args:
            peer_jid: JID requesting the subscription.

        Returns:
            bool: True when the peer is authorized.
        """

        if self.registration_presence and self.registration_fleet:
            return self.is_same_jid(peer_jid, self.registration_fleet)

        return False

    def should_subscribe_back(self, peer_jid):
        """
        Decide whether the agent should establish a reciprocal subscription.

        The base implementation returns False. Fleet managers override this
        behaviour for registered resources.
        """
        return False

    # Presence operations
    def subscribe_to_presence(self, agent_id):
        """
        Subscribe to the Presence of another agent using its bare JID.

        Args:
            agent_id: Target XMPP identifier.
        """
        self.presence.subscribe(self.bare_jid(agent_id))

    def approve_presence_subscription(self, agent_id):
        """
        Approve an incoming Presence subscription for the specified agent.

        Args:
            agent_id: XMPP identifier whose subscription is approved.
        """
        self.presence.approve_subscription(self.bare_jid(agent_id))

    def get_presence_contacts(self):
        """Return the Presence contacts known by the underlying XMPP client."""
        return self.presence.get_contacts()

    def set_agent_presence(
        self,
        status="",
        presence_type=PresenceType.AVAILABLE,
        show=PresenceShow.CHAT,
        priority=0
    ):
        """
        Publish the current XMPP Presence state of the agent.

        When Presence-based registration is enabled, the update is directed
        to the configured fleet manager. The same state may also be mirrored
        through the registration protocol so consumers can fall back to
        message-based Presence information when live XMPP Presence is not
        available.

        Args:
            status: Application payload carried in the Presence status field.
            presence_type: XMPP Presence availability type.
            show: XMPP Presence show value.
            priority: XMPP Presence priority.
        """
        self.presence.current_presence = PresenceInfo(
            presence_type,
            show,
            status,
            priority,
        )

        presence_to = None

        if self.registration_presence and self.registration_fleet:
            presence_to = self.bare_jid(self.registration_fleet)

        logger.debug(
            "Agent[{}]: publishing presence to [{}] with show [{}] and status [{}].".format(
                self.name,
                presence_to,
                show,
                status,
            )
        )

        self.client.send_presence(
            pto=presence_to,
            ptype=None if presence_type == PresenceType.AVAILABLE else presence_type.value,
            pshow=None if show == PresenceShow.NONE else show.value,
            pstatus=status,
            ppriority=str(priority),
        )

        if presence_to:
            try:
                asyncio.create_task(
                    self.send_presence_update(
                        presence_to,
                        status,
                        presence_type,
                        show,
                        priority,
                    )
                )
                self.set_registration_presence_ready(True)
            except RuntimeError:
                logger.debug(
                    "Agent[{}]: could not schedule presence update message.".format(
                        self.name
                    )
                )

    async def send_presence_update(
        self,
        agent_id,
        status,
        presence_type=PresenceType.AVAILABLE,
        show=PresenceShow.CHAT,
        priority=0,
    ):
        """
        Send a message-based mirror of the agent Presence state.

        The mirror uses ``REGISTER_PROTOCOL`` with ``INFORM_PERFORMATIVE``.
        Fleet managers use it as a fallback when live XMPP Presence
        information cannot be obtained.

        Args:
            agent_id: Destination agent JID.
            status: Application Presence payload.
            presence_type: Presence availability type.
            show: Presence show value.
            priority: Presence priority.
        """
        msg = Message()
        msg.to = str(agent_id)
        msg.set_metadata("protocol", REGISTER_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        msg.body = json.dumps(
            {
                "jid": str(self.jid),
                "type": presence_type.value,
                "show": show.value,
                "status": status,
                "priority": priority,
            }
        )

        await self.send(msg)

    def set_registration_presence_ready(self, status):
        self.registration_presence_ready = status

    def get_registration_presence_ready(self):
        return self.registration_presence_ready


    async def stop(self):
        """
            Stops the agent and marks it as stopped. Overrides the default stop behavior in spade.
        """
        self.stopped = True
        await super().stop()


    def is_stopped(self):
        """
            Checks if the agent is stopped.

            Returns:
                bool: True if the agent is stopped, False otherwise.
        """
        return self.stopped



    def is_ready(self):
        """
        Checks whether the agent is ready to start its
        operational strategy.
        """
        if not self.is_launched:
            return True

        if not self.ready:
            return False

        return self.dependencies_ready()

    def dependencies_ready(self):
        """
        Checks external dependencies required during bootstrap.

        Agents without a registration fleet have no generic
        registration dependency.
        """
        if self.registration_fleet is None:
            return True

        if not self.registration:
            return False

        if (
            self.registration_presence
            and not self.registration_presence_ready
        ):
            return False

        return True


    async def sleep(self, seconds):
        """
        Pauses the agent asynchronously for the specified duration.

        Args:
            seconds (float): Number of seconds to suspend execution.
        """
        await asyncio.sleep(seconds)


    def set(self, key, value):
        """
            Sets a value to a specific key in the agent’s properties. If an observer is registered for this key,
            the callback is triggered upon value change.

            Args:
                key (str): The property name.
                value (any): The value to be assigned.
        """
        old = self.get(key)
        super().set(key, value)
        if key in self.__observers:
            for callback in self.__observers[key]:
                callback(old, value)


    def set_registration(self, status, content=None):
        """
        Sets the registration status of the agent.

        Args:
            status (bool): True if the agent is registered, False otherwise.
            content (dict, optional): Additional information about the agent, such as its icon and fleet type.
        """
        if content is not None:
            self.icon = content["icon"] if self.icon is None else self.icon
            self.fleet_type = content["fleet_type"]
        self.registration = status

        if status:
            self.start_registration_presence()


    def configure_registration(self, fleet, presence=False):
        """
        Configure the fleet registration relationship for the agent.

        Args:
            fleet: JID of the FleetManager responsible for registration.
            presence: Enable Presence subscription after registration.
        """
        self.registration_fleet = fleet
        self.registration_presence = presence

        self.registration = False
        self.registration_presence_ready = False

    def get_registration_fleet(self):
        return self.registration_fleet

    def get_registration_presence(self):
        return self.registration_presence

    def start_registration_presence(self):
        """
        Start the Presence relationship with the configured FleetManager.

        No subscription is created when Presence registration is disabled or
        when no registration fleet has been configured.
        """

        if self.registration_presence and self.registration_fleet:
            self.subscribe_to_presence(self.registration_fleet)

    def watch_value(self, key, callback):
        """
        Registers a callback function that is triggered when a specified key's value changes.

        Args:
            key (str): The property name to observe.
            callback (function): The callback function to trigger when the property changes. Receives the old and new value.
        """
        self.__observers[key].append(callback)


    def set_fleet_type(self, fleet_type):
        """
        Sets the type of fleet to which the agent belongs.

        Args:
            fleet_type (str): The type of fleet (e.g., "bus", "taxi").
        """
        self.fleet_type = fleet_type

    #New version send for spade 4
    async def send(self, msg: Message) -> None:
        """
            Sends a message to another agent, ensuring that the sender's JID is correctly included in the message.

            Args:
                msg (spade.message.Message): The message to be sent.
        """
        if not msg.sender:
            msg.sender = str(self.jid)
            logger.debug(f"Adding agent's jid as sender to message: {msg}")
        await self.container.send(msg, self)
        msg.sent = True
        self.traces.append(msg, category=str(self))

    def set_id(self, agent_id):
        """
        Sets the agent's identifier.

        Args:
            agent_id (str): The new identifier for the agent.
        """
        self.agent_id = agent_id


    def get_id(self):
        """
        Retrieves the agent's identifier.

        Returns:
            str: The identifier of the agent.
        """
        return self.agent_id

    def set_name(self, name):
        """
            Sets the name of the bus stop.
        """
        self.agent_name = name


    def set_directory(self, directory_id):
        """
        Sets the JID of the directory agent responsible for managing the directory of services.

        Args:
            directory_id (str): The JID of the directory agent.
        """
        self.directory_id = directory_id

    def to_json(self):
        """
        Serialises the basic information of a Simfleet agent to a JSON format.
        This function is the basis for JSON representations of specific subclasses.
        """
        return {
            "id": self.agent_id,
            "status": self.status,
        }

    def total_time(self):
        """
        Calculates the total simulation time from the agent's activation until it reaches its destination.

        Returns:
            float: The total time in seconds, or None if the times are not available.
        """
        if self.init_time and self.end_time:
            return self.end_time - self.init_time
        else:
            return None

    def near_agent(self, coords_1, coords_2):
        """
            Determines if two agents are near each other, within 100 meters.

            Args:
                coords_1 (list): The coordinates of the first agent.
                coords_2 (list): The coordinates of the second agent.

            Returns:
                bool: True if the agents are near each other, False otherwise.
        """
        if geopy.distance.geodesic(coords_1, coords_2).km > 0.1:
            return False
        return True


    def nearst_agent(self, agent_list, position):
        """
            Finds the closest agent from a list of agents to the specified position.

            Args:
                agent_list (dict): A dictionary of agents with their positions.
                position (list): The position to compare against.

            Returns:
                tuple: The closest agent's JID and position.
        """

        agent_positions = []
        for key in agent_list.keys():
            dic = agent_list.get(key)
            agent_positions.append((dic["jid"], dic["position"]))

        closest_agent = min(
            agent_positions,
            key=lambda x: distance_in_meters(x[1], position),
        )
        logger.debug("Closest agent {}".format(closest_agent))
        agent = closest_agent[0]
        result = (
            agent,
            agent_list[agent]["position"],
        )
        logger.info(
            "Agent[{}]: The agent selected agent ({}).".format(self.name, agent)
        )
        return result
