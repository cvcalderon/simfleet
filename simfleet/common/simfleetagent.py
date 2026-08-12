import time
import asyncio
import geopy.distance

from loguru import logger
from spade.agent import Agent
from collections import defaultdict
from spade.message import Message

from simfleet.utils.helpers import distance_in_meters

from simfleet.utils.statistics import StatisticsStore
from spade.presence import PresenceType, PresenceShow

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

        # New - Implementation v1
        self.registration = None

        self.registration_fleet = None
        self.registration_presence = False

        # --------------------------

        self.events_store = StatisticsStore(agent_name=str(agentjid), class_type=type(self))

    async def setup(self):
        await super().setup()

        # New - Implementation v1
        self.presence.on_subscribe = self.on_subscribe
        self.presence.on_subscribed = self.on_subscribed
        self.presence.on_available = self.on_available
        self.presence.on_unavailable = self.on_unavailable
        # --------------------------

    # New - Implementation v1
    # Presence callbacks
    def on_subscribe(self, peer_jid):
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

            if self.should_subscribe_back(peer_jid):
                self.subscribe_to_presence(peer_jid)

        else:
            logger.debug(
                "Agent[{}]: Presence subscription not approved for {}".format(
                    self.name,
                    peer_jid
                )
            )

    def on_subscribed(self, peer_jid):
        logger.debug(
            "Agent[{}]: Agent {} accepted presence subscription".format(
                self.name,
                peer_jid
            )
        )

    def on_available(self, peer_jid, presence_info, last_presence):
        logger.debug(
            "Agent[{}]: Agent {} is available".format(
                self.name,
                peer_jid
            )
        )

    def on_unavailable(self, peer_jid, presence_info, last_presence):
        logger.debug(
            "Agent[{}]: Agent {} is unavailable".format(
                self.name,
                peer_jid
            )
        )

    #Authorization
    def can_accept_presence_subscription(self, peer_jid):
        if self.registration_presence and self.registration_fleet:
            return str(peer_jid) == str(self.registration_fleet)

        return False

    def should_subscribe_back(self, peer_jid):
        return False

    # Presence operations
    def subscribe_to_presence(self, agent_id):
        self.presence.subscribe(agent_id)

    def approve_presence_subscription(self, agent_id):
        self.presence.approve_subscription(agent_id)

    def get_presence_contacts(self):
        return self.presence.get_contacts()

    def set_agent_presence(
        self,
        status="",
        presence_type=PresenceType.AVAILABLE,
        show=PresenceShow.CHAT,
        priority=0
    ):
        self.presence.set_presence(
            presence_type=presence_type,
            show=show,
            status=status,
            priority=priority,
        )

    # --------------------------

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
            Checks if the agent is ready for operation.

            Returns:
                bool: True if the agent is ready, False otherwise.
        """
        return not self.is_launched or (self.is_launched and self.ready)


    async def sleep(self, seconds):
        """
            Pauses the agent’s operation for a specified duration.

            Args:
                seconds (int): The duration in seconds for which the agent should pause.
        """
        await asyncio.sleep(seconds)
        #time.sleep(seconds)


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


    # New - Implementation v1
    #Registration
    def configure_registration(self, fleet, presence=False):
        """
        Configures the agent registration information.

        Args:
            fleet (str): JID of the agent responsible for the registration.
            presence (bool): Indicates whether presence should be enabled
                after registration.
        """
        self.registration_fleet = fleet
        self.registration_presence = presence

    def get_registration_fleet(self):
        return self.registration_fleet

    def get_registration_presence(self):
        return self.registration_presence

    def start_registration_presence(self):
        if self.registration_presence and self.registration_fleet:
            self.subscribe_to_presence(self.registration_fleet)

    # --------------------------

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
