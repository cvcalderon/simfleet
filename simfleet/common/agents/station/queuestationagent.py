import json
import time

from loguru import logger
from collections import deque
from spade.message import Message
from spade.template import Template
from spade.behaviour import CyclicBehaviour, OneShotBehaviour

from simfleet.common.geolocatedagent import GeoLocatedAgent

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    COORDINATION_PROTOCOL,
)


class QueueStationAgent(GeoLocatedAgent):
    """
    Base station model for services that maintain waiting queues.

    Each configured service owns an independent FIFO queue stored in
    ``waiting_lists``. Queue entries contain the requesting agent JID together
    with service-specific arguments.

    Before a requester is admitted to a queue, QueueBehaviour asks the
    Simulator for its current position through COORDINATION_PROTOCOL and
    verifies that the requester is physically close enough to the station.

    This class manages queue admission and cancellation only. Concrete
    subclasses such as ServiceStationAgent define how queued requests are
    actually served.
    """

    def __init__(self, agentjid, password):
        """
        Initialize station queue state and its queue-management behaviour.

        Args:
            agentjid (str): XMPP JID used by the station.
            password (str): XMPP authentication password.
        """
        GeoLocatedAgent.__init__(self, agentjid, password)

        # Initialize queue management behaviour
        self.queuebehaviour = self.QueueBehaviour()

        self.waiting_lists = {}  # Waiting lists for each service type

        # JID of the simulator agent
        self.simulatorjid = None

    def set_simulatorjid(self, jid):
        """
        Store the Simulator JID used for proximity checks.

        Args:
            jid: Simulator XMPP identifier.
        """
        self.simulatorjid = str(jid)

    def get_simulatorjid(self):
        """
        Return the Simulator JID used for proximity checks.

        Returns:
            str | None: Configured Simulator JID.
        """
        return self.simulatorjid

    async def setup(self):
        """
        Install queue admission and cancellation handling.

        QueueBehaviour listens to REQUEST_PROTOCOL messages carrying either
        REQUEST_PERFORMATIVE or CANCEL_PERFORMATIVE.
        """

        await super().setup()

        logger.debug("Agent[{}]: Queue station running".format(self.name))

        template1 = Template()
        template1.set_metadata("protocol", REQUEST_PROTOCOL)
        template1.set_metadata("performative", REQUEST_PERFORMATIVE)

        template2 = Template()
        template2.set_metadata("protocol", REQUEST_PROTOCOL)
        template2.set_metadata("performative", CANCEL_PERFORMATIVE)

        self.add_behaviour(self.queuebehaviour, template1 | template2)

    def add_queue(self, name):
        """
        Create an empty FIFO waiting queue for a service.

        Args:
            name (str): Service identifier associated with the queue.
        """

        if name not in self.waiting_lists:

            self.waiting_lists[name] = deque()  # Create a deque for the line

            logger.debug(
                "Agent[{}]: The queue ({}) has been inserted.".format(self.name, name)
            )
        else:
            logger.warning("Agent[{}]: The queue ({}) exists.".format(self.name, name))

    def remove_queue(self, name):
        """
        Remove the waiting queue associated with a service.

        Args:
            name (str): Service identifier whose queue must be removed.
        """
        if name in self.waiting_lists:
            del self.waiting_lists[name]
            logger.warning(
                "Agent[{}]: The queue ({}) has been removed. ".format(self.name, name)
            )


    def to_json(self):
        """
        Serialize the common geolocated station state.

        Returns:
            dict: Serializable station representation.
        """
        data = super().to_json()
        return data

    # Queue management for agents requesting services
    class QueueBehaviour(CyclicBehaviour):
        """
        Manage queue admission, cancellation, and FIFO queue operations.

        REQUEST messages are admitted only after CheckNearBehaviour confirms
        proximity to the station. CANCEL messages remove the requesting agent
        from the corresponding service queue.
        """

        def __init__(self):
            """Initialize the cyclic queue-management behaviour."""
            super().__init__()

        def total_queue_size(self, service_name):
            """
            Return the number of requests waiting for a service.

            Args:
                service_name (str): Service queue identifier.

            Returns:
                int: Number of queued requests.
            """
            return len(self.agent.waiting_lists[service_name])

        def queue_agent_to_waiting_list(self, service_name, id_agent, **kwargs):
            """
            Append one request to a service FIFO queue.

            Queue entries are stored as ``(agent_jid, service_arguments)``.

            Args:
                service_name (str): Target service.
                id_agent (str): Requesting agent JID.
                **kwargs: Service-specific request arguments.
            """
            self.agent.waiting_lists[service_name].append((id_agent, kwargs))

        def dequeue_first_agent_to_waiting_list(self, service_name):
            """
            Remove and return the oldest request from a service queue.

            Args:
                service_name (str): Service queue identifier.

            Returns:
                tuple | None: ``(agent_jid, arguments)`` for the first request,
                or None when the queue is empty.
            """
            if len(self.agent.waiting_lists[service_name]) == 0:
                return None
            return self.agent.waiting_lists[service_name].popleft()

        def dequeue_agent_to_waiting_list(self, service_name, id_agent):
            """
            Remove a specific requesting agent from a service queue.

            Args:
                service_name (str): Service queue identifier.
                id_agent (str): Agent JID to remove.
            """
            if service_name in self.agent.waiting_lists:
                for agent in self.agent.waiting_lists[service_name]:
                    if agent[0] == id_agent:
                        self.agent.waiting_lists[service_name].remove(agent)
                        break

        def find_queue_position(self, service_name, agent_id):
            """
            Return the queue index reported for the supplied agent identifier.

            Args:
                service_name (str): Service queue identifier.
                agent_id (str): Agent identifier to locate.

            Returns:
                int | None: Queue index when found.

            Note:
                Queue entries currently store ``(agent_id, arguments)`` tuples.
                The lookup implementation is retained unchanged pending a dedicated
                consistency review.
            """
            try:
                position = self.agent.waiting_lists[service_name].index(agent_id)
                return position
            except ValueError:
                return None

        def get_queue(self, service_name):
            """
            Return the FIFO queue associated with a service.

            Args:
                service_name (str): Service identifier.

            Returns:
                deque | None: Service queue when configured.
            """
            if service_name in self.agent.waiting_lists:
                return self.agent.waiting_lists[service_name]

        async def accept_request_agent(self, agent_id, content=None):
            """
            Inform a requester that admission to the station queue was accepted.

            Args:
                agent_id: Requesting agent JID.
                content (dict | None): Optional acceptance payload.
            """
            if content is None:
                content = {}
            reply = Message()
            reply.to = str(agent_id)
            reply.set_metadata("protocol", REQUEST_PROTOCOL)
            reply.set_metadata("performative", ACCEPT_PERFORMATIVE)
            reply.body = json.dumps(content)
            await self.send(reply)
            logger.debug(
                "Agent[{}]: The agent accepted entry proposal".format(self.agent.name)
            )

        async def refuse_request_agent(self, agent_id):
            """
            Inform a requester that queue admission was refused.

            Args:
                agent_id: Requesting agent JID.
            """
            reply = Message()
            reply.to = str(agent_id)
            reply.set_metadata("protocol", REQUEST_PROTOCOL)
            reply.set_metadata("performative", REFUSE_PERFORMATIVE)
            content = {}
            reply.body = json.dumps(content)

            await self.send(reply)
            logger.debug(
                "Agent[{}]: The agent refused proposal from agent [{}]".format(
                    self.agent.name, agent_id
                )
            )

        async def on_start(self):
            """Log the start of queue management."""
            logger.debug(
                "Agent[{}]: Strategy ({}) started.".format(
                    self.agent.name, type(self).__name__
                )
            )

        async def run(self):
            """
            Process one queue-management message.

            REQUEST performs an asynchronous proximity check through the Simulator.
            A nearby requester is appended to the appropriate service queue and
            receives ACCEPT; unknown services or non-near requesters receive REFUSE.

            CANCEL removes the requester from the specified queue.
            """
            msg = await self.receive(timeout=5)

            if msg:
                performative = msg.get_metadata("performative")
                protocol = msg.get_metadata("protocol")
                agent_id = msg.sender
                content = json.loads(msg.body)

                if protocol == REQUEST_PROTOCOL and performative == CANCEL_PERFORMATIVE:

                    if "service_name" in content:
                        service_name = content["service_name"]

                    logger.warning(
                        "Agent[{}]: The agent received a REFUSE from agent [{}].".format(
                            self.agent.name, agent_id
                        )
                    )
                    self.dequeue_agent_to_waiting_list(service_name, str(agent_id))

                    logger.debug(
                        "Agent[{}]: The agent [{}] has been dequeue.".format(
                            self.agent.name, agent_id
                        )
                    )
                elif (
                    protocol == REQUEST_PROTOCOL
                    and performative == REQUEST_PERFORMATIVE
                ):

                    if "service_name" in content:
                        service_name = content["service_name"]

                    if "line" in content:
                        service_name = content["line"]

                    if "object_type" in content:
                        object_type = content["object_type"]

                    if "args" in content:
                        arguments = content["args"]
                    else:
                        arguments = {}

                    # Check proximity before enqueuing
                    template3 = Template()
                    template3.set_metadata("protocol", COORDINATION_PROTOCOL)
                    template3.set_metadata("performative", INFORM_PERFORMATIVE)

                    instance = CheckNearBehaviour(
                        self.agent.get_simulatorjid(),
                        str(agent_id),
                        service_name,
                        object_type,
                        arguments,
                    )
                    self.agent.add_behaviour(instance, template3)

                    await instance.join()  # Wait for the behaviour to complete

                    service_name = instance.service_name
                    agent_position = instance.agent_position
                    user_agent_id = instance.user_agent_id
                    arguments = instance.arguments

                    if (
                        service_name not in self.agent.waiting_lists
                        or not self.agent.near_agent(
                            coords_1=self.agent.get_position(), coords_2=agent_position
                        )
                    ):

                        await self.refuse_request_agent(user_agent_id)
                        logger.warning(
                            "Agent[{}]: The agent has REFUSED request from agent [{}] for service ({})".format(
                                self.agent.name, user_agent_id, service_name
                            )
                        )
                    else:

                        # Queue
                        self.queue_agent_to_waiting_list(
                            service_name, str(user_agent_id), **arguments
                        )

                        content = {"station_id": str(self.agent.jid)}
                        await self.accept_request_agent(user_agent_id, content)

                        logger.info(
                            "Agent[{}]: The agent [{}] has been queue".format(
                                self.agent.name,
                                user_agent_id,
                            )
                        )
                else:
                    logger.warning(
                        "Agent[{}]: The agent has not received agent position of [{}] from the Simulator".format(
                            self.agent.name,
                            agent_id,
                        )
                    )


class CheckNearBehaviour(OneShotBehaviour):
    """
    Resolve a requester's position through the Simulator before queue entry.

    QueueStationAgent does not assume that the position contained in a service
    request is authoritative. Instead, this one-shot behaviour queries the
    Simulator through COORDINATION_PROTOCOL and stores the resolved position
    for QueueBehaviour to validate with ``near_agent()``.
    """
    def __init__(
        self, simulatorjid, user_agent_id, service_name, object_type, arguments
    ):
        """
        Initialize one proximity-check request.

        Args:
            simulatorjid: Simulator JID.
            user_agent_id: Agent whose position must be resolved.
            service_name: Requested station service.
            object_type: Agent type supplied to the Simulator lookup.
            arguments: Original service-request arguments.
        """
        super().__init__()

        self.agent_simulator_id = simulatorjid
        self.user_agent_id = user_agent_id
        self.service_name = service_name
        self.object_type = object_type
        self.arguments = arguments
        self.agent_position = None

    async def request_agent_position_near(self, agent_id, content):
        """
        Request an agent position from the Simulator.

        Args:
            agent_id: Simulator JID.
            content (dict): Position-query payload.
        """
        reply = Message()
        reply.to = str(agent_id)
        reply.set_metadata("protocol", COORDINATION_PROTOCOL)
        reply.set_metadata("performative", REQUEST_PERFORMATIVE)
        reply.body = json.dumps(content)
        await self.send(reply)

    async def run(self):
        """
        Request and store the requester position used by queue admission.

        INFORM_PERFORMATIVE responses received through COORDINATION_PROTOCOL
        provide the position consumed later by QueueBehaviour.
        """

        content = {"user_agent_id": self.user_agent_id, "object_type": self.object_type}
        await self.request_agent_position_near(
            agent_id=self.agent_simulator_id, content=content
        )

        msg = await self.receive(timeout=30)

        if msg:
            performative = msg.get_metadata("performative")
            protocol = msg.get_metadata("protocol")
            agent_id = msg.sender
            content = json.loads(msg.body)

            if (
                protocol == COORDINATION_PROTOCOL
                and performative == INFORM_PERFORMATIVE
            ):

                if "agent_position" in content:
                    agent_position = content["agent_position"]

                if "user_agent_id" in content:
                    user_agent_id = content["user_agent_id"]

                logger.debug(
                    "Agent[{}]: The agent has received msg from agent [{}] for near check".format(
                        self.agent.name, agent_id
                    )
                )

                self.agent_position = agent_position

            else:
                logger.warning(
                    "Agent[{}]: The agent has not received agent position of [{}] from the Simulator".format(
                        self.agent.name,
                        agent_id,
                    )
                )
