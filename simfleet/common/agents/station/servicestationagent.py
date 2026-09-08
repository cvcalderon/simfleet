import json

from loguru import logger
from spade.behaviour import CyclicBehaviour
from spade.message import Message
from spade.template import Template

from simfleet.common.agents.station.queuestationagent import QueueStationAgent

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    INFORM_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
)


class ServiceStationAgent(QueueStationAgent):
    """
    Base station model that serves requests admitted through QueueStationAgent.

    Services are stored in ``services_list`` and support two execution models:

    ``mode="behaviour"``
        The station owns a fixed number of concurrent service slots. Each
        dequeued request launches a configured OneShotBehaviour.

    ``mode="agent"``
        The station owns a finite inventory of transport-agent JIDs. A
        dequeued customer is assigned one available transport from that pool.

    Both service modes reuse the FIFO queues managed by QueueStationAgent.
    """

    def __init__(self, agentjid, password):
        """
        Initialize service definitions and the service-dispatch behaviour.

        Args:
            agentjid (str): XMPP JID used by the station.
            password (str): XMPP authentication password.
        """
        QueueStationAgent.__init__(self, agentjid, password)

        self.servicebehaviour = self.ServiceRunBehaviour()

        self.services_list = {}  # Stores service types and their attributes

    async def setup(self):
        """
        Initialize queue handling and start service dispatch.
        """
        await super().setup()
        logger.debug("Agent[{}]: Service station running".format(self.name))
        self.add_behaviour(self.servicebehaviour)


    def add_service(self, service_name, mode, slots, one_shot_behaviour, **arguments):
        """
        Register a slot-based service executed by a OneShotBehaviour.

        Args:
            service_name (str): Service identifier.
            mode (str): Service execution mode, normally ``"behaviour"``.
            slots (int): Maximum number of concurrent executions.
            one_shot_behaviour: Behaviour class instantiated for each request.
            **arguments: Station-side parameters passed to the service behaviour.
        """
        if service_name not in self.services_list:
            self.services_list[service_name] = {
                'mode': mode,
                'slots': slots,
                'slots_in_use': 0,
                'one_shot_behaviour': one_shot_behaviour,
                'args': arguments,
            }

            self.add_queue(service_name)

            logger.debug(
                "Agent[{}]: The service ({}) has been inserted. ".format(
                    self.name, service_name
                )
            )
        else:
            logger.warning(
                "Agent[{}]: The service ({}) doesn't exists.".format(
                    self.name, service_name
                )
            )

    # Agent-backed services, such as StationSharing inventories.
    def add_service_agent(self, service_name, mode, max_agents, **arguments):
        """
        Register a service backed by a finite inventory of agents.

        Agent-backed services maintain a pool of transport JIDs instead of
        executing a one-shot service behaviour. StationSharing uses this
        model to represent vehicles available at a station.

        Args:
            service_name (str): Identifier of the service.
            mode (str): Service mode. Expected to be ``"agent"``.
            max_agents (int): Maximum number of agents stored by the service.
            **arguments: Additional service-specific configuration.
        """
        if service_name not in self.services_list:
            self.services_list[service_name] = {
                "mode": mode,
                "max_agents": max_agents,
                "agents": [],
                "args": arguments,
            }

            # También creamos su cola correspondiente
            self.add_queue(service_name)

            logger.debug(
                f"Agent[{self.name}]: Agent-based service '{service_name}' registered."
            )
        else:
            logger.warning(
                f"Agent[{self.name}]: Service '{service_name}' already exists."
            )

    def remove_service(self, service_name):
        """
        Remove a service definition together with its waiting queue.

        Args:
            service_name (str): Service identifier.
        """
        if service_name in self.services_list:
            del self.services_list[service_name]
            self.remove_queue(service_name)
            logger.debug(
                "Agent[{}]: The service ({}) has been removed. ".format(
                    self.name, service_name
                )
            )

    def show_services(self):
        """
        Return all service identifiers exposed by the station.

        Returns:
            tuple: Configured service names.
        """
        return tuple(self.services_list.keys())

    def show_service_arguments(self, service_name):
        """
        Return station-side arguments configured for a service.

        Args:
            service_name (str): Service identifier.

        Returns:
            dict: Service configuration arguments.
        """
        return self.services_list[service_name]["args"]

    def service_available(self, service_name):
        """
        Return whether a slot-based service can start another execution.

        Args:
            service_name (str): Service identifier.

        Returns:
            bool: True when ``slots_in_use`` is below the configured slot count.
        """
        if self.services_list[service_name]["slots_in_use"] >= self.services_list[service_name]["slots"]:
            return False
        return True


    def register_agent(self, service_name, agent_jid):
        """
        Add an agent to an agent-backed service inventory.

        The agent is registered only when the service exists, uses
        ``mode="agent"``, has available capacity, and does not already
        contain the same JID.

        Args:
            service_name (str): Service receiving the agent.
            agent_jid (str): JID of the transport being registered.
        """

        service = self.services_list.get(service_name, {})
        if service.get("mode") == "agent":
            if not self.is_station_full(service_name):
                if agent_jid not in service["agents"]:
                    service["agents"].append(agent_jid)
                    logger.debug(f"Agent[{self.name}]: Registered agent [{agent_jid}] to service '{service_name}'.")
                else:
                    logger.info(f"Agent[{self.name}]: Agent [{agent_jid}] already registered.")
            else:
                logger.warning(
                    f"Agent[{self.name}]: Service '{service_name}' is full! Cannot register more agents.")
        else:
            logger.warning(
                f"Agent[{self.name}]: Cannot register agent — service '{service_name}' not found or invalid mode.")

    def is_station_full(self, service_name):
        """
        Return whether an agent-backed service reached its configured capacity.
        """
        service = self.services_list.get(service_name, {})
        if service.get("mode") == "agent":
            return len(service["agents"]) >= service.get("max_agents")
        return False

    def has_available_agent(self, service_name):
        """
        Return whether an agent-backed service has at least one transport available.
        """
        return (
            service_name in self.services_list and
            self.services_list[service_name]["mode"] == "agent" and
            len(self.services_list[service_name]["agents"]) > 0
        )

    def available_agents(self, service_name):
        """
        Return the number of agents currently available for a service.
        """
        return len(self.services_list[service_name]["agents"])

    def assign_agent(self, service_name):
        """
        Remove and return the next available agent from a service inventory.

        Returns:
            str | None: Assigned agent JID, or None when none is available.
        """
        if self.has_available_agent(service_name):
            return self.services_list[service_name]["agents"].pop(0)
        return None

    def on_agent_assigned(self, service_name, agent_jid):
        """
        Hook invoked after an agent-backed service removes an assigned resource.

        Subclasses may override this hook to publish inventory changes or perform
        other station-specific actions.

        Args:
            service_name (str): Service from which the resource was assigned.
            agent_jid: Assigned resource JID.
        """
        pass

    def to_json(self):
        """
        Serialize the common station representation.

        Specialized stations may extend the returned mapping with service-specific
        inventory information.

        Returns:
            dict: Serializable station state.
        """
        data = super().to_json()
        return data


    class ServiceRunBehaviour(CyclicBehaviour):
        """
        Dispatch queued requests according to each service execution mode.

        Slot-based services start OneShotBehaviour instances while capacity is
        available. Agent-backed services remove one transport from the station
        inventory and inform the waiting customer of the assigned transport JID.
        """

        def __init__(self):
            """Initialize the cyclic service dispatcher."""
            super().__init__()

        def increase_slots_used(self, service_type):
            """
                Increments the number of slots currently in use for a given service type.
            """
            if service_type in self.agent.services_list:
                self.agent.services_list[service_type]["slots_in_use"] += 1

        def decrease_slots_used(self, service_type):
            """
                Decrements the number of slots currently in use for a given service type.
            """
            if service_type in self.agent.services_list:
                self.agent.services_list[service_type]["slots_in_use"] -= 1

        def get_slot_number(self, service_type):
            """
                Returns the total number of slots for the given service type.
            """
            return self.agent.services_list[service_type]["slots"]

        def get_slot_number_used(self, service_type):
            """
                Returns the number of slots currently in use for the given service type.
            """
            return self.agent.services_list[service_type]["slots_in_use"]


        async def refuse_service(self, agent_id, content=None):
            """
            Inform a requester that the station cannot provide the requested service.

            Args:
                agent_id: Requesting agent JID.
                content (dict | None): Optional refusal payload.
            """
            reply = Message()
            reply.to = str(agent_id)
            reply.set_metadata("protocol", REQUEST_PROTOCOL)
            reply.set_metadata("performative", REFUSE_PERFORMATIVE)
            reply.body = json.dumps(content)
            await self.send(reply)
            logger.debug(
                "Agent[{}]: The agent refuse to agent [{}]".format(
                    self.agent.name,
                    agent_id
                )
            )

        async def inform_service(self, agent_id, content=None):
            """
            Inform an agent that a slot-based service is starting or has progressed.

            Args:
                agent_id: Target agent JID.
                content (dict | None): Service-status payload.
            """
            if content is None:
                content = {}
            reply = Message()
            reply.to = str(agent_id)
            reply.set_metadata("protocol", REQUEST_PROTOCOL)
            reply.set_metadata("performative", INFORM_PERFORMATIVE)
            reply.body = json.dumps(content)
            await self.send(reply)
            logger.debug(
                "Agent[{}]: The agent inform to agent [{}]".format(
                    self.agent.name,
                    agent_id
                )
            )

        async def inform_transport_assignment(self, customer_jid, service_name, transport_jid):
            """
            Inform a customer which transport was assigned by an agent-backed service.

            Args:
                customer_jid: Customer JID.
                service_name (str): Station service identifier.
                transport_jid: Assigned transport JID.
            """
            msg = Message()
            msg.to = str(customer_jid)
            msg.set_metadata("protocol", REQUEST_PROTOCOL)
            msg.set_metadata("performative", INFORM_PERFORMATIVE)
            content = {
                "service_name": service_name,
                "transport_id": transport_jid
            }
            msg.body = json.dumps(content)
            await self.send(msg)
            logger.info(
                f"Agent[{self.agent.name}]: Notified customer [{customer_jid}] about transport [{transport_jid}] for service [{service_name}]"
            )

        async def on_start(self):
            """Log the start of service dispatch."""
            logger.debug("Agent[{}]: Standard behaviour ({}) started".format(self.agent.name, type(self).__name__))


        async def run(self):
            """
            Dispatch waiting requests for all configured station services.

            For ``mode="behaviour"``, one queued requester is dequeued when a service
            slot is free, the slot counter is incremented, and the configured
            OneShotBehaviour is started.

            For ``mode="agent"``, one queued customer receives one transport removed
            from the station inventory. ``on_agent_assigned()`` is invoked before the
            assignment is announced. If no transport is available, the queued request
            is refused.
            """

            # Iterate through the available service types and their corresponding queues
            for service_name, queue in self.agent.waiting_lists.items():

                service = self.agent.services_list[service_name]
                mode = service.get("mode")

                if len(queue) > 0:

                    if mode == "behaviour":

                        template1 = Template()
                        template1.set_metadata("protocol", REQUEST_PROTOCOL)
                        template1.set_metadata("performative", INFORM_PERFORMATIVE)

                        if self.agent.service_available(service_name):

                            # Dequeue the first agent from the queue for the given service
                            agent_info = self.agent.queuebehaviour.dequeue_first_agent_to_waiting_list(service_name)

                            if agent_info is not None:
                                agent, kwargs = agent_info

                                # Increase the number of slots in use for this service
                                self.increase_slots_used(service_name)

                                # Inform the agent that they are being served
                                content = {"station_id": self.agent.name, "serving": True}
                                await self.inform_service(str(agent), content)

                                logger.info(
                                    "Agent[{}]: The agent [{}] with args: ({}) has slots used: ({})".format(
                                        self.agent.name,
                                        agent,
                                        kwargs,
                                        self.get_slot_number_used(service_name)
                                    )
                                )

                                # Get service-specific arguments
                                arguments_station = self.agent.show_service_arguments(service_name)
                                arguments_station["service_name"] = service_name
                                kwargs.update(arguments_station)

                                # Retrieve and instantiate the appropriate service behavior
                                one_shot_behaviour = self.agent.services_list[service_name]["one_shot_behaviour"]
                                one_shot_behaviour = one_shot_behaviour(str(agent), **kwargs)

                                # Add the behavior to the agent
                                self.agent.add_behaviour(one_shot_behaviour, template1)

                    elif mode == "agent":

                        if self.agent.has_available_agent(
                            service_name
                        ):

                            agent_info = (
                                self.agent.queuebehaviour
                                .dequeue_first_agent_to_waiting_list(
                                    service_name
                                )
                            )

                            if agent_info is not None:
                                customer_jid, kwargs = agent_info

                                transport_jid = (
                                    self.agent.assign_agent(
                                        service_name
                                    )
                                )

                                self.agent.on_agent_assigned(
                                    service_name,
                                    transport_jid
                                )

                                await self.inform_transport_assignment(
                                    str(customer_jid),
                                    service_name,
                                    str(transport_jid)
                                )

                                logger.info(
                                    "Agent[{}]: Assigned transport [{}] "
                                    "to customer [{}]".format(
                                        self.agent.name,
                                        transport_jid,
                                        customer_jid
                                    )
                                )

                        else:

                            agent_info = (
                                self.agent.queuebehaviour
                                .dequeue_first_agent_to_waiting_list(
                                    service_name
                                )
                            )

                            if agent_info is not None:
                                customer_jid, kwargs = agent_info

                                await self.refuse_service(
                                    str(customer_jid)
                                )
