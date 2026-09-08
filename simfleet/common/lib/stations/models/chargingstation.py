import json
import datetime
import asyncio

from loguru import logger
from spade.behaviour import CyclicBehaviour, OneShotBehaviour
from spade.message import Message
from spade.template import Template
from asyncio import CancelledError

from simfleet.common.agents.station.servicestationagent import ServiceStationAgent

from simfleet.communications.protocol import (
    REGISTER_PROTOCOL,
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    INFORM_PERFORMATIVE,
)


class ChargingStationAgent(ServiceStationAgent):
    """
    Service station providing energy-replenishment operations.

    ChargingStationAgent extends ServiceStationAgent with registration in the
    DirectoryAgent and supports slot-based services such as electric charging,
    gasoline refuelling, and diesel refuelling.

    Individual service executions are represented by OneShotBehaviour
    implementations. Service capacity, waiting queues, and dispatch remain
    responsibilities of ServiceStationAgent.
    """

    def __init__(self, agentjid, password):
        """
        Initialize the common service-station infrastructure.

        Args:
            agentjid (str): XMPP JID used by the station.
            password (str): XMPP authentication password.
        """
        ServiceStationAgent.__init__(self, agentjid, password)

        self.arguments = []

    def run_strategy(self):
        """
        Mark the station operational strategy as started.

        ChargingStationAgent currently relies on its service and registration
        behaviours rather than installing an additional station strategy here.
        """
        if not self.running_strategy:
            self.running_strategy = True

    def to_json(self):
        """
        Serialize the station using the common ServiceStation representation.

        Returns:
            dict: Serializable station state.
        """
        data = super().to_json()
        return data


    async def setup(self):
        """
        Initialize service handling and Directory registration.

        The common ServiceStation lifecycle is initialized first. A dedicated
        RegistrationBehaviour then registers the station and its available
        services with the DirectoryAgent.

        Local setup completes by marking the station ready.
        """
        await super().setup()
        logger.info("Agent[{}]: Charging station running".format(self.name))
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
                "EXCEPTION creating RegisterBehaviour in Station {}: {}".format(
                    self.agent_id, e
                )
            )

        self.ready = True


class RegistrationBehaviour(CyclicBehaviour):
    """
    Register the charging station with the DirectoryAgent.

    Registration advertises the station JID, physical position, and service
    types. The cyclic behaviour retries until the DirectoryAgent accepts the
    registration.
    """
    async def on_start(self):
        """Log the start of charging-station Directory registration."""
        logger.debug("Agent[{}]: Strategy ({}) started".format(self.agent.name, type(self).__name__))

    async def send_registration(self):
        """
        Send the station service catalogue and position to the DirectoryAgent.
        """
        logger.info(
            "Agent[{}]: The agent sent proposal to register to directory ({})".format(
                self.agent.name, self.agent.directory_id
            )
        )

        content = {
            "jid": str(self.agent.jid),
            "type": self.agent.show_services(),
            "position": self.agent.get_position(),
        }
        msg = Message()
        msg.to = str(self.agent.directory_id)
        msg.set_metadata("protocol", REGISTER_PROTOCOL)
        msg.set_metadata("performative", REQUEST_PERFORMATIVE)
        msg.body = json.dumps(content)
        await self.send(msg)

    async def run(self):
        """
        Execute one Directory-registration cycle.

        Registration requests continue while the station is unregistered.
        ACCEPT_PERFORMATIVE marks Directory registration as complete.
        """
        try:
            if not self.agent.registration:
                await self.send_registration()
            msg = await self.receive(timeout=10)
            if msg:
                performative = msg.get_metadata("performative")
                if performative == ACCEPT_PERFORMATIVE:
                    self.agent.set_registration(True)
                    logger.debug("Agent[{}]: Registration in the directory".format(self.agent.name))
        except CancelledError:
            logger.debug("Agent[{}]: Cancelling async tasks...".format(self.agent.name))
        except Exception as e:
            logger.error(
                "Agent[{}]: EXCEPTION in RegisterBehaviour: {}".format(
                    self.agent.name, e
                )
            )


class ChargingService(OneShotBehaviour):
    """
    Execute one electric-charging service.

    Charging duration is derived from the requested transport energy need and
    the configured charging power. Once the simulated charging interval has
    elapsed, the transport is informed and the station service slot is
    released.
    """
    def __init__(self, agent_id, **kwargs):
        """
        Initialize one electric-charging execution.

        Args:
            agent_id: Transport receiving the service.
            **kwargs: Service parameters including ``transport_need``,
                ``service_name``, and ``power``.
        """
        super().__init__()
        self.agent_id = agent_id

        if 'transport_need' in kwargs:
            self.transport_need = kwargs['transport_need']

        if 'service_name' in kwargs:
            self.service_type = kwargs['service_name']

        if 'power' in kwargs:
            self.power = kwargs['power']

    async def charging_transport(self):
        """
        Simulate the charging duration from transport need and charging power.
        """
        total_time = self.transport_need / self.power
        recarge_time = datetime.timedelta(seconds=total_time)
        logger.info(
            "Agent[{}]: The agent started charging transport [{}] for ({}) seconds.".format(
                self.agent.name, self.agent_id, recarge_time.total_seconds()
            )
        )

        await asyncio.sleep(recarge_time.total_seconds())


    async def inform_charging_complete(self):
        """
        Inform the transport that its charging service has completed.
        """

        reply = Message()
        reply.to = str(self.agent_id)
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", INFORM_PERFORMATIVE)
        content = {"charged": True}
        reply.body = json.dumps(content)
        await self.send(reply)

    async def run(self):
        """
        Execute charging, notify the transport, and release the occupied slot.
        """
        logger.debug("Agent[{}]: The station start charging.".format(self.agent.name))

        await self.charging_transport()

        logger.info(
            "Agent[{}]: The agent has finished receiving the service ({})".format(
                self.agent_id,
                self.service_type
            )
        )

        await self.inform_charging_complete()

        self.agent.servicebehaviour.decrease_slots_used(self.service_type)

class GasolineService(OneShotBehaviour):
    """
    Execute one gasoline-refuelling service.

    Refuelling duration is derived from transport need and the configured
    refuelling rate. Completion is reported to the transport before the
    service slot is released.
    """
    def __init__(self, agent_id, **kwargs):
        """
        Initialize one gasoline-refuelling execution.

        Args:
            agent_id: Transport receiving the service.
            **kwargs: Service parameters including ``transport_need``,
                ``service_name``, and ``refueling_rate``.
        """
        super().__init__()
        self.agent_id = agent_id

        if 'transport_need' in kwargs:
            self.transport_need = kwargs['transport_need']

        if 'service_name' in kwargs:
            self.service_type = kwargs['service_name']

        if 'refueling_rate' in kwargs:
            self.refueling_rate = kwargs['refueling_rate']


    async def charging_transport(self):
        """
        Simulate gasoline refuelling for the configured service duration.
        """
        total_time = self.transport_need / self.refueling_rate
        recarge_time = datetime.timedelta(seconds=total_time)
        logger.info(
            "Station {} started charging transport {} for {} seconds.".format(
                self.agent.name, self.agent_id, recarge_time.total_seconds()
            )
        )

        await asyncio.sleep(recarge_time.total_seconds())


    async def inform_charging_complete(self):
        """
        Inform the transport that gasoline refuelling has completed.
        """

        reply = Message()
        reply.to = str(self.agent_id)
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", INFORM_PERFORMATIVE)
        content = {"charged": True}
        reply.body = json.dumps(content)
        await self.send(reply)

    async def run(self):
        """
        Execute refuelling, notify the transport, and release the occupied slot.
        """
        logger.debug("Station {} start charging.".format(self.agent.name))

        await self.charging_transport()

        logger.info(
            "Agent {} has finished receiving the service {}".format(
                self.agent_id,
                self.service_type
            )
        )

        await self.inform_charging_complete()

        self.agent.servicebehaviour.decrease_slots_used(self.service_type)


class DieselService(OneShotBehaviour):
    """
    Execute one diesel-refuelling service.

    Refuelling duration is derived from transport need and the configured
    refuelling rate. Completion is reported before the station service slot is
    released.
    """
    def __init__(self, agent_id, **kwargs):
        """
        Initialize one diesel-refuelling execution.

        Args:
            agent_id: Transport receiving the service.
            **kwargs: Service parameters including ``transport_need``,
                ``service_name``, and ``refueling_rate``.
        """
        super().__init__()
        self.agent_id = agent_id

        if 'transport_need' in kwargs:
            self.transport_need = kwargs['transport_need']

        if 'service_name' in kwargs:
            self.service_type = kwargs['service_name']

        if 'refueling_rate' in kwargs:
            self.refueling_rate = kwargs['refueling_rate']

    async def charging_transport(self):
        """
        Simulate diesel refuelling for the configured service duration.
        """
        total_time = self.transport_need / self.refueling_rate
        recarge_time = datetime.timedelta(seconds=total_time)
        logger.info(
            "Station {} started charging transport {} for {} seconds.".format(
                self.agent.name, self.agent_id, recarge_time.total_seconds()
            )
        )

        await asyncio.sleep(recarge_time.total_seconds())


    async def inform_charging_complete(self):
        """
        Inform the transport that diesel refuelling has completed.
        """
        reply = Message()
        reply.to = str(self.agent_id)
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", INFORM_PERFORMATIVE)
        content = {"charged": True}
        reply.body = json.dumps(content)
        await self.send(reply)


    async def run(self):
        """
        Execute refuelling, notify the transport, and release the occupied slot.
        """
        logger.debug("Station {} start charging.".format(self.agent.name))

        await self.charging_transport()

        logger.info(
            "Agent {} has finished receiving the service {}".format(
                self.agent_id,
                self.service_type
            )
        )

        await self.inform_charging_complete()

        self.agent.servicebehaviour.decrease_slots_used(self.service_type)

