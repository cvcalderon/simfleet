import json

from asyncio import CancelledError

from loguru import logger
from spade.message import Message
from spade.template import Template
from spade.behaviour import CyclicBehaviour

from simfleet.common.agents.station.queuestationagent import (
    QueueStationAgent,
)

from simfleet.communications.protocol import (
    REGISTER_PROTOCOL,
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    INFORM_PERFORMATIVE,
)


class PublicTransportStopAgent(
    QueueStationAgent
):

    def __init__(
        self,
        agentjid,
        password,
        **kwargs
    ):
        super().__init__(
            agentjid,
            password
        )

        self.patterns = []

        patterns = kwargs.get(
            "patterns",
            []
        )

        if not isinstance(
            patterns,
            list
        ):
            raise ValueError(
                "Public transport stop patterns must be a list."
            )

        for pattern_id in patterns:

            if not isinstance(
                pattern_id,
                str
            ):
                raise ValueError(
                    "Public transport stop pattern IDs must be strings."
                )

            if pattern_id in self.patterns:
                continue

            self.patterns.append(
                pattern_id
            )

            self.add_queue(
                pattern_id
            )

    def get_stop_id(self):
        return self.agent_id

    def get_registration_content(
        self
    ):

        return {
            "name":
                self.get_stop_id(),

            "jid":
                str(self.jid),

            "fleet_type":
                self.fleet_type,

            "resource_type":
                "stop",

            "position":
                self.get_position(),
        }

    async def setup(self):

        await super().setup()

        logger.info(
            "Public transport stop {} running".format(
                self.name
            )
        )

        #
        # Bootstrap registration
        #

        if self.get_registration_fleet():

            template = Template()
            template.set_metadata(
                "protocol",
                REGISTER_PROTOCOL
            )

            registration_behaviour = (
                PublicTransportStopRegistrationBehaviour()
            )

            self.add_behaviour(
                registration_behaviour,
                template
            )

        #
        # Local setup is complete.
        # Registration is checked separately
        # by dependencies_ready().
        #

        self.ready = True


    def run_strategy(self):

        if self.running_strategy:
            return

        if self.strategy is None:

            logger.error(
                "Public transport stop {} has no strategy".format(
                    self.name
                )
            )

            return

        template = Template()

        template.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        template.set_metadata(
            "performative",
            INFORM_PERFORMATIVE
        )

        self.add_behaviour(
            self.strategy(),
            template
        )

        self.running_strategy = True


class PublicTransportStopRegistrationBehaviour(CyclicBehaviour):

    async def on_start(self):

        logger.debug(
            "Registration behaviour started in public transport stop {}".format(
                self.agent.name
            )
        )

    async def send_registration(
        self
    ):

        fleet_id = (
            self.agent.get_registration_fleet()
        )

        if fleet_id is None:
            return

        content = (
            self.agent.get_registration_content()
        )

        msg = Message()

        msg.to = str(
            fleet_id
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

    async def run(self):

        try:

            if not self.agent.registration:

                await self.send_registration()

            msg = await self.receive(
                timeout=5
            )

            if not msg:
                return

            performative = (
                msg.get_metadata(
                    "performative"
                )
            )

            if (
                performative
                == ACCEPT_PERFORMATIVE
            ):

                content = {}

                if msg.body:
                    content = json.loads(
                        msg.body
                    )

                self.agent.set_registration(
                    True,
                    content
                )

                logger.info(
                    "Public transport stop {} registered in fleet {}".format(
                        self.agent.get_stop_id(),
                        self.agent.get_registration_fleet()
                    )
                )

                return

            if (
                performative
                == REFUSE_PERFORMATIVE
            ):

                logger.warning(
                    "Public transport stop {} registration refused by {}".format(
                        self.agent.get_stop_id(),
                        msg.sender
                    )
                )

        except CancelledError:

            logger.debug(
                "Cancelling public transport stop registration behaviour."
            )

        except Exception as exc:

            logger.error(
                "Exception registering public transport stop {}: {}".format(
                    self.agent.name,
                    exc
                )
            )


class PublicTransportStopStrategyBehaviour(CyclicBehaviour):

    async def on_start(self):

        logger.debug(
            "Public transport stop strategy started in {}".format(
                self.agent.name
            )
        )

    async def inform_customer(
        self,
        customer_id,
        vehicle_id,
        pattern_id
    ):

        content = {
            "request_type":
                "public_transport_vehicle_available",

            "vehicle_id":
                str(vehicle_id),

            "pattern_id":
                pattern_id,

            "stop":
                self.agent.get_stop_id(),
        }

        msg = Message()

        msg.to = str(
            customer_id
        )

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            INFORM_PERFORMATIVE
        )

        msg.body = json.dumps(
            content
        )

        await self.send(
            msg
        )

    async def process_vehicle_arrival(
        self,
        sender,
        content
    ):

        pattern_id = content.get(
            "pattern_id"
        )

        if pattern_id is None:
            logger.warning(
                "Vehicle {} arrived at stop {} without pattern_id".format(
                    sender,
                    self.agent.get_stop_id()
                )
            )
            return

        if pattern_id not in (
            self.agent.waiting_lists
        ):
            logger.warning(
                "Vehicle {} arrived at stop {} with unknown pattern {}".format(
                    sender,
                    self.agent.get_stop_id(),
                    pattern_id
                )
            )

            return

        free_capacity = content.get(
            "free_capacity",
            0
        )

        try:
            free_capacity = int(
                free_capacity
            )
        except Exception:
            free_capacity = 0

        if free_capacity <= 0:
            return

        queue = (
            self.agent.queuebehaviour.get_queue(
                pattern_id
            )
        )

        if not queue:
            return

        #
        # Snapshot of first N customers.
        # Customers are NOT removed yet.
        #

        waiting_customers = list(
            queue
        )[:free_capacity]

        vehicle_id = content.get(
            "vehicle_id",
            str(sender)
        )

        for agent_info in waiting_customers:

            if agent_info is None:
                continue

            customer_id, _ = (
                agent_info
            )

            await self.inform_customer(
                customer_id=customer_id,
                vehicle_id=vehicle_id,
                pattern_id=pattern_id
            )

        logger.debug(
            "Stop {} informed {} customers for pattern {} and vehicle {}".format(
                self.agent.get_stop_id(),
                len(waiting_customers),
                pattern_id,
                vehicle_id
            )
        )

    def process_customer_boarded(
        self,
        content
    ):

        pattern_id = content.get(
            "pattern_id"
        )

        customer_id = content.get(
            "customer_id"
        )

        if (
            pattern_id is None
            or customer_id is None
        ):
            return

        if pattern_id not in (
            self.agent.waiting_lists
        ):
            return

        self.agent.queuebehaviour.dequeue_agent_to_waiting_list(
            pattern_id,
            str(customer_id)
        )

        logger.debug(
            "Customer {} removed from queue {} at stop {}".format(
                customer_id,
                pattern_id,
                self.agent.get_stop_id()
            )
        )


    async def run(self):

        try:

            msg = await self.receive(
                timeout=5
            )

            if not msg:
                return

            content = json.loads(
                msg.body
            )

            request_type = content.get(
                "request_type"
            )

            if request_type == (
                "public_transport_vehicle_arrival"
            ):

                await self.process_vehicle_arrival(
                    msg.sender,
                    content
                )

                return

            if request_type == (
                "public_transport_customer_boarded"
            ):

                self.process_customer_boarded(
                    content
                )

                return

            logger.warning(
                "Public transport stop {} received unsupported INFORM {}".format(
                    self.agent.get_stop_id(),
                    request_type
                )
            )

        except CancelledError:

            logger.debug(
                "Cancelling public transport stop strategy."
            )

        except Exception as exc:

            logger.error(
                "Exception in public transport stop {}: {}".format(
                    self.agent.name,
                    exc
                )
            )

