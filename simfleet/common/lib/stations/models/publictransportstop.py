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
    """
    Station model representing a scheduled public transport stop.

    Each directional Pattern served by the stop owns an independent FIFO
    customer queue. Customers therefore wait for a Pattern rather than for a
    generic route or bus line.

    The stop registers as a fleet resource with its PublicTransport
    FleetManager. Its operational strategy receives vehicle-arrival
    notifications and informs a bounded number of waiting customers according
    to the vehicle's free passenger capacity.

    Customers are removed from a Pattern queue only after boarding is
    explicitly confirmed.
    """

    def __init__(
        self,
        agentjid,
        password,
        **kwargs
    ):
        """
        Initialize Pattern-specific waiting queues for the stop.

        Args:
            agentjid (str): XMPP JID used by the stop.
            password (str): XMPP authentication password.
            **kwargs: Stop configuration containing an optional ``patterns`` list.

        Raises:
            ValueError: If ``patterns`` is not a list or contains non-string
                Pattern identifiers.
        """
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
        """
        Return the canonical stop identifier.

        Returns:
            str: Agent identifier used as the stop ID.
        """
        return self.agent_id

    def get_registration_content(
        self
    ):
        """
        Build the stop resource payload sent to its FleetManager.

        Returns:
            dict: Stop identity, resource type, fleet type, and physical position.
        """

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
        """
        Initialize queue handling and bootstrap FleetManager registration.

        Local setup marks the stop ready. Registration dependencies remain
        evaluated separately by the inherited readiness contract.
        """

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
        """
        Start the public transport stop strategy once.

        The strategy listens only to REQUEST_PROTOCOL / INFORM_PERFORMATIVE
        messages carrying vehicle-arrival and customer-boarded notifications.

        ``running_strategy`` prevents duplicate strategy instances.
        """

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
    """
    Register a public transport stop with its configured FleetManager.

    Registration is retried while the stop remains unregistered.
    ACCEPT_PERFORMATIVE completes registration; REFUSE_PERFORMATIVE leaves
    the cyclic behaviour available for subsequent attempts.
    """

    async def on_start(self):
        """Log the start of public transport stop registration."""

        logger.debug(
            "Registration behaviour started in public transport stop {}".format(
                self.agent.name
            )
        )

    async def send_registration(
        self
    ):
        """
        Send the stop resource definition to its configured FleetManager.
        """
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
        """
        Execute one FleetManager-registration cycle and process its response.
        """
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
    """
    Coordinate waiting customers with arriving public transport vehicles.

    When a vehicle reports arrival, the behaviour takes a snapshot of at most
    ``free_capacity`` customers from the queue associated with the vehicle's
    directional Pattern and informs them that the vehicle is available.

    Queue entries are deliberately retained during this notification phase.
    A customer is removed only after an explicit
    ``public_transport_customer_boarded`` confirmation is received.
    """
    async def on_start(self):
        """Log the start of public transport stop coordination."""
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
        """
        Inform one waiting customer that a compatible vehicle is available.

        Args:
            customer_id: Waiting customer JID.
            vehicle_id: Available public transport vehicle JID.
            pattern_id: Directional Pattern served by the arriving vehicle.
        """
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
        """
        Process arrival of a public transport vehicle at this stop.

        The vehicle must advertise a Pattern known by the stop and positive free
        capacity. At most that number of customers is selected from the
        corresponding Pattern queue.

        Selection uses a queue snapshot: customers are informed but remain queued
        until boarding confirmation.

        Args:
            sender: Vehicle message sender.
            content (dict): Vehicle-arrival payload.
        """
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
        """
        Remove a customer from its Pattern queue after confirmed boarding.

        Args:
            content (dict): Boarding confirmation containing ``pattern_id`` and
                ``customer_id``.
        """
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
        """
        Process one public transport stop INFORM message.

        Supported request types are:

        ``public_transport_vehicle_arrival``
            Advertise an arriving vehicle and its free capacity.

        ``public_transport_customer_boarded``
            Confirm boarding and remove that customer from the Pattern queue.

        Unsupported INFORM messages are logged and ignored.
        """
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

