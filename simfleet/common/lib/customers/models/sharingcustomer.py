import asyncio
import json

from loguru import logger
from spade.behaviour import State
from spade.template import Template
from spade.message import Message

from simfleet.common.lib.customers.models.pedestrian import PedestrianAgent
from simfleet.communications.protocol import REQUEST_PROTOCOL, QUERY_PROTOCOL, REQUEST_PERFORMATIVE, INFORM_PERFORMATIVE, PROPOSE_PERFORMATIVE, CANCEL_PERFORMATIVE

from simfleet.utils.helpers import distance_in_meters
from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination
)

class SharingCustomerAgent(PedestrianAgent):

    def __init__(self, agentjid, password):
        super().__init__(agentjid, password)

        self.request = "taxi"       #Analizar para que se usa
        self.available_transports = None
        self.set("current_transport", None)
        self.current_transport_pos = None
        self.previous_closest_transport = None

        # NEW: Station Sharing
        self.station_dic = None         # Comparte con buscustomer
        self.type_service = "bike-sharing"   # Comparte con buscustomer
        self.destination_station = None    # Comparte con buscustomer
        self.current_station = None        # Comparte con buscustomer



        # ATRIBUTES FOR EVENT AND CALLBACK MANAGEMENT
        # self.__observers = defaultdict(list)
        # Customer arrived to transport event. Triggers when the customer stops its
        # MovingBehavior because it has arrived to the booked transport's position
        self.set("arrived_to_transport", None)

        self.arrived_to_transport_event = asyncio.Event(loop=self.loop)

        def arrived_to_transport_callback(old, new):
            if not self.arrived_to_transport_event.is_set() and new is True:
                self.arrived_to_transport_event.set()

        self.arrived_to_transport_callback = arrived_to_transport_callback

        self.set("arrived_to_destination", False)

        self.arrived_to_destination_event = asyncio.Event(loop=self.loop)

        def arrived_to_destination_callback(old, new):
            if not self.arrived_to_destination_event.is_set() and new is True:
                self.arrived_to_destination_event.set()

        self.arrived_to_destination_callback = arrived_to_destination_callback

    async def setup(self):
        """
        Sets up the customer agent by configuring behaviors and templates (TravelBehaviour).
        """
        await super().setup()

    #def watch_value(self, key, callback):
    #    """
    #    Registers an observer callback to be run when a value is changed
    #
    #    Args:
    #        key (str): the name of the value
    #        callback (function): a function to be called when the value changes. It receives two arguments: the old and the new value.
    #    """
    #    self.__observers[key].append(callback)

    async def set_position(self, coords=None):
        """
        Sets the position of the customer. If no position is provided, assigns a random one.

        Args:
            coords (list): Coordinates (longitude and latitude) where the customer is located.
        """
        await super().set_position(coords)
        self.set("current_pos", coords)

        if self.is_in_destination():
            logger.debug("Customer {} has arrived to its moving destination. Status: {}".format(self.agent_id, self.status))
            # inform the transport
            await self.arrived_to_transport()
        elif self.destination_station[1] == self.get_position():
            logger.debug(
                "Customer {} has arrived to its moving destination. Status: {}".format(self.agent_id, self.status))
            # inform the transport
            self.set("arrived_to_destination", True)

    def run_strategy(self):
        # CHECK IF IT NEEDS MODIFICATION
        """import json
        Runs the strategy for the customer agent.
        """
        if not self.running_strategy:
            template1 = Template()
            template1.set_metadata("protocol", REQUEST_PROTOCOL)
            template2 = Template()
            template2.set_metadata("protocol", QUERY_PROTOCOL)
            self.add_behaviour(self.strategy(), template1 | template2)
            self.running_strategy = True

    # Analizar si es útil
    def can_walk(self, coords):
        """
        Returns a boolean indicating id the distance between the customer's current position and
        the coordinates passed as a parameter is lower than the maximum walking distance of the agent

        Returns:
            boolean: If the customer is able to walk from their position to "coords"
        """
        if self.max_walking_dist is None:
            return True
        pos = self.get("current_pos")
        dist = distance_in_meters(self.get("current_pos"), coords)
        logger.debug(f"{self.name} max walking distance is {self.max_walking_dist}")
        logger.debug(f"Customer's position {pos}, transport position {coords}, distance {dist}")
        return distance_in_meters(self.get("current_pos"), coords) <= self.max_walking_dist


    async def arrived_to_transport(self):
        """
        Informs that the customer has arrived to its booked transport position.
        It must change the appropriate value to trigger a callback
        """

        self.set("path", None)
        self.chunked_path = None
        logger.info("Customer {} arrived to the transport {} position".format(self.name,
                                                                              self.get("current_transport")))
        logger.error(f"{self.name}::: length of distances: {len(self.distances)}, dist. walked to car: {sum(self.distances):.2f}")
        self.set("arrived_to_transport", True)


    # NEW: Station Sharing
    def setup_stations(self):
        """
        Sets up the current and destination stations for the customer based on the nearest available stations.
        """
        if self.current_station is None and self.destination_station is None:
            self.current_station = self.nearst_agent(self.station_dic, self.get_position())
            self.destination_station = self.nearst_agent(self.station_dic, self.customer_dest)

            logger.debug("Customer {} set current_station {} and destination_station {}".format(self.name,
                                                                                      self.current_station[0],
                                                                                      self.destination_station[0]))



class SharingCustomerStrategyBehaviour(State):

    async def on_start(self):
        """
        Initializes the logger and timers. Call to parent method if overloaded.
        """
        logger.debug("Strategy {} started in customer {}".format(type(self).__name__, self.agent.name))


    async def go_to_transport(self, transport_id, dest):
        logger.info("Customer {} on route to transport {}".format(self.agent.name, transport_id))
        self.agent.set("current_transport", transport_id)
        self.agent.current_transport_pos = dest
        logger.debug("go_to_transport --> {} {}".format(self.get("current_transport"), self.agent.current_transport_pos))
        try:
            await self.agent.move_to(self.agent.current_transport_pos)
        except AlreadyInDestination:
            await self.agent.arrived_to_transport()

    async def send_get_transports(self, content=None):
        """
        Sends an ``spade.message.Message`` to the FleetManager to request a list of available transports.
        It uses the QUERY_PROTOCOL and the REQUEST_PERFORMATIVE.
        If no content is set a default content with the type_service that needs
        Args:
            content (dict): Optional content dictionary
        """
        if content is None or len(content) == 0:
            content = {"customer_id": str(self.agent.jid)}
        if self.agent.fleetmanagers is not None:
            for fleetmanager in self.agent.fleetmanagers.keys():
                msg = Message()
                msg.to = str(fleetmanager)
                msg.set_metadata("protocol", REQUEST_PROTOCOL)
                msg.set_metadata("performative", REQUEST_PERFORMATIVE)
                msg.body = json.dumps(content)
                await self.send(msg)
            logger.info("Customer {} asked for a available transports to {}.".format(self.agent.name, fleetmanager))
        else:
            logger.warning("Customer {} has no fleet managers.".format(self.agent.name))


    async def send_proposal(self, transport_id, content=None):
        """
        Send a ``spade.message.Message`` with a proposal to a customer to pick up him.
        If the content is empty the proposal is sent without content.

        Args:
            transport_id (str): the id of the customer
            content (dict, optional): the optional content of the message
        """
        # COPIED FROM transport.py, CHECK IF NEEDS MODIFICATION
        reply = Message()
        reply.to = transport_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", PROPOSE_PERFORMATIVE)
        if content is None:
            content = {
                "customer_id": str(self.agent.jid),
            }
        reply.body = json.dumps(content)
        await self.send(reply)
        logger.info("Customer {} sent booking to transport {}".format(self.agent.name, transport_id))

    async def request_a_transport(self, content):
        """
            Request a transport to sharing-station

            Args:
                content (dict, optional): Information needed for registration.
        """
        if content is None:
            content = {}
        msg = Message()
        msg.to = self.agent.current_station[0]
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", REQUEST_PERFORMATIVE)
        msg.body = json.dumps(content)
        logger.debug("Customer {} asked to register to stop {} with destination {}".format(self.agent.name,
                                                                                           self.agent.current_station[0],
                                                                                           self.agent.destination_station[
                                                                                               1]))
        await self.send(msg)


    async def request_a_place_for_transport(self, content):
        """
            Request a transport to sharing-station

            Args:
                content (dict, optional): Information needed for registration.
        """
        if content is None:
            content = {}
        msg = Message()
        msg.to = self.agent.destination_station[0]
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        msg.body = json.dumps(content)
        logger.debug("Customer {} asked to register transport {}".format(self.agent.name,
                                                                         self.agent.destination_station[0]))
        await self.send(msg)


    async def cancel_proposal(self, transport_id, content=None):
        """
        Sends an ``spade.message.Message`` to a transport to cancel its booking.
        It uses the REQUEST_PROTOCOL and the REFUSE_PERFORMATIVE.

        Args:
            transport_id (str): The Agent JID of the transport
        """
        # COPIED FROM transport.py, CHECK IF NEEDS MODIFICATION
        reply = Message()
        reply.to = transport_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", CANCEL_PERFORMATIVE)
        if content is None:
            content = {
                "customer_id": str(self.agent.jid),
            }
        reply.body = json.dumps(content)
        await self.send(reply)
        logger.info("Customer {} sent cancel booking to transport {}".format(self.agent.name, transport_id))


    async def inform_transport(self, content=None):
        """
        Sends a ``spade.message.Mesade`` to the booked transport.
        It uses the ???_PROTOCOL and the ???_PERFORMATIVE.
        This method is used to inform the transport that the customer agent is in its position.
        The transport will be waiting for this message and, upon receiving it, it will pick the
        customer up and start to move.

        Args:
            content (dict): Optional content dictionary
        """
        # NOT SURE IF THIS WILL BE PERFORMED INSIDE arrived_to_transport OR HERE
        # DON'T IMPLEMENT BY NOW
        # ++++++++++++ IMPORTANT: SEND AS "origin": THE COORDINATES OF MY CURRENT POSITION
        # ++++++++++++ WHICH IS ALSO THE TRANSPORT'S POSITION
        msg = Message()
        msg.to = self.get("current_transport")
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        if content is None:
            content = {
                "customer_id": str(self.agent.jid),
                "origin": self.get("current_pos"),
                "dest": self.agent.customer_dest
            }
        msg.body = json.dumps(content)
        await self.send(msg)
        logger.info("Customer {} informed transport {} that they are in its position".format(self.agent.name,
                                                                                             self.get(
                                                                                                 "current_transport")))

    async def run(self):
        raise NotImplementedError
