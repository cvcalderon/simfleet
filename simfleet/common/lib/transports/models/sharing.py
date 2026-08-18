import asyncio
import json

from loguru import logger
from spade.behaviour import State
from spade.template import Template
from spade.message import Message

from simfleet.common.agents.transport import TransportAgent

from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_IN_CUSTOMER_PLACE, TRANSPORT_MOVING_TO_STATION, TRANSPORT_MOVING_TO_DESTINATION, CUSTOMER_IN_DEST, CUSTOMER_LOCATION

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    INFORM_PERFORMATIVE,
    ACCEPT_PERFORMATIVE
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination
)

class SharingAgent(TransportAgent):

    # def __init__(self, agentjid, password, **kwargs):
    #     super().__init__(agentjid, password)
    #
    #     self.fleetmanager_id = kwargs.get('fleet', None)
    #     self.current_customer_orig = None
    #     self.current_customer_dest = None
    pass

    # async def setup(self):
    #     #self.set_type("transport")
    #     self.set_status()
    #
    #     await super().setup()

        #try:
        #    template = Template()
        #    template.set_metadata("protocol", REGISTER_PROTOCOL)
        #    register_behaviour = RegistrationBehaviour()
        #    self.add_behaviour(register_behaviour, template)
        #    while not self.has_behaviour(register_behaviour):
        #        logger.warning("Transport {} could not create RegisterBehaviour. Retrying...".format(self.agent_id))
        #        self.add_behaviour(register_behaviour, template)
        #    self.ready = True
        #except Exception as e:
        #    logger.error("EXCEPTION creating RegisterBehaviour in Transport {}: {}".format(self.agent_id, e))

    #Analizar si quitarlo
    #def set_type(self, transport_type):  # new
    #    self.transport_type = transport_type

    #def set_status(self, state=TRANSPORT_WAITING):  # new
    #    self.status = state

    #def is_customer_in_transport(self):
    #    return self.get("customer_in_transport") is not None

    # async def drop_customer(self):
    #     """
    #     Drops the customer that the transport is carring in the current location.
    #     """
    #     customers = self.get("current_customer")
    #     customer_id = next(iter(customers.items()))[0]
    #
    #     #await self.inform_customer(customer_id, CUSTOMER_IN_DEST)
    #     self.status = TRANSPORT_WAITING
    #     logger.info("Transport {} has dropped the customer {} in destination.".format(self.agent_id,
    #                                                                                   customer_id))
    #
    #     self.remove_customer_in_transport(customer_id)
    #
    #     #self.set("current_customer", None)
    #     #self.set("current_customer", {})
    #     self.set("customer_in_transport", None)

    # async def set_position(self, coords=None):
    #     """
    #     Sets the position of the transport. If no position is provided it is located in a random position.
    #
    #     Args:
    #         coords (list): a list coordinates (longitude and latitude)
    #     """
    #
    #     await super().set_position(coords)
    #     self.set("current_pos", coords)
    #
    #     # if the transport has arrived to its self.dest position
    #     if self.is_in_destination():
    #         logger.info("Transport {} has arrived to destination. Status: {}".format(self.agent_id, self.status))
    #         await self.drop_customer()


