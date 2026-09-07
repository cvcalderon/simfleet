import json
#import ast
from loguru import logger
#from rich.status import Status

from spade.behaviour import State
from spade.message import Message

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    PROPOSE_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    REQUEST_PERFORMATIVE,
#    ACCEPT_PERFORMATIVE,
#    QUERY_PROTOCOL,
)

from simfleet.common.agents.transport import TransportAgent


class TaxiAgent(TransportAgent):
    """
        Represents a taxi agent, inheriting from `TransportAgent`. This class provides
        functionalities specific to taxis, such as managing assigned customers.

        Attributes:
            fleetmanager_id (str): The ID of the fleet manager the taxi belongs to.
            assigned_customer (dict): A dictionary storing the currently assigned customer's
                                      ID, origin, and destination.

        Methods:
            async add_assigned_taxicustomer(customer_id, origin=None, dest=None):
                Adds a customer to the taxi's assigned customer list.
            async remove_assigned_taxicustomer():
                Removes all assigned customers from the taxi's customer list.
        """
   # OLD
    def __init__(self, agentjid, password, **kwargs):
        super().__init__(agentjid, password)

        self.initial_position = None
        self.return_position = None
    #    self.set("assigned_customer", {})
        #OLD
        #self.fleetmanager_id = kwargs.get('fleet', None)

    async def setup(self):

        await super().setup()


    def set_initial_position(self, coords=None):
        super().set_initial_position(coords)

        position = self.get_position()

        if position:
            self.initial_position = list(position)

    def get_initial_position(self):
        return self.initial_position

    def set_return_position(self, position):
        if position:
            self.return_position = list(position)
        else:
            self.return_position = None

    def get_return_position(self):
        return self.return_position

    def has_return_position(self):
        return self.return_position is not None

    def clear_return_position(self):
        self.return_position = None

