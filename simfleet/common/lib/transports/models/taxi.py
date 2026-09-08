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
    Transport model for on-demand taxi services.

    TaxiAgent extends TransportAgent with the positional context required by
    the taxi strategy:

    ``initial_position``
        Position from which the taxi started its operational lifecycle.

    ``return_position``
        Pending position to which the taxi must return after completing a
        service when the active strategy requires a return journey.

    Customer assignment, service progression, movement, availability, and
    return decisions are handled by the taxi strategy.
    """

    def __init__(self, agentjid, password, **kwargs):
        """
        Initialize taxi-specific positional context.

        Args:
            agentjid (str): XMPP JID used by the taxi.
            password (str): XMPP authentication password.
            **kwargs: Additional compatibility arguments accepted by the model.
        """
        super().__init__(agentjid, password)

        self.initial_position = None
        self.return_position = None

    async def setup(self):
        """
        Initialize the taxi using the common TransportAgent lifecycle.
        """
        await super().setup()


    def set_initial_position(self, coords=None):
        """
        Set and preserve the taxi initial physical position.

        The common transport initialization is performed first and the resolved
        position is then copied into ``initial_position``.

        Args:
            coords (list | None): Initial coordinates.
        """
        super().set_initial_position(coords)

        position = self.get_position()

        if position:
            self.initial_position = list(position)

    def get_initial_position(self):
        """
        Return the taxi initial position.

        Returns:
            list | None: Initial coordinates when available.
        """
        return self.initial_position

    def set_return_position(self, position):
        """
        Store a pending return destination for the taxi.

        Args:
            position (list | None): Return coordinates. ``None`` clears the
            pending return destination.
        """
        if position:
            self.return_position = list(position)
        else:
            self.return_position = None

    def get_return_position(self):
        """
        Return the currently configured return destination.

        Returns:
            list | None: Pending return coordinates.
        """
        return self.return_position

    def has_return_position(self):
        """
        Return whether the taxi has a pending return destination.
        """
        return self.return_position is not None

    def clear_return_position(self):
        """
        Clear the pending taxi return destination.
        """
        self.return_position = None

