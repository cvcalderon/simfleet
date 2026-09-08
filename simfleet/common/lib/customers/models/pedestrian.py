from spade.template import Template

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    QUERY_PROTOCOL,
)

from simfleet.common.agents.customer import CustomerAgent
from simfleet.common.mixins.movable import MovableMixin

class PedestrianAgent(MovableMixin, CustomerAgent):
    """
    Customer model capable of independent pedestrian movement.

    PedestrianAgent combines the common CustomerAgent lifecycle with
    MovableMixin so a customer can walk between mobility-service stages.

    The model stores:

    - pedestrian movement destination context;
    - an optional maximum walking distance.

    Its operational strategy may use REQUEST_PROTOCOL and QUERY_PROTOCOL,
    while the inherited TravelBehaviour continues handling TRAVEL_PROTOCOL
    position updates received from transports.
    """
    def __init__(self, agentjid, password):
        """
        Initialize customer and pedestrian-movement state.

        CustomerAgent and MovableMixin are initialized explicitly because this
        class combines both capabilities.

        Args:
            agentjid (str): XMPP JID used by the pedestrian customer.
            password (str): XMPP authentication password.
        """
        CustomerAgent.__init__(self, agentjid, password)
        MovableMixin.__init__(self)

        self.pedestrian_dest = None
        self.max_walking_dist = None

    def run_strategy(self):
        """
        Start the configured pedestrian-capable customer strategy once.

        The strategy receives both REQUEST_PROTOCOL and QUERY_PROTOCOL messages.
        ``running_strategy`` prevents duplicate strategy instances.
        """
        if not self.running_strategy:
            template1 = Template()
            template1.set_metadata("protocol", REQUEST_PROTOCOL)
            template2 = Template()
            template2.set_metadata("protocol", QUERY_PROTOCOL)
            self.add_behaviour(self.strategy(), template1 | template2)
            self.running_strategy = True

    async def set_position(self, coords=None):
        """
        Update the physical position of the pedestrian customer.

        The inherited geolocation state and the customer ``current_pos`` value
        are kept synchronized.

        Args:
            coords (list | None): New longitude/latitude coordinates.
        """
        await super().set_position(coords)
        self.set("current_pos", coords)


    def set_max_walking_dist(self, distance):
        """
        Configure the maximum distance the customer is willing to walk.

        Args:
            distance: Maximum walking distance. ``None`` means that this generic
                pedestrian constraint is not configured.
        """
        self.max_walking_dist = distance
