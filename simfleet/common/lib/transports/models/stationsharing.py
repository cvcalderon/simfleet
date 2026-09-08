from spade.template import Template

from simfleet.common.agents.transport import TransportAgent
from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REGISTER_PROTOCOL,
)


class StationSharingAgent(TransportAgent):
    """
    Transport model for station-based sharing services.

    A StationSharingAgent belongs to a station while it is available. During
    a customer trip the vehicle leaves its origin station and later registers
    with the destination station.

    The model stores only the station context required by that lifecycle.
    Station selection, registration messages, movement, recovery, and FSM
    transitions are implemented by the StationSharing strategy.
    """

    def __init__(self, agentjid, password):
        """
        Initialize station-sharing transfer context.

        Args:
            agentjid (str): XMPP JID used by the shared vehicle.
            password (str): XMPP authentication password.
        """
        super().__init__(agentjid, password)

        self.origin_station = None
        self.destination_station = None

    def set_origin_station(self, station_id):
        """
        Store the station from which the current sharing trip originated.

        Args:
            station_id: Origin station JID or identifier.
        """
        self.origin_station = station_id

    def get_origin_station(self):
        """
        Return the current trip origin station.
        """
        return self.origin_station

    def clear_origin_station(self):
        """
        Clear the current trip origin-station context.
        """
        self.origin_station = None

    def set_destination_station(self, station_id):
        """
        Store the station in which the vehicle must finish the current trip.

        Args:
            station_id: Destination station JID or identifier.
        """
        self.destination_station = station_id

    def get_destination_station(self):
        """
        Return the current trip destination station.
        """
        return self.destination_station

    def clear_destination_station(self):
        """
        Clear the current trip destination-station context.
        """
        self.destination_station = None

    def run_strategy(self):
        """
        Start the StationSharing operational strategy once.

        The strategy listens to both REQUEST_PROTOCOL service messages and
        REGISTER_PROTOCOL station-registration messages. The combined template
        allows the same FSM to manage customer service and station transfer
        lifecycle events.

        ``running_strategy`` prevents duplicate strategy instances.
        """
        if not self.running_strategy:

            request_template = Template()
            request_template.set_metadata(
                "protocol",
                REQUEST_PROTOCOL
            )

            register_template = Template()
            register_template.set_metadata(
                "protocol",
                REGISTER_PROTOCOL
            )

            self.add_behaviour(
                self.strategy(),
                request_template | register_template
            )

            self.running_strategy = True
