from spade.template import Template

from simfleet.common.agents.transport import TransportAgent
from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REGISTER_PROTOCOL,
)


class StationSharingAgent(TransportAgent):
    """
    Represents a shared transport belonging to a station-based
    sharing system.

    The transport is initially registered in a sharing station.
    During a trip it leaves that station and, when the trip ends,
    registers in the destination station.
    """

    def __init__(self, agentjid, password):
        super().__init__(agentjid, password)

        self.origin_station = None
        self.destination_station = None

    def set_origin_station(self, station_id):
        self.origin_station = station_id

    def get_origin_station(self):
        return self.origin_station

    def clear_origin_station(self):
        self.origin_station = None

    def set_destination_station(self, station_id):
        self.destination_station = station_id

    def get_destination_station(self):
        return self.destination_station

    def clear_destination_station(self):
        self.destination_station = None

    def run_strategy(self):
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
