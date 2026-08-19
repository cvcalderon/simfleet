import json

from loguru import logger
from spade.message import Message

from simfleet.common.agents.fleetmanager import FleetManagerStrategyBehaviour

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    INFORM_PERFORMATIVE,
    REQUEST_PERFORMATIVE,
)

from simfleet.utils.helpers import distance_in_meters


class StationSharingFleetManagerStrategy(FleetManagerStrategyBehaviour):
    """
    FleetManager strategy for station-based sharing services.

    The strategy discovers operational sharing stations and
    provides the customer with two candidate lists:

    - origin stations with available bikes
    - destination stations with available docks

    The FleetManager performs preliminary filtering based on
    walking distance, but the final station selection is made
    by the customer.
    """

    async def run(self):

        #if not self.agent.registration:
        #    await self.send_registration()

        msg = await self.receive(timeout=5)

        if not msg:
            return

        logger.debug(
            "Agent[{}]: FleetManager received message from [{}]: {}".format(
                self.agent.name,
                msg.sender,
                msg.body
            )
        )

        performative = msg.get_metadata(
            "performative"
        )

        if performative != REQUEST_PERFORMATIVE:
            return

        try:
            content = json.loads(
                msg.body
            )

        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: Invalid station-sharing request "
                "from [{}].".format(
                    self.agent.name,
                    msg.sender
                )
            )
            return

        if (
            content.get("request_type")
            != "station_sharing_candidates"
        ):
            return

        origin = content.get(
            "origin"
        )

        dest = content.get(
            "dest"
        )

        max_walking_distance = content.get(
            "max_walking_distance"
        )

        if origin is None or dest is None:

            logger.warning(
                "Agent[{}]: Station-sharing request from [{}] "
                "has no origin or destination.".format(
                    self.agent.name,
                    msg.sender
                )
            )

            return

        origin_stations = []
        destination_stations = []

        for available_resource in (
            self.agent.get_available_resources()
        ):

            resource = available_resource.get(
                "resource"
            )

            data = available_resource.get(
                "data"
            )

            if resource is None or data is None:
                continue

            station_jid = resource.get(
                "jid"
            )

            position = data.get(
                "p"
            )

            available_bikes = data.get(
                "b"
            )

            available_docks = data.get(
                "d"
            )

            capacity = data.get(
                "c"
            )

            if (
                station_jid is None
                or position is None
                or available_bikes is None
                or available_docks is None
                or capacity is None
            ):
                continue

            station = {
                "jid": str(station_jid),
                "position": position,
                "available_bikes": available_bikes,
                "available_docks": available_docks,
                "capacity": capacity,
            }

            distance_to_origin = distance_in_meters(
                origin,
                position
            )

            distance_to_destination = distance_in_meters(
                dest,
                position
            )

            if available_bikes > 0:

                if (
                    max_walking_distance is None
                    or distance_to_origin <= max_walking_distance
                ):
                    origin_stations.append(
                        station.copy()
                    )

            if available_docks > 0:

                if (
                    max_walking_distance is None
                    or distance_to_destination <= max_walking_distance
                ):
                    destination_stations.append(
                        station.copy()
                    )

        reply = Message()

        reply.to = str(
            msg.sender
        )

        reply.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        reply.set_metadata(
            "performative",
            INFORM_PERFORMATIVE
        )

        reply.body = json.dumps(
            {
                "request_type": "station_sharing_candidates",
                "origin_stations": origin_stations,
                "destination_stations": destination_stations,
            }
        )

        await self.send(
            reply
        )

        logger.debug(
            "Agent[{}]: Sent {} origin stations and {} "
            "destination stations to customer [{}].".format(
                self.agent.name,
                len(origin_stations),
                len(destination_stations),
                msg.sender
            )
        )
