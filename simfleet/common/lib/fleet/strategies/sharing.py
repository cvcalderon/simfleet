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


class SharingFleetManagerStrategy(FleetManagerStrategyBehaviour):
    """
    FleetManager strategy for free-floating sharing services.

    The strategy provides sharing customers with the currently
    available fleet resources that can be reached on foot.

    The FleetManager performs discovery and preliminary filtering,
    but the final resource selection is made by the customer.
    """

    async def run(self):

        if not self.agent.registration:
            await self.send_registration()

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
                "Agent[{}]: Invalid sharing request from [{}].".format(
                    self.agent.name,
                    msg.sender
                )
            )
            return

        if content.get("request_type") != "sharing_candidates":
            return

        origin = content.get(
            "origin"
        )

        max_walking_distance = content.get(
            "max_walking_distance"
        )

        if origin is None:
            logger.warning(
                "Agent[{}]: Sharing candidate request from [{}] "
                "has no origin.".format(
                    self.agent.name,
                    msg.sender
                )
            )
            return

        candidates = []

        for available_resource in self.agent.get_available_resources():

            resource = available_resource.get(
                "resource"
            )

            data = available_resource.get(
                "data"
            )

            if resource is None or data is None:
                continue

            resource_jid = resource.get(
                "jid"
            )

            position = data.get(
                "p"
            )

            if (
                resource_jid is None
                or position is None
            ):
                continue

            distance = distance_in_meters(
                origin,
                position
            )

            if (
                max_walking_distance is not None
                and distance > max_walking_distance
            ):
                continue

            candidates.append(
                {
                    "jid": str(resource_jid),
                    "position": position,
                    "distance": distance,
                    "assignments": data.get("a", 0),
                }
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
                "request_type": "sharing_candidates",
                "vehicles": candidates,
            }
        )

        await self.send(
            reply
        )

        logger.debug(
            "Agent[{}]: Sent {} sharing candidates "
            "to customer [{}].".format(
                self.agent.name,
                len(candidates),
                msg.sender
            )
        )
