import json
from spade.message import Message
from loguru import logger

from simfleet.common.agents.fleetmanager import FleetManagerStrategyBehaviour

from simfleet.communications.protocol import REQUEST_PROTOCOL, REQUEST_PERFORMATIVE, QUERY_PROTOCOL, INFORM_PERFORMATIVE
from simfleet.utils.status import TRANSPORT_WAITING
from simfleet.utils.helpers import distance_in_meters

################################################################
#                                                              #
#                     FleetManager Strategy                    #
#                                                              #
################################################################

class DelegateRequestBehaviour(FleetManagerStrategyBehaviour):
    """
    The default strategy for the FleetManager agent. By default it delegates all requests to all transports.
    """

    async def run(self):

        msg = await self.receive(timeout=5)

        logger.warning(
            "Agent[{}]: FleetManager has a queue of ({})".format(
                self.agent.name, self.mailbox_size()
            )
        )

        logger.debug("Manager received message: {}".format(msg))
        if msg:
            for resource in self.agent.get_fleet_resources().values():
                msg.to = str(resource["jid"])

                logger.debug(
                    "Manager sent request to resource {}".format(
                        resource["name"]
                    )
                )

                await self.send(msg)


################################################################
#                                                              #
#                 Sharing FleetManager Strategy                #
#                                                              #
################################################################
class SendAvailableTransportsBehaviour(FleetManagerStrategyBehaviour):
    """
    Awaits customer's requests and replies with the lest of available transports
    """
    async def on_start(self):
        await super().on_start()
        self.agent.available_transports = {}

    async def run(self):
        #if not self.agent.registration:
        #    await self.send_registration()

        msg = await self.receive(timeout=5)
        if msg:
            logger.debug("Manager received message: {}".format(msg))
            protocol = msg.get_metadata("protocol")
            if protocol == REQUEST_PROTOCOL:
                performative = msg.get_metadata("performative")
                if performative == REQUEST_PERFORMATIVE:  # Message from customer asking for transports
                    body = json.loads(msg.body)
                    reply = Message()
                    content = self.agent.available_transports
                    reply.to = str(msg.sender)
                    reply.set_metadata("protocol", QUERY_PROTOCOL)
                    reply.set_metadata("performative", INFORM_PERFORMATIVE)
                    reply.body = json.dumps(content)
                    await self.send(reply)
                    logger.debug("Fleet manager sent list of transports to customer {}".format(msg.sender))

                # Status message from transport
                elif performative == INFORM_PERFORMATIVE:
                    logger.debug(f"FleetManager STATUS MESSAGE received: current transports = {self.agent.available_transports.keys()} ***")
                    body = json.loads(msg.body)
                    if body["jid"] in self.agent.available_transports:
                        # If the transport is not waiting it is not available
                        if body["status"] != TRANSPORT_WAITING:
                            del self.agent.available_transports[body["jid"]]
                            logger.debug(f"DELETED {body['jid']} from available transports: current transports = {self.agent.available_transports.keys()}")
                    else:
                        # Store new transport if it is waiting(=available)
                        if body["status"] == TRANSPORT_WAITING:
                            self.agent.available_transports[body["jid"]] = body
                            logger.debug(f"ADDED {body['jid']} to available transports: current transports = {self.agent.available_transports.keys()}")



################################################################
#                                                              #
#               Logistic FleetManager Strategy                 #
#                                                              #
################################################################

class PresenceRequestBehaviour(FleetManagerStrategyBehaviour):
    """
    Selects an available resource using its presence information.
    """

    requires_origin = True
    requires_assignments = False

    sort_key = staticmethod(lambda candidate: candidate["distance"])

    def get_presence_candidates(self, origin=None):
        candidates = []

        resources = self.agent.get_available_resources()

        for item in resources:
            resource = item["resource"]
            data = item["data"]

            position = data.get("p")

            if position is None:
                logger.debug(
                    "Agent[{}]: skipping resource [{}], presence has no position.".format(
                        self.agent.name,
                        resource.get("name")
                    )
                )
                continue

            assignments = data.get("a")

            if self.requires_assignments and assignments is None:
                logger.debug(
                    "Agent[{}]: skipping resource [{}], presence has no assignments.".format(
                        self.agent.name,
                        resource.get("name")
                    )
                )
                continue

            distance = None

            if origin is not None:
                distance = distance_in_meters(
                    position,
                    origin
                )

            candidates.append(
                {
                    "distance": distance,
                    "assignments": assignments,
                    "jid": str(resource["jid"]),
                    "position": position,
                }
            )

        return candidates

    async def run(self):
        #if not self.agent.registration:
        #    await self.send_registration()

        msg = await self.receive(timeout=5)

        if not msg:
            return

        logger.debug(
            "Manager received message: {}".format(msg)
        )

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: invalid request content.".format(
                    self.agent.name
                )
            )
            return

        origin = content.get("origin")

        if self.requires_origin and origin is None:
            logger.warning(
                "Agent[{}]: request has no origin.".format(
                    self.agent.name
                )
            )
            return

        candidates = self.get_presence_candidates(origin)

        if not candidates:
            logger.warning(
                "Agent[{}]: no available resources found from presence data.".format(
                    self.agent.name
                )
            )
            return

        candidates.sort(key=self.sort_key)

        selected = candidates[0]

        logger.info(
            "Agent[{}]: selected resource [{}].".format(
                self.agent.name,
                selected["jid"]
            )
        )

        msg.to = selected["jid"]
        await self.send(msg)


class NearRequestBehaviour(PresenceRequestBehaviour):
    """
    Selects the nearest available vehicle.
    """

    requires_origin = True
    requires_assignments = False

    sort_key = staticmethod(
        lambda candidate: candidate["distance"]
    )


class FewerAssignmentsRequestBehaviour(PresenceRequestBehaviour):
    """
    Selects the available transport with the fewest completed assignments.
    """

    requires_origin = False
    requires_assignments = True

    sort_key = staticmethod(
        lambda candidate: candidate["assignments"]
    )


class FewerAssignmentsAndNearRequestBehaviour(PresenceRequestBehaviour):
    """
    Selects by fewer completed assignments first,
    then by nearest distance.
    """

    requires_origin = True
    requires_assignments = True

    sort_key = staticmethod(
        lambda candidate: (
            candidate["assignments"],
            candidate["distance"]
        )
    )


class NearAndFewerAssignmentsRequestBehaviour(PresenceRequestBehaviour):
    """
    Selects by nearest distance first,
    then by fewer completed assignments.
    """

    requires_origin = True
    requires_assignments = True

    sort_key = staticmethod(
        lambda candidate: (
            candidate["distance"],
            candidate["assignments"]
        )
    )
