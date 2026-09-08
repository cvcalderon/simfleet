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
    Default FleetManager strategy that broadcasts requests to the fleet.

    Every message received by the strategy is forwarded to every resource
    currently registered with the FleetManager. The original message object
    is reused while its destination is changed for each resource.

    Resource selection, availability ranking, and load balancing are
    deliberately not performed by this strategy.
    """

    async def run(self):
        """
        Receive one fleet request and forward it to every registered resource.

        When no message is received before the behaviour timeout, no delegation
        is performed during that iteration.
        """
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
    Maintain and expose a message-driven registry of available transports.

    The strategy supports two REQUEST_PROTOCOL interactions:

    ``REQUEST_PERFORMATIVE``
        A customer requests currently available transports. The complete local
        ``available_transports`` mapping is returned through QUERY_PROTOCOL /
        INFORM_PERFORMATIVE.

    ``INFORM_PERFORMATIVE``
        A transport reports its current status. Transports in
        ``TRANSPORT_WAITING`` are stored as available; transports leaving that
        status are removed from the local registry.

    This strategy uses explicit transport status messages rather than the
    FleetManager's generic live-Presence resource-selection API.
    """
    async def on_start(self):
        """
        Initialize the message-driven available-transport registry.

        FleetManager strategy startup logging is delegated to the parent hook.
        """
        await super().on_start()
        self.agent.available_transports = {}

    async def run(self):
        """
        Process one available-transport registry interaction.

        Customer REQUEST messages receive the complete current registry.

        Transport INFORM messages update that registry according to
        ``TRANSPORT_WAITING`` status: waiting transports are added or refreshed,
        while previously registered transports reporting another status are
        removed.
        """
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
    Base FleetManager strategy for selecting one resource from Presence data.

    Candidate resources come from ``FleetManagerAgent.get_available_resources()``,
    which prefers live XMPP Presence and may fall back to the registered
    message-based Presence mirror.

    Each candidate may expose two compact Presence attributes:

    ``p``
        Current resource position.

    ``a``
        Number of completed assignments advertised by the transport.

    Concrete subclasses define three policy parameters:

    ``requires_origin``
        Whether the incoming request must contain an origin.

    ``requires_assignments``
        Whether candidate Presence must contain assignment information.

    ``sort_key``
        Candidate ordering used to select the first available resource.

    The original incoming message is delegated to exactly one selected
    resource.
    """

    requires_origin = True
    requires_assignments = False

    sort_key = staticmethod(lambda candidate: candidate["distance"])

    def get_presence_candidates(self, origin=None):
        """
        Build sortable candidates from currently available fleet resources.

        Resources without Presence position ``p`` are excluded.

        When the active policy requires assignment information, resources without
        Presence field ``a`` are also excluded.

        Geographic distance to ``origin`` is calculated only when an origin is
        supplied. Candidate positions and assignment counts are otherwise
        preserved from Presence.

        Args:
            origin (list | None): Request origin used for distance ranking.

        Returns:
            list[dict]: Candidate mappings containing ``jid``, ``position``,
            ``distance``, and ``assignments``.
        """
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
        """
        Select and delegate one fleet request using the configured Presence policy.

        The request body must be valid JSON. Policies requiring an origin reject
        requests that do not provide one.

        Available resources are converted into policy candidates, sorted with
        ``sort_key``, and the first candidate receives the original request
        message.

        Invalid requests or an empty candidate set are logged and ignored; no
        refusal reply is generated by this strategy.
        """

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
    Select the geographically nearest available resource.

    The request must contain an origin. Candidate assignment counts are not
    required and do not influence ordering.
    """

    requires_origin = True
    requires_assignments = False

    sort_key = staticmethod(
        lambda candidate: candidate["distance"]
    )


class FewerAssignmentsRequestBehaviour(PresenceRequestBehaviour):
    """
    Select the available resource with the fewest completed assignments.

    Request origin is not required. Candidates must advertise assignment count
    ``a`` through Presence.

    Geographic distance does not influence ordering.
    """

    requires_origin = False
    requires_assignments = True

    sort_key = staticmethod(
        lambda candidate: candidate["assignments"]
    )


class FewerAssignmentsAndNearRequestBehaviour(PresenceRequestBehaviour):
    """
    Select by completed assignments first and geographic distance second.

    Both request origin and candidate assignment information are required.

    Candidate ordering is lexicographic:

    1. fewer completed assignments;
    2. shorter geographic distance when assignment counts tie.
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
    Select by geographic distance first and completed assignments second.

    Both request origin and candidate assignment information are required.

    Candidate ordering is lexicographic:

    1. shorter geographic distance;
    2. fewer completed assignments when distances tie.
    """

    requires_origin = True
    requires_assignments = True

    sort_key = staticmethod(
        lambda candidate: (
            candidate["distance"],
            candidate["assignments"]
        )
    )
