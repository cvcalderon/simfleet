import json

from loguru import logger

from simfleet.common.agents.fleetmanager import FleetManagerStrategyBehaviour
from simfleet.utils.helpers import distance_in_meters


################################################################
#                                                              #
#                     FleetManager Strategy                    #
#                                                              #
################################################################

class PresenceRequestBehaviour(FleetManagerStrategyBehaviour):
    """
    Selects an available vehicle using its presence information.
    """

    requires_origin = True
    requires_assignments = False

    sort_key = staticmethod(lambda candidate: candidate["distance"])

    def get_presence_candidates(self, origin=None):
        candidates = []

        vehicles = self.agent.get_available_vehicles()

        for item in vehicles:
            vehicle = item["vehicle"]
            data = item["data"]

            position = data.get("p")

            if position is None:
                logger.debug(
                    "Agent[{}]: skipping vehicle [{}], presence has no position.".format(
                        self.agent.name,
                        vehicle.get("name")
                    )
                )
                continue

            assignments = data.get("a")

            if self.requires_assignments and assignments is None:
                logger.debug(
                    "Agent[{}]: skipping vehicle [{}], presence has no assignments.".format(
                        self.agent.name,
                        vehicle.get("name")
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
                    "jid": str(vehicle["jid"]),
                    "position": position,
                }
            )

        return candidates

    async def run(self):
        if not self.agent.registration:
            await self.send_registration()

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
                "Agent[{}]: no available vehicles found from presence data.".format(
                    self.agent.name
                )
            )
            return

        candidates.sort(key=self.sort_key)

        selected = candidates[0]

        logger.info(
            "Agent[{}]: selected vehicle [{}].".format(
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
