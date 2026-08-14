import json
import ast

from simfleet.utils.helpers import distance_in_meters
from spade.presence import PresenceNotFound
from loguru import logger

from simfleet.common.lib.fleet.models.logisticfleetmanager import LogisticFleetManagerStrategyBehaviour


################################################################
#                                                              #
#                     FleetManager Strategy                    #
#                                                              #
################################################################

class PresenceRequestBehaviour(LogisticFleetManagerStrategyBehaviour):
    """Selects a vehicle from presence data and falls back to registered vehicles."""

    sort_key = staticmethod(lambda candidate: candidate[0])

    async def _broadcast_to_registered_transports(self, msg):
        vehicles = self.get_vehicle_agents() or {}
        if not vehicles:
            logger.warning(
                "Agent[{}]: no registered vehicles available for broadcast.".format(
                    self.agent.name
                )
            )
            return

        logger.warning(
            "Agent[{}]: no valid presence candidates. Broadcasting to {} registered vehicles.".format(
                self.agent.name, len(vehicles)
            )
        )
        for vehicle in vehicles.values():
            msg.to = str(vehicle["jid"])
            logger.debug(
                "Manager sent request to vehicle {}".format(vehicle["name"])
            )
            await self.send(msg)

    def _get_presence_candidates(self, origin):
        candidates = []
        contacts = self.agent.presence.get_contacts()

        for jid in contacts:
            try:
                presence = self.agent.presence.get_contact_presence(jid)
            except PresenceNotFound:
                logger.debug(
                    "Agent[{}]: skipping [{}], no presence information yet.".format(
                        self.agent.name, jid
                    )
                )
                continue

            if not self.agent._is_available(presence.type):
                continue

            try:
                status_presence = ast.literal_eval(presence.status)
                pos, clients = status_presence
            except (SyntaxError, ValueError, TypeError):
                logger.debug(
                    "Agent[{}]: skipping [{}], invalid presence status: {!r}.".format(
                        self.agent.name, jid, presence.status
                    )
                )
                continue

            if pos is None:
                logger.debug(
                    "Agent[{}]: skipping [{}], presence has no position.".format(
                        self.agent.name, jid
                    )
                )
                continue

            dist = distance_in_meters(pos, origin)
            candidates.append((dist, clients, str(jid), pos))

        return candidates

    async def run(self):
        if not self.agent.registration:
            await self.send_registration()

        msg = await self.receive(timeout=5)

        logger.debug("Manager received message: {}".format(msg))
        if not msg:
            return

        content = json.loads(msg.body)
        origin = content.get("origin")

        if origin is None:
            logger.warning(
                "Agent[{}]: request has no origin. Falling back to broadcast.".format(
                    self.agent.name
                )
            )
            await self._broadcast_to_registered_transports(msg)
            return

        candidates = self._get_presence_candidates(origin)

        if not candidates:
            await self._broadcast_to_registered_transports(msg)
            return

        candidates.sort(key=self.sort_key)
        best_dist, best_clients, best_jid, best_pos = candidates[0]

        logger.info("Agent [{}]: Candidate {}".format(self.agent.name, candidates[0]))

        msg.to = str(best_jid)
        await self.send(msg)


class NearRequestBehaviour(PresenceRequestBehaviour):
    """
    Selects the nearest available vehicle according to presence data.
    """
    sort_key = staticmethod(lambda candidate: candidate[0])


class FewerCustomersRequestBehaviour(PresenceRequestBehaviour):
    """
    Selects the available vehicle with the fewest assigned customers.
    """
    sort_key = staticmethod(lambda candidate: candidate[1])


class FewerCustomersAndNearRequestBehaviour(PresenceRequestBehaviour):
    """
    Selects by fewer assigned customers first, then nearest distance.
    """
    sort_key = staticmethod(lambda candidate: (candidate[1], candidate[0]))


class NearAndFewerCustomersRequestBehaviour(PresenceRequestBehaviour):
    """
    Selects by nearest distance first, then fewer assigned customers.
    """
    sort_key = staticmethod(lambda candidate: (candidate[0], candidate[1]))
