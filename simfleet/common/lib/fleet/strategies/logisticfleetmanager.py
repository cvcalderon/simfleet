import asyncio
import json
import time
import ast

from asyncio import Queue

from simfleet.utils.helpers import distance_in_meters
from spade.presence import PresenceManager
from spade.message import Message
from loguru import logger

from simfleet.common.lib.fleet.models.logisticfleetmanager import LogisticFleetManagerStrategyBehaviour

from simfleet.communications.protocol import REQUEST_PROTOCOL, REQUEST_PERFORMATIVE, QUERY_PROTOCOL, INFORM_PERFORMATIVE
from simfleet.utils.status import TRANSPORT_WAITING


################################################################
#                                                              #
#                     FleetManager Strategy                    #
#                                                              #
################################################################

class NearRequestBehaviour(LogisticFleetManagerStrategyBehaviour):
    """
    The default strategy for the FleetManager agent. By default it delegates all requests to all transports.
    """

    async def run(self):
        if not self.agent.registration:
            await self.send_registration()

        msg = await self.receive(timeout=5)

        logger.debug("Manager received message: {}".format(msg))
        if msg:

            content = json.loads(msg.body)

            if "origin" in content:
                origin = content["origin"]

            contacts = self.agent.presence.get_contacts()
            candidates = []

            for jid, contact in contacts.items():

                presence = self.agent.presence.get_contact_presence(jid)

                current_presence = presence.type
                status_presence = ast.literal_eval(presence.status) #Castear a tupla

                if not self.agent._is_available(current_presence):
                    continue

                pos, clients = status_presence
                dist = distance_in_meters(pos, origin)

                candidates.append((dist, clients, str(jid), pos))

            if not candidates:
                logger.warning("Agent[{}]: no available transports. Falling back to broadcast.".format(self.agent.name))
                return

            # Orden: primero menor distancia, luego menor nº de clientes
            #candidates.sort(key=lambda t: (t[0], t[1]))

            # Orden: primero menor nº de clientes, luego menor distancia
            #candidates.sort(key=lambda t: (t[1], t[0]))

            # Orden: primero menor distancia solo
            candidates.sort(key=lambda t: t[0])

            # Orden: primero menor nº de clientes
            #candidates.sort(key=lambda t: t[1])

            best_dist, best_clients, best_jid, best_pos = candidates[0]

            logger.info(
                "Agent [{}]: Candidate {}".format(self.agent.name, candidates[0])
            )

            msg.to = str(best_jid)
            await self.send(msg)


class FewerCustomersRequestBehaviour(LogisticFleetManagerStrategyBehaviour):
    """
    The default strategy for the FleetManager agent. By default it delegates all requests to all transports.
    """

    async def run(self):
        if not self.agent.registration:
            await self.send_registration()

        msg = await self.receive(timeout=5)

        logger.debug("Manager received message: {}".format(msg))
        if msg:

            content = json.loads(msg.body)

            if "origin" in content:
                origin = content["origin"]

            contacts = self.agent.presence.get_contacts()
            candidates = []

            for jid, contact in contacts.items():

                presence = self.agent.presence.get_contact_presence(jid)

                current_presence = presence.type
                status_presence = ast.literal_eval(presence.status) #Castear a tupla

                if not self.agent._is_available(current_presence):
                    continue

                pos, clients = status_presence
                dist = distance_in_meters(pos, origin)

                candidates.append((dist, clients, str(jid), pos))

            if not candidates:
                logger.warning("Agent[{}]: no available transports. Falling back to broadcast.".format(self.agent.name))
                return

            # Orden: primero menor distancia, luego menor nº de clientes
            #candidates.sort(key=lambda t: (t[1], t[0]))

            # Orden: primero menor nº de clientes, luego menor distancia
            #candidates.sort(key=lambda t: (t[0], t[1]))

            # Orden: primero menor distancia solo
            #candidates.sort(key=lambda t: t[0])

            # Orden: primero menor nº de clientes
            candidates.sort(key=lambda t: t[1])

            best_dist, best_clients, best_jid, best_pos = candidates[0]

            logger.info(
                "Agent [{}]: Candidate {}".format(self.agent.name, candidates[0])
            )

            msg.to = str(best_jid)
            await self.send(msg)


class FewerCustomersAndNearRequestBehaviour(LogisticFleetManagerStrategyBehaviour):
    """
    The default strategy for the FleetManager agent. By default it delegates all requests to all transports.
    """

    async def run(self):
        if not self.agent.registration:
            await self.send_registration()

        msg = await self.receive(timeout=5)

        logger.debug("Manager received message: {}".format(msg))
        if msg:

            content = json.loads(msg.body)

            if "origin" in content:
                origin = content["origin"]

            contacts = self.agent.presence.get_contacts()
            candidates = []

            for jid, contact in contacts.items():

                presence = self.agent.presence.get_contact_presence(jid)

                current_presence = presence.type
                status_presence = ast.literal_eval(presence.status) #Castear a tupla

                if not self.agent._is_available(current_presence):
                    continue

                pos, clients = status_presence
                dist = distance_in_meters(pos, origin)

                candidates.append((dist, clients, str(jid), pos))

            if not candidates:
                logger.warning("Agent[{}]: no available transports. Falling back to broadcast.".format(self.agent.name))
                return

            # Orden: primero menor distancia, luego menor nº de clientes
            #candidates.sort(key=lambda t: (t[0], t[1]))

            # Orden: primero menor nº de clientes, luego menor distancia
            candidates.sort(key=lambda t: (t[1], t[0]))

            # Orden: primero menor distancia solo
            #candidates.sort(key=lambda t: t[0])

            # Orden: primero menor nº de clientes
            #candidates.sort(key=lambda t: t[1])

            best_dist, best_clients, best_jid, best_pos = candidates[0]

            logger.info(
                "Agent [{}]: Candidate {}".format(self.agent.name, candidates[0])
            )

            msg.to = str(best_jid)
            await self.send(msg)


class NearAndFewerCustomersRequestBehaviour(LogisticFleetManagerStrategyBehaviour):
    """
    The default strategy for the FleetManager agent. By default it delegates all requests to all transports.
    """

    async def run(self):
        if not self.agent.registration:
            await self.send_registration()

        msg = await self.receive(timeout=5)

        logger.debug("Manager received message: {}".format(msg))
        if msg:

            content = json.loads(msg.body)

            if "origin" in content:
                origin = content["origin"]

            contacts = self.agent.presence.get_contacts()
            candidates = []

            for jid, contact in contacts.items():

                presence = self.agent.presence.get_contact_presence(jid)

                current_presence = presence.type
                status_presence = ast.literal_eval(presence.status) #Castear a tupla

                if not self.agent._is_available(current_presence):
                    continue

                pos, clients = status_presence
                dist = distance_in_meters(pos, origin)

                candidates.append((dist, clients, str(jid), pos))

            if not candidates:
                logger.warning("Agent[{}]: no available transports. Falling back to broadcast.".format(self.agent.name))
                return

            # Orden: primero menor distancia, luego menor nº de clientes
            candidates.sort(key=lambda t: (t[0], t[1]))

            # Orden: primero menor nº de clientes, luego menor distancia
            #candidates.sort(key=lambda t: (t[1], t[0]))

            # Orden: primero menor distancia solo
            #candidates.sort(key=lambda t: t[0])

            # Orden: primero menor nº de clientes
            #candidates.sort(key=lambda t: t[1])

            best_dist, best_clients, best_jid, best_pos = candidates[0]

            logger.info(
                "Agent [{}]: Candidate {}".format(self.agent.name, candidates[0])
            )

            msg.to = str(best_jid)
            await self.send(msg)