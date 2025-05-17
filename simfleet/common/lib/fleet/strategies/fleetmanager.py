import asyncio
import json
import time
from asyncio import Queue
from spade.presence import PresenceManager
from spade.message import Message
from loguru import logger

from simfleet.common.agents.fleetmanager import FleetManagerStrategyBehaviour

from simfleet.communications.protocol import REQUEST_PROTOCOL, REQUEST_PERFORMATIVE, QUERY_PROTOCOL, INFORM_PERFORMATIVE
from simfleet.utils.status import TRANSPORT_WAITING


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
        if not self.agent.registration:
            await self.send_registration()

        msg = await self.receive(timeout=5)

        logger.warning(
            "Agent[{}]: FleetManager has a queue of ({})".format(
                self.agent.name, self.mailbox_size()
            )
        )

        logger.debug("Manager received message: {}".format(msg))
        if msg:
            for transport in self.get_transport_agents().values():
                msg.to = str(transport["jid"])
                logger.debug(
                    "Manager sent request to transport {}".format(transport["name"])
                )
                await self.send(msg)


################################################################
#                                                              #
#                     FleetManager Strategy                    #
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
        if not self.agent.registration:
            await self.send_registration()

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
