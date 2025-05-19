import asyncio
import json

from loguru import logger
from spade.message import Message

from simfleet.common.lib.transports.models.sharing import SharingStrategyBehaviour
from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_BOOKED, TRANSPORT_MOVING_TO_DESTINATION

from simfleet.communications.protocol import (
    REQUEST_PERFORMATIVE,
    PROPOSE_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    INFORM_PERFORMATIVE,
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination,
    distance_in_meters
)

################################################################
#                                                              #
#                     Transport Strategy                       #
#                                                              #
################################################################
class SharingWaitingState(SharingStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING
        await self.agent.send_status_fleetmanager()
        logger.debug("{} in Transport Waiting State".format(self.agent.jid))
        logger.warning(f"Transport {self.agent.jid} is free again")

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING)
            return
        logger.debug("Transport {} received: {}".format(self.agent.jid, msg))
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")
        if performative == PROPOSE_PERFORMATIVE:
            await self.accept_customer(content["customer_id"])
            self.set_next_state(TRANSPORT_BOOKED)
            return
        else:
            self.set_next_state(TRANSPORT_WAITING)
            return


class SharingBookedState(SharingStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_BOOKED
        await self.agent.send_status_fleetmanager()
        logger.debug("{} in Transport Booked State".format(self.agent.jid))

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING)
            return
        logger.debug("Transport {} received: {}".format(self.agent.jid, msg.body))
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")
        # We can receive 3 types of messages
        # 1) Another booking request
        if performative == PROPOSE_PERFORMATIVE:
            await self.refuse_customer(content["customer_id"])
            self.set_next_state(TRANSPORT_BOOKED)
            return
        # 2) Customer cancels request
        elif performative == CANCEL_PERFORMATIVE:
            await self.deassign_customer()
            self.set_next_state(TRANSPORT_WAITING)
            return
        # 3) Customer informs that they are in my position
        elif performative == INFORM_PERFORMATIVE:
            try:
                await self.pick_up_customer(content["customer_id"], content["origin"], content["dest"])
                # CHECK APPROPRIATE UPDATE OF STATUS
                self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)
                return
            except PathRequestException:
                logger.error("Transport {} could not get a path to customer {}. Cancelling..."
                             .format(self.agent.name, content["customer_id"]))
                await self.refuse_customer(content["customer_id"])
                self.set_next_state(TRANSPORT_WAITING)
                return
            except Exception as e:
                logger.error("Unexpected error in transport {}: {}".format(self.agent.name, e))
                await self.refuse_customer(content["customer_id"])
                self.set_next_state(TRANSPORT_WAITING)
                return
        else:
            logger.debug("Transport {} received an unexpected message from {} with content {}"
                           .format(self.agent.name, msg.sender, content))
            self.set_next_state(TRANSPORT_BOOKED)
            return


class SharingMovingToDestinationState(SharingStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        #self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
        #await self.agent.send_status_fleetmanager()
        logger.debug("{} in Transport Moving To Destination State".format(self.agent.jid))

    # Blocks the strategy behaviour (not the Transport Agent) until the transport agent has
    # dropped the customer in its destination
    async def run(self):
        # Reset internal flag to False. coroutines calling
        # wait() will block until set() is called
        self.agent.customer_in_transport_event.clear()
        # Registers an observer callback to be run when the "customer_in_transport" is changed
        self.agent.watch_value("customer_in_transport", self.agent.customer_in_transport_callback)
        # block behaviour until another coroutine calls set()
        await self.agent.customer_in_transport_event.wait()
        return self.set_next_state(TRANSPORT_WAITING)


class SharingMovingToDestinationState2(SharingStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        #self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
        #await self.agent.send_status_fleetmanager()
        logger.debug("{} in Transport Moving To Destination State".format(self.agent.jid))

    # Blocks the strategy behaviour (not the Transport Agent) until the transport agent has
    # dropped the customer in its destination
    async def run(self):
        # Reset internal flag to False. coroutines calling
        # wait() will block until set() is called
        self.agent.customer_in_transport_event.clear()
        # Registers an observer callback to be run when the "customer_in_transport" is changed
        self.agent.watch_value("customer_in_transport", self.agent.customer_in_transport_callback)
        # block behaviour until another coroutine calls set()
        while not self.agent.customer_in_transport_event.is_set():
            # listen to messages and reject them
            msg = await self.receive(timeout=5)
            if msg:
                logger.debug("Transport {} received: {}".format(self.agent.jid, msg.body))
                content = json.loads(msg.body)
                performative = msg.get_metadata("performative")
                if performative == PROPOSE_PERFORMATIVE:
                    await self.refuse_customer(content["customer_id"])

        return self.set_next_state(TRANSPORT_WAITING)


class FSMSharingStrategyBehaviour(FSMSimfleetBehaviour):
    def setup(self):
        # Create states
        self.add_state(TRANSPORT_WAITING, SharingWaitingState(), initial=True)
        self.add_state(TRANSPORT_BOOKED, SharingBookedState())
        self.add_state(TRANSPORT_MOVING_TO_DESTINATION, SharingMovingToDestinationState2())

        # Create transitions
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING)  # waiting for messages
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_BOOKED)  # booking

        self.add_transition(TRANSPORT_BOOKED, TRANSPORT_BOOKED)  # waiting for customer to arrive and messages
        self.add_transition(TRANSPORT_BOOKED, TRANSPORT_WAITING)  # booking cancelled
        self.add_transition(TRANSPORT_BOOKED, TRANSPORT_MOVING_TO_DESTINATION)  # customer arrived, start movement

        self.add_transition(TRANSPORT_MOVING_TO_DESTINATION, TRANSPORT_WAITING)  # transport is free again
