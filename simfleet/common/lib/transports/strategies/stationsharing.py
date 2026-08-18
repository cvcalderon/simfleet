import asyncio
import json

from loguru import logger
from spade.message import Message
from spade.behaviour import State

from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_BOOKED, TRANSPORT_MOVING_TO_DESTINATION, \
    TRANSPORT_IN_DEST, TRANSPORT_IN_CUSTOMER_PLACE

from simfleet.communications.protocol import (
    REQUEST_PERFORMATIVE,
    PROPOSE_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    REQUEST_PROTOCOL,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination,
    distance_in_meters
)


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================


class SharingStrategyBehaviour(State):

    async def on_start(self):
        logger.debug(
            "Strategy {} started in transport {}".format(
                type(self).__name__,
                self.agent.name
            )
        )


    # async def send_status_fleetmanager(self):
    #     msg = Message()
    #     msg.to = str(self.agent.fleetmanager_id)
    #     msg.set_metadata("protocol", REQUEST_PROTOCOL)
    #     msg.set_metadata("performative", INFORM_PERFORMATIVE)
    #     msg.body = json.dumps({
    #         "name": self.agent.name,
    #         "jid": str(self.agent.jid),
    #         "status": self.agent.status,
    #         "position": self.agent.get_position()
    #     })
    #     await self.send(msg)

    async def pick_up_customer(self, customer_id, origin, dest):
        # Save customer attributes and travel destination
        #self.set("current_customer", customer_id)
        #self.agent.current_customer_orig = origin
        #self.agent.current_customer_dest = dest

        self.agent.add_customer_in_transport(
            customer_id=customer_id, origin=origin, dest=dest
        )

        if not self.agent.is_customer_in_transport():
            try:
                # try to pick up the customer and move towards its destination
                #self.set("customer_in_transport", self.get("current_customer"))
                self.set("customer_in_transport", customer_id)
                #await self.agent.move_to(self.agent.current_customer_dest)
                await self.agent.move_to(dest)
                #self.agent.num_assignments += 1
            except PathRequestException:
                # if there is no path to customer's destination, cancel it
                await self.cancel_customer()
                self.agent.status = TRANSPORT_WAITING
            #except AlreadyInDestination:
                # if the transport is already in the customer's destination, drop the customer off
                #logger.error("++++++++++ transport {} is already in customers destination {}".format(
                #    self.agent.name, self.agent.current_customer_dest))
            #    logger.error("++++++++++ transport {} is already in customers destination {}".format(
            #        self.agent.name, dest))
            #    await self.agent.drop_customer()
            else:
                # if there is no error moving to the destination,
                # inform the customer that it has been picked up
                #await self.agent.inform_customer(self.get("current_customer"), TRANSPORT_IN_CUSTOMER_PLACE)
                await self.inform_customer(customer_id, TRANSPORT_IN_CUSTOMER_PLACE)
                self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
                #logger.info("Transport {} has picked up the customer {}.".format(
                #    self.agent.agent_id, self.get("current_customer")))
                logger.info("Transport {} has picked up the customer {}.".format(
                    self.agent.agent_id, customer_id))

    #
    # async def pick_up_customer_in_station(self, customer_id, origin, dest):
    #     # Save customer attributes and travel destination
    #     #self.set("current_customer", customer_id)
    #     #self.agent.current_customer_orig = origin
    #     #self.agent.current_customer_dest = dest
    #
    #     self.agent.add_customer_in_transport(
    #         customer_id=customer_id, origin=origin, dest=dest
    #     )
    #
    #     if not self.agent.is_customer_in_transport():
    #         try:
    #             # try to pick up the customer and move towards its destination
    #             #self.set("customer_in_transport", self.get("current_customer"))
    #             self.set("customer_in_transport", customer_id)
    #             #await self.agent.move_to(self.agent.current_customer_dest)
    #             await self.agent.move_to(dest)
    #             #self.agent.num_assignments += 1
    #         except PathRequestException:
    #             # if there is no path to customer's destination, cancel it
    #             await self.cancel_customer()
    #             self.agent.set_registration(status=False)  # Registro esta a FALSE para que se registre nuevamente en la estación.
    #             self.agent.status = TRANSPORT_WAITING
    #         else:
    #             logger.info("Transport {} has picked up the customer {}.".format(
    #                 self.agent.agent_id, customer_id))

    async def accept_customer(self, customer_id):

        reply = Message()

        reply.to = str(customer_id)
        reply.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        reply.set_metadata(
            "performative",
            ACCEPT_PERFORMATIVE
        )

        reply.body = json.dumps(
            {
                "transport_id": str(self.agent.jid),
                "position": self.agent.get_position()
            }
        )

        await self.send(reply)

        logger.info(
            "Transport {} accepted booking from customer {}".format(
                self.agent.name,
                customer_id
            )
        )

    async def refuse_customer(self, customer_id):

        reply = Message()

        reply.to = str(customer_id)
        reply.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        reply.set_metadata(
            "performative",
            REFUSE_PERFORMATIVE
        )

        reply.body = json.dumps(
            {
                "transport_id": str(self.agent.jid),
                "position": self.agent.get_position()
            }
        )

        await self.send(reply)

        logger.info(
            "Transport {} refused booking from customer {}".format(
                self.agent.name,
                customer_id
            )
        )

    async def inform_customer(
        self,
        customer_id,
        status,
        data=None
    ):

        if data is None:
            data = {}

        msg = Message()

        msg.to = str(customer_id)
        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        msg.set_metadata(
            "performative",
            INFORM_PERFORMATIVE
        )

        data["status"] = status

        msg.body = json.dumps(data)

        await self.send(msg)

    async def cancel_customer(
        self,
        customer_id,
        data=None
    ):

        if data is None:
            data = {}

        msg = Message()

        msg.to = str(customer_id)
        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        msg.set_metadata(
            "performative",
            CANCEL_PERFORMATIVE
        )

        msg.body = json.dumps(data)

        await self.send(msg)

        logger.debug(
            "Agent[{}]: Cancelled booking with customer [{}].".format(
                self.agent.agent_id,
                customer_id
            )
        )
    #
    # async def inform_station(self, station_id, content):
    #     """
    #         Inform to station
    #
    #         Args:
    #             content (dict, optional): Information needed for registration.
    #     """
    #     if content is None:
    #         content = {}
    #     msg = Message()
    #     msg.to = station_id
    #     msg.set_metadata("protocol", REQUEST_PROTOCOL)
    #     msg.set_metadata("performative", INFORM_PERFORMATIVE)
    #     msg.body = json.dumps(content)
    #     await self.send(msg)

    async def deassign_customer(self):
        """
        Triggered when, by any reason, a customer cancels their already accepted booking
        """
        # Delete saved values (destination, etc.) belonging to booked customer
        self.agent.set("current_customer", None)
        self.agent.current_customer_orig = None
        self.agent.current_customer_dest = None

    async def run(self):
        raise NotImplementedError


# ==================================================================
# -------------------------End Behaviour----------------------------
# ==================================================================



################################################################
#                                                              #
#                  Station Transport Strategy                  #
#                                                              #
################################################################
class StationTransportWaitingState(SharingStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING
        #await self.agent.send_status_fleetmanager()
        logger.debug("{} in Transport Waiting State".format(self.agent.jid))
        #logger.warning(f"Transport {self.agent.jid} is free again")

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING)
            return
        logger.debug("Transport {} received: {}".format(self.agent.jid, msg))
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")
        #if performative == PROPOSE_PERFORMATIVE:
        #    await self.accept_customer(content["customer_id"])
        #    self.set_next_state(TRANSPORT_BOOKED)
        #    return
        #else:
        #    self.set_next_state(TRANSPORT_WAITING)
        #    return

        if performative == CANCEL_PERFORMATIVE:
            await self.deassign_customer()
            self.agent.set_registration(status=False)       #Registro esta a FALSE para que se registre nuevamente en la estación.
            self.set_next_state(TRANSPORT_WAITING)
            return
        # 3) Customer informs that they are in my position
        elif performative == PROPOSE_PERFORMATIVE:
            try:
                await self.pick_up_customer_in_station(content["customer_id"], content["origin"], content["dest"])
                # CHECK APPROPRIATE UPDATE OF STATUS
                self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)
                return
            except PathRequestException:
                logger.error("Transport {} could not get a path to customer {}. Cancelling..."
                             .format(self.agent.name, content["customer_id"]))
                await self.refuse_customer(content["customer_id"])
                self.agent.set_registration(status=False)  # Registro esta a FALSE para que se registre nuevamente en la estación.
                self.set_next_state(TRANSPORT_WAITING)
                return
            except Exception as e:
                logger.error("Unexpected error in transport {}: {}".format(self.agent.name, e))
                await self.refuse_customer(content["customer_id"])
                self.agent.set_registration(status=False)  # Registro esta a FALSE para que se registre nuevamente en la estación.
                self.set_next_state(TRANSPORT_WAITING)
                return
        else:
            logger.debug("Transport {} received an unexpected message from {} with content {}"
                           .format(self.agent.name, msg.sender, content))
            self.set_next_state(TRANSPORT_WAITING)
            return


class StationTransportMovingToDestinationState(SharingStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
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
        return self.set_next_state(TRANSPORT_IN_DEST)

class StationTransportInDestinationState(SharingStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_IN_DEST
        #await self.agent.send_status_fleetmanager()
        logger.debug("{} in Transport Moving To Destination State".format(self.agent.jid))

    # Blocks the strategy behaviour (not the Transport Agent) until the transport agent has
    # dropped the customer in its destination
    async def run(self):

        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_IN_DEST)
            return
        logger.debug("Transport {} received: {}".format(self.agent.jid, msg))
        sender = msg.sender
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")

        if performative == INFORM_PERFORMATIVE:

            available = content.get("available_place")

            logger.warning("DEBUG: Agent {} - available: {}".format(self.agent.jid, available))

            if available:
                station = content["station"]
                self.agent.fleetmanager_id = station
                content = {"service_name": self.agent.fleet_type, "register": True}
                await self.inform_station(station, content)

                logger.warning("DEBUG: Transport {} - Fleetmanager: {} - content: {}".format(self.agent.jid, self.agent.fleetmanager_id, content))

                self.set_next_state(TRANSPORT_WAITING)
                return
            else:
                self.set_next_state(TRANSPORT_IN_DEST)
                return

        else:
            logger.debug("Transport {} received an unexpected message from {} with content {}"
                         .format(self.agent.name, msg.sender, content))
            self.set_next_state(TRANSPORT_IN_DEST)
            return




class FSMStationTransportStrategyBehaviour(FSMSimfleetBehaviour):
    def setup(self):
        # Create states
        self.add_state(TRANSPORT_WAITING, StationTransportWaitingState(), initial=True)
        self.add_state(TRANSPORT_MOVING_TO_DESTINATION, StationTransportMovingToDestinationState())
        self.add_state(TRANSPORT_IN_DEST, StationTransportInDestinationState())

        # Create transitions
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING)  # waiting for messages
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_MOVING_TO_DESTINATION)  # booking

        self.add_transition(TRANSPORT_MOVING_TO_DESTINATION, TRANSPORT_IN_DEST)  # waiting for customer to arrive and messages
        self.add_transition(TRANSPORT_IN_DEST, TRANSPORT_IN_DEST)  # booking cancelled
        self.add_transition(TRANSPORT_IN_DEST, TRANSPORT_WAITING)  # customer arrived, start movement
