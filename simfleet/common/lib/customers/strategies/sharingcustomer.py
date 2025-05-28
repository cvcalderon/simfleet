import asyncio
import json

from loguru import logger
from spade.message import Message

from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour
from simfleet.common.lib.customers.models.sharingcustomer import SharingCustomerStrategyBehaviour

from simfleet.communications.protocol import QUERY_PROTOCOL, INFORM_PERFORMATIVE, ACCEPT_PERFORMATIVE, REFUSE_PERFORMATIVE, CANCEL_PERFORMATIVE
from simfleet.utils.status import CUSTOMER_WAITING, CUSTOMER_WAITING_FOR_APPROVAL, CUSTOMER_MOVING_TO_TRANSPORT, CUSTOMER_IN_TRANSPORT, CUSTOMER_IN_DEST
from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination,
    distance_in_meters
)


################################################################
#                                                              #
#                       Customer Strategy                      #
#                                                              #
################################################################
class SharingCustomerWaitingState(SharingCustomerStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_WAITING
        logger.debug("{} in Customer Waiting State".format(self.agent.jid))
        await asyncio.sleep(1)

    async def run(self):
        # Get fleet managers
        #if self.agent.fleetmanagers is None:
        #    await self.send_get_managers(self.agent.fleet_type)

        # If the customer does not have a fleet manager assigned, get the list of fleet managers.
        if self.agent.get_fleetmanagers() is None:
            fleetmanager_list = await self.agent.get_list_agent_position(self.agent.fleet_type,
                                                                             self.agent.get_fleetmanagers())

            self.agent.set_fleetmanagers(fleetmanager_list)

            #Código antiguo
            #msg = await self.receive(timeout=30)
            #if msg:
            #    protocol = msg.get_metadata("protocol")
            #    if protocol == QUERY_PROTOCOL:
            #        performative = msg.get_metadata("performative")
            #        if performative == INFORM_PERFORMATIVE:
            #            self.agent.fleetmanagers = json.loads(msg.body)
            #            logger.debug("{} Get fleet managers {}".format(self.agent.name, self.agent.fleetmanagers))
            #            self.set_next_state(CUSTOMER_WAITING)
            #            return
            #        elif performative == CANCEL_PERFORMATIVE:
            #            logger.debug("Cancellation of request for {} information".format(self.agent.type_service))
            #            self.set_next_state(CUSTOMER_WAITING)
            #            return
        else:
            # Get list of available transports
            if self.agent.available_transports is None or len(self.agent.available_transports) < 1:
                await self.send_get_transports()

                msg = await self.receive(timeout=5)
                if not msg:
                    self.agent.available_transports = None
                    self.set_next_state(CUSTOMER_WAITING)
                    return
                logger.debug("Customer received message: {}".format(msg))
                try:
                    content = json.loads(msg.body)
                    # if the list of available transports is empty, the customer waits 5 seconds before asking for it again
                    if content == {}:
                        logger.debug(f"Customer {self.agent.name} received empty available transports list. It will "
                                       f" wait 5 seconds before asking again.")
                        await asyncio.sleep(5)
                        return self.set_next_state(CUSTOMER_WAITING)
                except TypeError:
                    content = {}
                performative = msg.get_metadata("performative")
                protocol = msg.get_metadata("protocol")
                if protocol == QUERY_PROTOCOL:
                    #Analizar porque después de utilizar un instance.join() el agente recibe el mensaje del director en este lugar
                    #Añadido un filtro para descartar dirección del director
                    if performative == INFORM_PERFORMATIVE and (msg.sender != self.agent.directory_id and msg.sender != None):
                        self.agent.available_transports = content
                        logger.debug("Customer {} got dict of available transports {}".format(self.agent.name,
                                                                                              self.agent.available_transports))
                        self.set_next_state(CUSTOMER_WAITING)
                        return
                    elif performative == CANCEL_PERFORMATIVE:
                        logger.info("Cancellation of request for stations information.")
                        self.set_next_state(CUSTOMER_WAITING)
                        return
                    else:
                        logger.warning("Customer {} received an unexpected message from {} with content {}"
                                       .format(self.agent.name, msg.sender, content))
                        self.set_next_state(CUSTOMER_WAITING)
                        return

            else:  # Send proposal

                closest_transport = self.agent.nearst_agent(self.agent.available_transports, self.agent.get_position())

                #transport_positions = []
                #for key in self.agent.available_transports.keys():
                #    dic = self.agent.available_transports.get(key)
                #    transport_positions.append((dic['jid'], dic['position']))
                #######################
                # for debugging purposes
                #for jid, pos in transport_positions:
                #    logger.debug("Transport {} is {} meters away from customer {}".format(jid,
                #        distance_in_meters(self.agent.get_position(), pos), self.agent.name))
                #######################
                #closest_transport = min(transport_positions,
                #                        key=lambda x: distance_in_meters(x[1], self.agent.get_position()))


                # If the customer is receiving the same closes transport over and over and it can't walk to it,
                # make it wait 5 seconds in between requests
                if self.agent.previous_closest_transport is not None and self.agent.previous_closest_transport == closest_transport:
                    await asyncio.sleep(5)
                self.agent.previous_closest_transport = closest_transport
                logger.debug("Closest transport: {}".format(closest_transport))
                transport_id = closest_transport[0]
                transport_position = closest_transport[1]
                # Check if the transport is close enough for the customer to walk to it
                if not self.agent.can_walk(transport_position):
                    closest_transport = None
                    logger.info(f"Customer {self.agent.name} cannot walk to their closest transport")
                # delete that transport from the available_transports list
                del self.agent.available_transports[transport_id]
                if closest_transport is not None:
                    await self.send_proposal(transport_id)  # maybe str(transport_id)
                    self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
                    return
                else:
                    logger.debug("Closest transport to customer {} was None".format(self.agent.name))
                    # self.agent.available_transports = []
                    self.set_next_state(CUSTOMER_WAITING)
                    return

        self.set_next_state(CUSTOMER_WAITING)
        return

class SharingCustomerWaitingForApprovalState(SharingCustomerStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_WAITING_FOR_APPROVAL
        logger.debug("{} in Customer Waiting for Approval State".format(self.agent.jid))

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
            return
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            try:
                logger.info("Customer {} booked transport {}".format(self.agent.name,
                                                                      content["transport_id"]))
                await self.go_to_transport(content["transport_id"], content["position"])
                self.agent.status = CUSTOMER_MOVING_TO_TRANSPORT
                self.set_next_state(CUSTOMER_MOVING_TO_TRANSPORT)
                return
            except PathRequestException:
                logger.warning("Customer {} could not get a path to customer {}. Cancelling..."
                             .format(self.agent.name, content["transport_id"]))
                await self.cancel_proposal(content["transport_id"])
                self.set_next_state(CUSTOMER_WAITING)
                return
            except Exception as e:
                logger.error("Unexpected error in customer {}: {}".format(self.agent.name, e))
                await self.cancel_proposal(content["transport_id"])
                self.set_next_state(CUSTOMER_WAITING)
                return

        elif performative == REFUSE_PERFORMATIVE:
            logger.info("Customer {} got refusal from transport".format(self.agent.name))
            self.set_next_state(CUSTOMER_WAITING)
            return

        else:
            self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
            return


class SharingCustomerMovingToTransportState(SharingCustomerStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_MOVING_TO_TRANSPORT
        logger.debug("{} in Customer Moving To Transport State".format(self.agent.jid))

    async def run(self):
        if self.agent.get("arrived_to_transport"):
            logger.warning("Customer {} is already in their transport place".format(self.agent.jid))
            return self.set_next_state(CUSTOMER_IN_TRANSPORT)
        self.agent.arrived_to_transport_event.clear()
        self.agent.watch_value("arrived_to_transport", self.agent.arrived_to_transport_callback)
        await self.agent.arrived_to_transport_event.wait()
        return self.set_next_state(CUSTOMER_IN_TRANSPORT)


class SharingCustomerInTransportState(SharingCustomerStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_TRANSPORT
        logger.debug("{} in Customer In Transport State".format(self.agent.jid))

    async def run(self):
        await self.inform_transport()
        # block strategy execution
        self.agent.arrived_to_destination_event.clear()
        self.agent.watch_value("arrived_to_destination", self.agent.arrived_to_destination_callback)
        await self.agent.arrived_to_destination_event.wait()
        return self.set_next_state(CUSTOMER_IN_DEST)


class SharingCustomerInDestState(SharingCustomerStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_DEST
        logger.debug("{} in Customer In Dest State".format(self.agent.jid))

    async def run(self):
        logger.info(f"Customer {self.agent.name} has reached their destination")
        return #self.set_next_state(CUSTOMER_IN_DEST)


class FSMSharingCustomerStrategyBehaviour(FSMSimfleetBehaviour):
    def setup(self):
        # Create states
        self.add_state(CUSTOMER_WAITING, SharingCustomerWaitingState(), initial=True)
        self.add_state(CUSTOMER_WAITING_FOR_APPROVAL, SharingCustomerWaitingForApprovalState())
        self.add_state(CUSTOMER_MOVING_TO_TRANSPORT, SharingCustomerMovingToTransportState())
        self.add_state(CUSTOMER_IN_TRANSPORT, SharingCustomerInTransportState())
        self.add_state(CUSTOMER_IN_DEST, SharingCustomerInDestState())

        # Create transitions
        self.add_transition(CUSTOMER_WAITING, CUSTOMER_WAITING)  # get list of transports
        self.add_transition(CUSTOMER_WAITING, CUSTOMER_WAITING_FOR_APPROVAL)  # send booking proposal

        self.add_transition(CUSTOMER_WAITING_FOR_APPROVAL, CUSTOMER_WAITING)  # booking is rejected
        self.add_transition(CUSTOMER_WAITING_FOR_APPROVAL,
                            CUSTOMER_WAITING_FOR_APPROVAL)  # waiting for approval message
        self.add_transition(CUSTOMER_WAITING_FOR_APPROVAL, CUSTOMER_MOVING_TO_TRANSPORT)  # booking accepted

        self.add_transition(CUSTOMER_MOVING_TO_TRANSPORT,
                            CUSTOMER_IN_TRANSPORT)  # arrived to transport, picked up by it

        self.add_transition(CUSTOMER_IN_TRANSPORT, CUSTOMER_IN_TRANSPORT)

        self.add_transition(CUSTOMER_IN_TRANSPORT, CUSTOMER_IN_DEST)  # arrived to destination

        self.add_transition(CUSTOMER_IN_DEST, CUSTOMER_IN_DEST)
