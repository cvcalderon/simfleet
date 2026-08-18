import asyncio
import json

from loguru import logger
from spade.message import Message
from spade.behaviour import State

from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from simfleet.communications.protocol import QUERY_PROTOCOL, INFORM_PERFORMATIVE, ACCEPT_PERFORMATIVE, \
    REFUSE_PERFORMATIVE, CANCEL_PERFORMATIVE, REQUEST_PROTOCOL, REQUEST_PERFORMATIVE, PROPOSE_PERFORMATIVE
from simfleet.utils.status import CUSTOMER_WAITING, CUSTOMER_WAITING_FOR_APPROVAL, CUSTOMER_MOVING_TO_TRANSPORT, \
    CUSTOMER_IN_TRANSPORT, CUSTOMER_IN_DEST, CUSTOMER_IN_STATION, CUSTOMER_MOVING_TO_DEST
from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination,
    distance_in_meters
)


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class SharingCustomerStrategyBehaviour(State):

    async def on_start(self):
        """
        Initializes the logger and timers. Call to parent method if overloaded.
        """
        logger.debug("Strategy {} started in customer {}".format(type(self).__name__, self.agent.name))

    async def go_to_transport(self):
        transport_id = (
            self.agent.get_current_transport_id()
        )

        transport_position = (
            self.agent.get_current_transport_position()
        )

        if (
            transport_id is None
            or transport_position is None
        ):
            logger.warning(
                "Customer [{}]: No current transport "
                "available to walk to.".format(
                    self.agent.name
                )
            )
            return

        logger.info(
            "Customer [{}]: Walking to sharing transport [{}].".format(
                self.agent.name,
                transport_id
            )
        )

        await self.agent.move_to(
            transport_position
        )

    async def request_transport_candidates(
        self,
        fleetmanager_id
    ):
        content = {
            "request_type": "sharing_candidates",
            "customer_id": str(self.agent.jid),
            "origin": self.agent.get_position()
        }

        if self.agent.max_walking_dist is not None:
            content["max_walking_distance"] = (
                self.agent.max_walking_dist
            )

        msg = Message()

        msg.to = str(fleetmanager_id)

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            REQUEST_PERFORMATIVE
        )

        msg.body = json.dumps(content)

        logger.debug(
            "Customer [{}]: Requesting sharing candidates "
            "from fleet manager [{}].".format(
                self.agent.name,
                fleetmanager_id
            )
        )

        await self.send(msg)

    def select_transport(self):
        candidates = (
            self.agent.get_transport_candidates()
        )

        if not candidates:
            return None

        return min(
            candidates,
            key=lambda candidate: candidate["distance"]
        )

    async def request_transport_booking(
        self,
        transport
    ):
        transport_id = transport.get("jid")

        if transport_id is None:
            logger.warning(
                "Customer [{}]: Cannot book transport "
                "without a jid.".format(
                    self.agent.name
                )
            )
            return

        content = {
            "customer_id": str(self.agent.jid),
            "dest": self.agent.customer_dest
        }

        msg = Message()

        msg.to = str(transport_id)

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            PROPOSE_PERFORMATIVE
        )

        msg.body = json.dumps(content)

        logger.info(
            "Customer [{}]: Requesting booking "
            "of sharing transport [{}].".format(
                self.agent.name,
                transport_id
            )
        )

        await self.send(msg)


    async def request_a_transport(self, content):
        """
            Request a transport to sharing-station

            Args:
                content (dict, optional): Information needed for registration.
        """
        if content is None:
            content = {}
        msg = Message()
        msg.to = self.agent.current_station[0]
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", REQUEST_PERFORMATIVE)
        msg.body = json.dumps(content)
        logger.debug("Customer {} asked to register to stop {} with destination {}".format(self.agent.name,
                                                                                           self.agent.current_station[0],
                                                                                           self.agent.destination_station[
                                                                                               1]))
        await self.send(msg)


    async def request_a_place_for_transport(self, content):
        """
            Request a transport to sharing-station

            Args:
                content (dict, optional): Information needed for registration.
        """
        if content is None:
            content = {}
        msg = Message()
        msg.to = self.agent.destination_station[0]
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        msg.body = json.dumps(content)
        logger.debug("Customer {} asked to register transport {}".format(self.agent.name,
                                                                         self.agent.destination_station[0]))
        await self.send(msg)


    async def cancel_transport_booking(
        self,
        transport_id
    ):
        if transport_id is None:
            return

        msg = Message()

        msg.to = str(transport_id)

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            CANCEL_PERFORMATIVE
        )

        msg.body = json.dumps(
            {
                "customer_id": str(self.agent.jid)
            }
        )

        logger.info(
            "Customer [{}]: Cancelling booking "
            "with sharing transport [{}].".format(
                self.agent.name,
                transport_id
            )
        )

        await self.send(msg)

    async def inform_transport_arrival(self):
        transport_id = (
            self.agent.get_current_transport_id()
        )

        if transport_id is None:
            logger.warning(
                "Customer [{}]: Cannot inform arrival "
                "without a current transport.".format(
                    self.agent.name
                )
            )
            return

        msg = Message()

        msg.to = str(transport_id)

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            INFORM_PERFORMATIVE
        )

        msg.body = json.dumps(
            {
                "customer_id": str(self.agent.jid)
            }
        )

        logger.info(
            "Customer [{}]: Arrived at sharing transport [{}].".format(
                self.agent.name,
                transport_id
            )
        )

        await self.send(msg)

    async def run(self):
        raise NotImplementedError

# ==================================================================
# -------------------------End Behaviour----------------------------
# ==================================================================



################################################################
#                                                              #
#                       Customer Strategy                      #
#                                                              #
################################################################
class SharingCustomerWaitingState(SharingCustomerStrategyBehaviour):
    """
    Represents the state where the sharing customer searches for
    available free-floating transports and selects a candidate.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = CUSTOMER_WAITING

        logger.debug(
            "Agent[{}]: The sharing customer is waiting.".format(
                self.agent.name
            )
        )

    async def run(self):

        # Discover the FleetManager if it is not known yet.
        if not self.agent.get_fleetmanagers():

            logger.info(
                "Agent[{}]: Looking for sharing fleet managers.".format(
                    self.agent.name
                )
            )

            fleetmanagers = await self.agent.get_list_agent_position(
                self.agent.fleet_type,
                self.agent.get_fleetmanagers()
            )

            self.agent.set_fleetmanagers(
                fleetmanagers
            )

            if not fleetmanagers:
                logger.warning(
                    "Agent[{}]: No sharing fleet managers available.".format(
                        self.agent.name
                    )
                )

                await self.agent.sleep(5)

            self.set_next_state(
                CUSTOMER_WAITING
            )
            return

        # Request a new list only when there are no local candidates.
        if not self.agent.get_transport_candidates():

            fleetmanagers = (
                self.agent.get_fleetmanagers()
                or {}
            )

            for fleetmanager_id in fleetmanagers.keys():

                await self.request_transport_candidates(
                    fleetmanager_id
                )

            candidates = {}

            # A response is expected from each FleetManager.
            for _ in range(len(fleetmanagers)):

                msg = await self.receive(timeout=5)

                if not msg:
                    break

                protocol = msg.get_metadata(
                    "protocol"
                )

                performative = msg.get_metadata(
                    "performative"
                )

                if (
                    protocol != REQUEST_PROTOCOL
                    or performative != INFORM_PERFORMATIVE
                ):
                    continue

                try:
                    content = json.loads(
                        msg.body
                    )

                except (json.JSONDecodeError, TypeError):
                    logger.warning(
                        "Agent[{}]: Invalid sharing candidates response.".format(
                            self.agent.name
                        )
                    )
                    continue

                if (
                    content.get("request_type")
                    != "sharing_candidates"
                ):
                    continue

                vehicles = content.get(
                    "vehicles",
                    []
                )

                if not isinstance(vehicles, list):
                    continue

                for vehicle in vehicles:

                    if not isinstance(vehicle, dict):
                        continue

                    vehicle_id = vehicle.get(
                        "jid"
                    )

                    position = vehicle.get(
                        "position"
                    )

                    if (
                        vehicle_id is None
                        or position is None
                    ):
                        continue

                    candidates[
                        str(vehicle_id)
                    ] = vehicle

            self.agent.set_transport_candidates(
                list(candidates.values())
            )

            if not self.agent.get_transport_candidates():

                logger.info(
                    "Agent[{}]: No sharing transports available.".format(
                        self.agent.name
                    )
                )

                await self.agent.sleep(5)

                self.set_next_state(
                    CUSTOMER_WAITING
                )
                return

        # The FleetManager performs discovery, but the customer
        # keeps control over its own walking constraint.
        valid_candidates = []

        for candidate in self.agent.get_transport_candidates():

            position = candidate.get(
                "position"
            )

            if position is None:
                continue

            if self.agent.can_walk(
                position
            ):
                valid_candidates.append(
                    candidate
                )

        self.agent.set_transport_candidates(
            valid_candidates
        )

        if not valid_candidates:

            logger.info(
                "Agent[{}]: No sharing transport is within "
                "walking distance.".format(
                    self.agent.name
                )
            )

            await self.agent.sleep(5)

            self.set_next_state(
                CUSTOMER_WAITING
            )
            return

        # The decision belongs to the customer strategy.
        selected_transport = (
            self.select_transport()
        )

        if selected_transport is None:

            self.agent.clear_transport_candidates()

            self.set_next_state(
                CUSTOMER_WAITING
            )
            return

        transport_id = selected_transport.get(
            "jid"
        )

        if transport_id is None:

            self.agent.clear_transport_candidates()

            self.set_next_state(
                CUSTOMER_WAITING
            )
            return

        logger.info(
            "Agent[{}]: Selected sharing transport [{}].".format(
                self.agent.name,
                transport_id
            )
        )

        self.agent.set_pending_transport(
            selected_transport
        )

        await self.request_transport_booking(
            selected_transport
        )

        self.agent.status = (
            CUSTOMER_WAITING_FOR_APPROVAL
        )

        self.set_next_state(
            CUSTOMER_WAITING_FOR_APPROVAL
        )
        return

class SharingCustomerWaitingForApprovalState(
    SharingCustomerStrategyBehaviour
):
    """
    Represents the state where the customer waits for the
    selected sharing transport to accept or refuse the booking.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            CUSTOMER_WAITING_FOR_APPROVAL
        )

        logger.debug(
            "Agent[{}]: Waiting for sharing transport "
            "booking approval.".format(
                self.agent.name
            )
        )

    async def run(self):

        pending_transport = (
            self.agent.get_pending_transport()
        )

        if pending_transport is None:

            logger.warning(
                "Agent[{}]: Waiting for approval without "
                "a pending transport.".format(
                    self.agent.name
                )
            )

            self.agent.status = CUSTOMER_WAITING

            self.set_next_state(
                CUSTOMER_WAITING
            )
            return

        pending_transport_id = (
            self.agent.get_pending_transport_id()
        )

        msg = await self.receive(timeout=60)

        # The booking request is still pending.
        if not msg:
            self.set_next_state(
                CUSTOMER_WAITING_FOR_APPROVAL
            )
            return

        protocol = msg.get_metadata(
            "protocol"
        )

        if protocol != REQUEST_PROTOCOL:
            self.set_next_state(
                CUSTOMER_WAITING_FOR_APPROVAL
            )
            return

        # Ignore responses from transports other than the one
        # whose booking is currently pending.
        if str(msg.sender) != str(pending_transport_id):

            logger.debug(
                "Agent[{}]: Ignoring booking response "
                "from transport [{}].".format(
                    self.agent.name,
                    msg.sender
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_FOR_APPROVAL
            )
            return

        try:
            content = json.loads(
                msg.body
            )

        except (json.JSONDecodeError, TypeError):

            logger.warning(
                "Agent[{}]: Invalid booking response "
                "from transport [{}].".format(
                    self.agent.name,
                    pending_transport_id
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_FOR_APPROVAL
            )
            return

        performative = msg.get_metadata(
            "performative"
        )

        if performative == ACCEPT_PERFORMATIVE:

            logger.info(
                "Agent[{}]: Sharing transport [{}] "
                "accepted the booking.".format(
                    self.agent.name,
                    pending_transport_id
                )
            )

            transport = dict(
                pending_transport
            )

            # The position returned by the transport is the
            # most recent known position.
            position = content.get(
                "position"
            )

            if position is not None:
                transport["position"] = position

            self.agent.set_current_transport(
                transport
            )

            self.agent.clear_pending_transport()

            try:

                await self.go_to_transport()

            except AlreadyInDestination:

                logger.info(
                    "Agent[{}]: Customer is already at "
                    "sharing transport [{}].".format(
                        self.agent.name,
                        pending_transport_id
                    )
                )

                self.agent.clear_transport_candidates()

                await self.inform_transport_arrival()

                self.agent.status = (
                    CUSTOMER_IN_TRANSPORT
                )

                self.set_next_state(
                    CUSTOMER_IN_TRANSPORT
                )
                return

            except PathRequestException:

                logger.warning(
                    "Agent[{}]: Could not get a walking path "
                    "to sharing transport [{}].".format(
                        self.agent.name,
                        pending_transport_id
                    )
                )

                await self.cancel_transport_booking(
                    pending_transport_id
                )

                self.agent.remove_transport_candidate(
                    pending_transport_id
                )

                self.agent.clear_current_transport()

                self.agent.status = CUSTOMER_WAITING

                self.set_next_state(
                    CUSTOMER_WAITING
                )
                return

            except Exception as e:

                logger.error(
                    "Unexpected error in sharing customer [{}]: {}".format(
                        self.agent.name,
                        e
                    )
                )

                await self.cancel_transport_booking(
                    pending_transport_id
                )

                self.agent.remove_transport_candidate(
                    pending_transport_id
                )

                self.agent.clear_current_transport()

                self.agent.status = CUSTOMER_WAITING

                self.set_next_state(
                    CUSTOMER_WAITING
                )
                return

            # The booking is confirmed and a valid walking route
            # to the sharing transport has been started.
            self.agent.clear_transport_candidates()

            self.agent.status = (
                CUSTOMER_MOVING_TO_TRANSPORT
            )

            self.set_next_state(
                CUSTOMER_MOVING_TO_TRANSPORT
            )
            return

        elif performative == REFUSE_PERFORMATIVE:

            logger.info(
                "Agent[{}]: Sharing transport [{}] "
                "refused the booking.".format(
                    self.agent.name,
                    pending_transport_id
                )
            )

            self.agent.remove_transport_candidate(
                pending_transport_id
            )

            self.agent.clear_pending_transport()

            self.agent.status = CUSTOMER_WAITING

            self.set_next_state(
                CUSTOMER_WAITING
            )
            return

        self.set_next_state(
            CUSTOMER_WAITING_FOR_APPROVAL
        )
        return


class SharingCustomerMovingToTransportState(
    SharingCustomerStrategyBehaviour
):
    """
    Represents the state where the customer is walking
    towards the reserved sharing transport.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            CUSTOMER_MOVING_TO_TRANSPORT
        )

        logger.debug(
            "Agent[{}]: The sharing customer is moving "
            "to the reserved transport.".format(
                self.agent.name
            )
        )

    async def run(self):

        transport_id = (
            self.agent.get_current_transport_id()
        )

        if transport_id is None:

            logger.warning(
                "Agent[{}]: Moving to transport without "
                "a current transport.".format(
                    self.agent.name
                )
            )

            self.agent.status = CUSTOMER_WAITING

            self.set_next_state(
                CUSTOMER_WAITING
            )
            return

        if not self.agent.is_in_destination():

            self.set_next_state(
                CUSTOMER_MOVING_TO_TRANSPORT
            )

            await self.agent.sleep(1)
            return

        logger.info(
            "Agent[{}]: Customer reached sharing "
            "transport [{}].".format(
                self.agent.name,
                transport_id
            )
        )

        await self.inform_transport_arrival()

        self.agent.status = CUSTOMER_IN_TRANSPORT

        self.set_next_state(
            CUSTOMER_IN_TRANSPORT
        )
        return


class SharingCustomerInTransportState(
    SharingCustomerStrategyBehaviour
):
    """
    Represents the state where the customer is using
    the reserved sharing transport.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = CUSTOMER_IN_TRANSPORT

        logger.debug(
            "Agent[{}]: The sharing customer is in "
            "the transport.".format(
                self.agent.name
            )
        )

    async def run(self):

        transport_id = (
            self.agent.get_current_transport_id()
        )

        if transport_id is None:

            logger.warning(
                "Agent[{}]: Customer is in transport "
                "without a current transport.".format(
                    self.agent.name
                )
            )

            self.agent.status = CUSTOMER_WAITING

            self.set_next_state(
                CUSTOMER_WAITING
            )
            return

        msg = await self.receive(timeout=60)

        if not msg:
            self.set_next_state(
                CUSTOMER_IN_TRANSPORT
            )
            return

        protocol = msg.get_metadata(
            "protocol"
        )

        performative = msg.get_metadata(
            "performative"
        )

        if (
            protocol != REQUEST_PROTOCOL
            or performative != INFORM_PERFORMATIVE
        ):
            self.set_next_state(
                CUSTOMER_IN_TRANSPORT
            )
            return

        if str(msg.sender) != str(transport_id):

            logger.debug(
                "Agent[{}]: Ignoring transport message "
                "from [{}].".format(
                    self.agent.name,
                    msg.sender
                )
            )

            self.set_next_state(
                CUSTOMER_IN_TRANSPORT
            )
            return

        try:
            content = json.loads(
                msg.body
            )

        except (json.JSONDecodeError, TypeError):

            logger.warning(
                "Agent[{}]: Invalid message received "
                "from sharing transport [{}].".format(
                    self.agent.name,
                    transport_id
                )
            )

            self.set_next_state(
                CUSTOMER_IN_TRANSPORT
            )
            return

        status = content.get(
            "status"
        )

        if status == CUSTOMER_IN_DEST:

            logger.info(
                "Agent[{}]: Customer reached the destination "
                "using sharing transport [{}].".format(
                    self.agent.name,
                    transport_id
                )
            )

            self.agent.clear_current_transport()

            self.agent.status = CUSTOMER_IN_DEST

            self.set_next_state(
                CUSTOMER_IN_DEST
            )
            return

        self.set_next_state(
            CUSTOMER_IN_TRANSPORT
        )
        return


class SharingCustomerInDestState(
    SharingCustomerStrategyBehaviour
):
    """
    Represents the state where the sharing customer
    has reached the final destination.
    """

    async def on_start(self):
        await super().on_start()

        self.agent.status = CUSTOMER_IN_DEST

        self.agent.clear_pending_transport()
        self.agent.clear_current_transport()
        self.agent.clear_transport_candidates()

        logger.debug(
            "Agent[{}]: The sharing customer is "
            "at the destination.".format(
                self.agent.name
            )
        )

    async def run(self):

        logger.info(
            "Customer {} has reached their destination.".format(
                self.agent.name
            )
        )

        return


class FSMSharingCustomerStrategyBehaviour(
    FSMSimfleetBehaviour
):
    """
    Finite State Machine behaviour for a free-floating
    sharing customer.
    """

    def setup(self):

        # States
        self.add_state(
            CUSTOMER_WAITING,
            SharingCustomerWaitingState(),
            initial=True
        )

        self.add_state(
            CUSTOMER_WAITING_FOR_APPROVAL,
            SharingCustomerWaitingForApprovalState()
        )

        self.add_state(
            CUSTOMER_MOVING_TO_TRANSPORT,
            SharingCustomerMovingToTransportState()
        )

        self.add_state(
            CUSTOMER_IN_TRANSPORT,
            SharingCustomerInTransportState()
        )

        self.add_state(
            CUSTOMER_IN_DEST,
            SharingCustomerInDestState()
        )

        # Waiting
        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING_FOR_APPROVAL
        )

        # Waiting for booking approval
        self.add_transition(
            CUSTOMER_WAITING_FOR_APPROVAL,
            CUSTOMER_WAITING_FOR_APPROVAL
        )

        self.add_transition(
            CUSTOMER_WAITING_FOR_APPROVAL,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_WAITING_FOR_APPROVAL,
            CUSTOMER_MOVING_TO_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_WAITING_FOR_APPROVAL,
            CUSTOMER_IN_TRANSPORT
        )

        # Moving to reserved transport
        self.add_transition(
            CUSTOMER_MOVING_TO_TRANSPORT,
            CUSTOMER_MOVING_TO_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_TRANSPORT,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_TRANSPORT,
            CUSTOMER_IN_TRANSPORT
        )

        # In transport
        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_IN_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_IN_DEST
        )





################################################################
#                                                              #
#                       Customer Strategy                      #
#                                                              #
################################################################
class SharingStationCustomerWaitingState(SharingCustomerStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_WAITING
        logger.debug("{} in Customer Waiting State".format(self.agent.jid))
        #await asyncio.sleep(1)

    async def run(self):

        """
           Manages the movement of the customer to the bus stop.
        """

        if self.agent.station_dic is None:
            # Obtain the list of sharing-station
            self.agent.station_dic = await self.agent.get_list_agent_position(self.agent.type_service, self.agent.station_dic)

            self.set_next_state(CUSTOMER_WAITING)
            return
        else:

            self.agent.setup_stations()

            # Si el agente no puede caminar la distancia hacia la estación continua en bucle
            if self.agent.current_station == None:
                self.set_next_state(CUSTOMER_WAITING)

                return

            logger.debug("Closest station: {}".format(self.agent.current_station))
            station_id = self.agent.current_station[0]
            station_position = self.agent.current_station[1]

            if station_position != self.agent.get("current_pos"):
                self.agent.pedestrian_dest = station_position

                # Check if the transport is close enough for the customer to walk to it
                if not self.agent.can_walk(station_position):
                    #closest_transport = None
                    self.agent.current_station = None
                    logger.info(f"Customer {self.agent.name} cannot walk to their closest transport")
                # delete that transport from the available_transports list
                #del self.agent.available_transports[transport_id]
                if self.agent.current_station is not None:
                    #await self.send_proposal(station_id)  # maybe str(transport_id)
                    #self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
                    #return

                    logger.info(
                        "Agent {} on route to destination {}".format(self.agent.name, station_position)
                    )

                    try:
                        logger.debug("{} move_to destination {}".format(self.agent.name, station_position))

                        await self.agent.move_to(station_position)
                        #self.set_next_state(CUSTOMER_MOVING_TO_DEST)
                        self.set_next_state(CUSTOMER_MOVING_TO_DEST)
                        return

                    except AlreadyInDestination:
                        logger.debug(
                            "{} is already in the destination' {} position. . .".format(
                                self.agent.name, station_position
                            )
                        )
                        #self.set_next_state(CUSTOMER_WAITING_TO_MOVE)
                        self.agent.arrived_to_transport()

                        # Testear las 2 líneas
                        content = {"service_name": self.agent.type_service, "object_type": "customer"}
                        await self.request_a_transport(content)

                        self.set_next_state(CUSTOMER_IN_STATION)
                        return

                else:
                    logger.debug("Closest transport to customer {} was None".format(self.agent.name))
                    # self.agent.available_transports = []
                    self.set_next_state(CUSTOMER_WAITING)
                    return

            else:
                self.set_next_state(CUSTOMER_IN_STATION)
                return


class SharingStationCustomerMovingToDestState(SharingCustomerStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_MOVING_TO_DEST
        logger.debug("{} in Customer Moving To Transport State".format(self.agent.jid))

    async def run(self):
        if self.agent.get("arrived_to_transport"):
            logger.warning("Customer {} is already in their transport place".format(self.agent.jid))

            #Testear las 2 líneas
            #content = {"service_name": self.agent.type_service, "object_type": "customer"}
            #await self.request_a_transport(content)

            if not self.agent.get_position() == self.agent.customer_dest:
                content = {"service_name": self.agent.type_service, "object_type": "customer"}
                await self.request_a_transport(content)
                return self.set_next_state(CUSTOMER_IN_STATION)
            else:
                return self.set_next_state(CUSTOMER_IN_DEST)

            #return self.set_next_state(CUSTOMER_IN_STATION)
        self.agent.arrived_to_transport_event.clear()
        self.agent.watch_value("arrived_to_transport", self.agent.arrived_to_transport_callback)
        await self.agent.arrived_to_transport_event.wait()

        if not self.agent.get_position() == self.agent.customer_dest:
            content = {"service_name": self.agent.type_service, "object_type": "customer"}
            await self.request_a_transport(content)
            return self.set_next_state(CUSTOMER_IN_STATION)
        else:
            return self.set_next_state(CUSTOMER_IN_DEST)

        #Testear las dos líneas
        #content = {"service_name": self.agent.type_service, "object_type": "customer"}
        #await self.request_a_transport(content)

        #return self.set_next_state(CUSTOMER_IN_STATION)


class SharingStationCustomerInStationState(SharingCustomerStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_STATION
        logger.debug("{} in Customer in Station State".format(self.agent.jid))

    async def run(self):

        # Send registration petition to the bus stop
        #self.agent.arguments["jid"] = str(self.agent.jid)
        #self.agent.arguments["destination_stop"] = self.agent.destination_stop[1]

        #content = {"service_name": self.agent.type_service, "object_type": "customer"}
        #await self.request_a_transport(content)

        #await self.register_to_stop(content)
        # Wait for registration acceptance
        msg = await self.receive(timeout=30)

        if msg:
            sender = str(msg.sender)
            performative = msg.get_metadata("performative")
            protocol = msg.get_metadata("protocol")
            content = json.loads(msg.body)

            logger.warning("DEBUG: Customer {} msg - {}".format(self.agent.name, msg))

            if performative == ACCEPT_PERFORMATIVE and protocol == REQUEST_PROTOCOL:
                #self.agent.registered_in = sender
                logger.info("Customer {} registered in bus stop {}".format(self.agent.name, sender))
                #self.set_next_state(CUSTOMER_WAITING_FOR_APPROVAL)
                self.set_next_state(CUSTOMER_IN_STATION)
                return
            elif performative == INFORM_PERFORMATIVE and protocol == REQUEST_PROTOCOL:

                station_origin = self.agent.current_station[1]
                station_dest = self.agent.destination_station[1]
                transport_id = content["transport_id"]

                content = {"customer_id": str(self.agent.jid), "origin": station_origin, "dest": station_dest}
                await self.send_proposal(transport_id, content)
                self.agent.set("current_transport", transport_id)
                #logger.info("Customer {} registered in bus stop {}".format(self.agent.name, sender))
                self.set_next_state(CUSTOMER_IN_TRANSPORT)
                return

            elif performative == REFUSE_PERFORMATIVE:
                # Entraría en bucle si no hay transportes disponibles en la estación origen
                logger.warning("Station {} has not transport for {}".format(sender, self.agent.name))
                self.set_next_state(CUSTOMER_IN_STATION)
                return
            else:
                self.set_next_state(CUSTOMER_IN_STATION)
        else:
            self.set_next_state(CUSTOMER_IN_STATION)
            return



class SharingStationCustomerInTransportState(SharingCustomerStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_TRANSPORT
        logger.debug("{} in Customer In Transport State".format(self.agent.jid))

    async def run(self):
        #await self.inform_transport()
        # block strategy execution
        self.agent.arrived_to_destination_event.clear()
        self.agent.watch_value("arrived_to_destination", self.agent.arrived_to_destination_callback)
        await self.agent.arrived_to_destination_event.wait()

        content = {"service_name": self.agent.type_service}
        await self.request_a_place_for_transport(content)

        return self.set_next_state(CUSTOMER_IN_DEST)


class SharingStationCustomerInDestState(SharingCustomerStrategyBehaviour):

    async def on_start(self):
        await super().on_start()
        self.agent.status = CUSTOMER_IN_DEST
        logger.debug("{} in Customer In Dest State".format(self.agent.jid))

    async def run(self):
        try:
            # wait for the transport to inform the customer that the destination station has been reached
            msg = await self.receive(timeout=10)
            if msg:

                sender = msg.sender
                performative = msg.get_metadata("performative")
                content = json.loads(msg.body)

                if performative == INFORM_PERFORMATIVE:
                    logger.info("Customer {} has reached their destination".format(self.agent.name))

                    if "available_place" in content:
                        available_place = content["available_place"]
                        content = {"available_place": available_place, "station": str(sender)}
                        await self.inform_transport(content)

                    if self.agent.destination_station[1] != self.agent.customer_dest:

                        self.agent.pedestrian_dest = self.agent.customer_dest

                        logger.info(
                            "Agent {} on route to destination {}".format(self.agent.name, self.agent.customer_dest)
                        )

                        try:
                            logger.debug("{} move_to destination {}".format(self.agent.name, self.agent.customer_dest))

                            self.set("arrived_to_transport", False)

                            await self.agent.move_to(self.agent.customer_dest)
                            self.set_next_state(CUSTOMER_MOVING_TO_DEST)
                            return
                        except AlreadyInDestination:
                            logger.debug(
                                "{} is already in the destination' {} position. . .".format(
                                    self.agent.name, self.agent.customer_dest
                                )
                            )
                            self.set_next_state(CUSTOMER_IN_DEST)
                            return
            self.set_next_state(CUSTOMER_IN_DEST)
            return
        except Exception as e:
            logger.critical("Agent {}, Exception {} in CustomerInDestState".format(self.agent.name, e))



class FSMSharingStationCustomerStrategyBehaviour(FSMSimfleetBehaviour):
    def setup(self):
        # Create states
        self.add_state(CUSTOMER_WAITING, SharingStationCustomerWaitingState(), initial=True)
        self.add_state(CUSTOMER_MOVING_TO_DEST, SharingStationCustomerMovingToDestState())
        self.add_state(CUSTOMER_IN_STATION, SharingStationCustomerInStationState())
        self.add_state(CUSTOMER_IN_TRANSPORT, SharingStationCustomerInTransportState())
        self.add_state(CUSTOMER_IN_DEST, SharingStationCustomerInDestState())

        # Create transitions
        self.add_transition(CUSTOMER_WAITING, CUSTOMER_WAITING)  # get list of transports
        self.add_transition(CUSTOMER_WAITING, CUSTOMER_MOVING_TO_DEST)  # send booking proposal
        self.add_transition(CUSTOMER_WAITING, CUSTOMER_IN_STATION)  # send booking proposal

        self.add_transition(CUSTOMER_MOVING_TO_DEST, CUSTOMER_IN_STATION)  # booking is rejected
        self.add_transition(CUSTOMER_MOVING_TO_DEST, CUSTOMER_IN_DEST)  # booking accepted

        self.add_transition(CUSTOMER_IN_STATION, CUSTOMER_IN_TRANSPORT)  # arrived to transport, picked up by it
        self.add_transition(CUSTOMER_IN_STATION, CUSTOMER_IN_STATION)

        self.add_transition(CUSTOMER_IN_TRANSPORT, CUSTOMER_IN_DEST)  # arrived to destination

        self.add_transition(CUSTOMER_IN_DEST, CUSTOMER_IN_DEST)
        self.add_transition(CUSTOMER_IN_DEST, CUSTOMER_MOVING_TO_DEST)
