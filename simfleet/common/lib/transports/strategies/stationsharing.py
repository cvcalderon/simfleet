import json

from loguru import logger
from spade.behaviour import State
from spade.message import Message

from simfleet.communications.protocol import (
    ACCEPT_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    PROPOSE_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    REGISTER_PROTOCOL,
    REQUEST_PERFORMATIVE,
    REQUEST_PROTOCOL,
)

from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from simfleet.utils.helpers import (
    AlreadyInDestination,
    PathRequestException,
)

from simfleet.utils.status import (
    CUSTOMER_IN_DEST,
    CUSTOMER_IN_TRANSPORT,
    TRANSPORT_IN_DEST,
    TRANSPORT_MOVING_TO_DESTINATION,
    TRANSPORT_WAITING,
    TRANSPORT_WAITING_FOR_STATION_APPROVAL,
)


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================


class StationSharingStrategyBehaviour(State):

    async def on_start(self):
        logger.debug(
            "Strategy {} started in station-sharing transport [{}]".format(
                type(self).__name__,
                self.agent.name
            )
        )

    async def inform_customer(
        self,
        customer_id,
        performative,
        content
    ):
        msg = Message()

        msg.to = str(
            customer_id
        )

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            performative
        )

        msg.body = json.dumps(
            content
        )

        await self.send(
            msg
        )

    async def send_station_registration(
        self,
        station_id
    ):
        content = {
            "name": self.agent.name,
            "jid": str(self.agent.jid),
            "fleet_type": self.agent.fleet_type,
        }

        msg = Message()

        msg.to = str(
            station_id
        )

        msg.set_metadata(
            "protocol",
            REGISTER_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            REQUEST_PERFORMATIVE
        )

        msg.body = json.dumps(
            content
        )

        await self.send(
            msg
        )

        logger.debug(
            "Agent[{}]: Requested registration in station [{}].".format(
                self.agent.name,
                station_id
            )
        )

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
class StationSharingWaitingState(
    StationSharingStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_WAITING

        logger.debug(
            "{} in Station Sharing Waiting State".format(
                self.agent.jid
            )
        )

    async def run(self):

        msg = await self.receive(
            timeout=60
        )

        if not msg:
            self.set_next_state(
                TRANSPORT_WAITING
            )
            return

        if (
            msg.get_metadata("protocol")
            != REQUEST_PROTOCOL
        ):
            self.set_next_state(
                TRANSPORT_WAITING
            )
            return

        performative = msg.get_metadata(
            "performative"
        )

        if performative != PROPOSE_PERFORMATIVE:
            self.set_next_state(
                TRANSPORT_WAITING
            )
            return

        try:
            content = json.loads(
                msg.body
            )
        except (json.JSONDecodeError, TypeError):

            self.set_next_state(
                TRANSPORT_WAITING
            )
            return

        customer_id = content.get(
            "customer_id"
        )

        origin = content.get(
            "origin"
        )

        dest = content.get(
            "dest"
        )

        destination_station = content.get(
            "destination_station"
        )

        if (
            customer_id is None
            or dest is None
            or destination_station is None
        ):
            self.set_next_state(
                TRANSPORT_WAITING
            )
            return

        self.agent.set_origin_station(
            self.agent.get_registration_fleet()
        )

        self.agent.set_destination_station(
            destination_station
        )

        self.agent.add_customer_in_transport(
            customer_id=customer_id,
            origin=origin,
            dest=dest
        )

        self.agent.set_registration(
            False
        )

        try:
            await self.agent.move_to(
                dest
            )

        except AlreadyInDestination:

            await self.send_station_registration(
                destination_station
            )

            self.agent.status = (
                TRANSPORT_WAITING_FOR_STATION_APPROVAL
            )

            self.set_next_state(
                TRANSPORT_WAITING_FOR_STATION_APPROVAL
            )

            return

        except PathRequestException:

            logger.error(
                "Agent[{}]: Could not calculate path "
                "to destination station [{}].".format(
                    self.agent.name,
                    destination_station
                )
            )

            await self.inform_customer(
                customer_id,
                REFUSE_PERFORMATIVE,
                {
                    "reason": "path_unavailable",
                    "station": destination_station,
                }
            )

            self.agent.remove_customer_in_transport(
                customer_id
            )

            self.agent.status = TRANSPORT_IN_DEST

            self.set_next_state(
                TRANSPORT_IN_DEST
            )

            return

        await self.inform_customer(
            customer_id,
            INFORM_PERFORMATIVE,
            {
                "status": CUSTOMER_IN_TRANSPORT,
                "transport_id": str(self.agent.jid),
            }
        )

        self.agent.status = (
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.set_next_state(
            TRANSPORT_MOVING_TO_DESTINATION
        )

class StationSharingMovingToDestinationState(
    StationSharingStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            TRANSPORT_MOVING_TO_DESTINATION
        )

        logger.debug(
            "{} moving to destination station".format(
                self.agent.jid
            )
        )

    async def run(self):

        if not self.agent.is_in_destination():

            self.set_next_state(
                TRANSPORT_MOVING_TO_DESTINATION
            )

            await self.agent.sleep(
                1
            )

            return

        destination_station = (
            self.agent.get_destination_station()
        )

        if destination_station is None:

            logger.error(
                "Agent[{}]: Destination station is missing.".format(
                    self.agent.name
                )
            )

            self.agent.status = TRANSPORT_IN_DEST

            self.set_next_state(
                TRANSPORT_IN_DEST
            )

            return

        await self.send_station_registration(
            destination_station
        )

        self.agent.status = (
            TRANSPORT_WAITING_FOR_STATION_APPROVAL
        )

        self.set_next_state(
            TRANSPORT_WAITING_FOR_STATION_APPROVAL
        )

class StationSharingWaitingForStationApprovalState(
    StationSharingStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            TRANSPORT_WAITING_FOR_STATION_APPROVAL
        )

        logger.debug(
            "{} waiting for destination station approval".format(
                self.agent.jid
            )
        )

    async def run(self):

        destination_station = (
            self.agent.get_destination_station()
        )

        if destination_station is None:

            self.agent.status = TRANSPORT_IN_DEST

            self.set_next_state(
                TRANSPORT_IN_DEST
            )

            return

        msg = await self.receive(
            timeout=60
        )

        if not msg:

            self.set_next_state(
                TRANSPORT_WAITING_FOR_STATION_APPROVAL
            )

            return

        if (
            msg.get_metadata("protocol")
            != REGISTER_PROTOCOL
        ):

            self.set_next_state(
                TRANSPORT_WAITING_FOR_STATION_APPROVAL
            )

            return

        if not self.agent.is_same_jid(
            msg.sender,
            destination_station
        ):

            self.set_next_state(
                TRANSPORT_WAITING_FOR_STATION_APPROVAL
            )

            return

        performative = msg.get_metadata(
            "performative"
        )

        current_customers = (
            self.agent.get("current_customer")
        )

        if not current_customers:

            self.agent.status = TRANSPORT_IN_DEST

            self.set_next_state(
                TRANSPORT_IN_DEST
            )

            return

        customer_id = next(
            iter(current_customers)
        )

        if performative == ACCEPT_PERFORMATIVE:

            try:
                content = json.loads(
                    msg.body
                )
            except (json.JSONDecodeError, TypeError):
                content = None

            self.agent.configure_registration(
                destination_station,
                False
            )

            self.agent.set_registration(
                True,
                content
            )

            await self.inform_customer(
                customer_id,
                INFORM_PERFORMATIVE,
                {
                    "status": CUSTOMER_IN_DEST,
                    "station": destination_station,
                    "transport_id": str(self.agent.jid),
                }
            )

            self.agent.remove_customer_in_transport(
                customer_id
            )

            self.agent.increment_completed_assignments()

            self.agent.clear_origin_station()
            self.agent.clear_destination_station()

            self.agent.status = TRANSPORT_WAITING

            self.set_next_state(
                TRANSPORT_WAITING
            )

            logger.info(
                "Agent[{}]: Registered successfully in "
                "destination station [{}].".format(
                    self.agent.name,
                    destination_station
                )
            )

            return

        if performative == REFUSE_PERFORMATIVE:

            await self.inform_customer(
                customer_id,
                REFUSE_PERFORMATIVE,
                {
                    "reason": "no_slots_available",
                    "station": destination_station,
                    "transport_id": str(self.agent.jid),
                }
            )

            self.agent.remove_customer_in_transport(
                customer_id
            )

            self.agent.status = TRANSPORT_IN_DEST

            self.set_next_state(
                TRANSPORT_IN_DEST
            )

            logger.warning(
                "Agent[{}]: Destination station [{}] "
                "is full.".format(
                    self.agent.name,
                    destination_station
                )
            )

            return

        self.set_next_state(
            TRANSPORT_WAITING_FOR_STATION_APPROVAL
        )


class StationSharingInDestinationState(
    StationSharingStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = TRANSPORT_IN_DEST

        logger.warning(
            "Agent[{}]: Station-sharing transport "
            "finished with an unsuccessful trip.".format(
                self.agent.name
            )
        )

    async def run(self):

        await self.agent.stop()


class FSMStationSharingStrategyBehaviour(
    FSMSimfleetBehaviour
):

    def setup(self):

        self.add_state(
            TRANSPORT_WAITING,
            StationSharingWaitingState(),
            initial=True
        )

        self.add_state(
            TRANSPORT_MOVING_TO_DESTINATION,
            StationSharingMovingToDestinationState()
        )

        self.add_state(
            TRANSPORT_WAITING_FOR_STATION_APPROVAL,
            StationSharingWaitingForStationApprovalState()
        )

        self.add_state(
            TRANSPORT_IN_DEST,
            StationSharingInDestinationState()
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING_FOR_STATION_APPROVAL
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_IN_DEST
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_WAITING_FOR_STATION_APPROVAL
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_IN_DEST
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_STATION_APPROVAL,
            TRANSPORT_WAITING_FOR_STATION_APPROVAL
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_STATION_APPROVAL,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_STATION_APPROVAL,
            TRANSPORT_IN_DEST
        )
