import json

from loguru import logger
from spade.behaviour import State
from spade.message import Message

from simfleet.communications.protocol import (
    ACCEPT_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    PROPOSE_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    REQUEST_PERFORMATIVE,
    REQUEST_PROTOCOL,
)

from simfleet.utils.abstractstrategies import (
    FSMSimfleetBehaviour
)

from simfleet.utils.helpers import (
    AlreadyInDestination,
    PathRequestException,
    distance_in_meters,
)

from simfleet.utils.status import (
    CUSTOMER_WAITING,
    CUSTOMER_MOVING_TO_TRANSPORT,
    CUSTOMER_IN_STATION,
    CUSTOMER_IN_TRANSPORT,
    CUSTOMER_MOVING_TO_DEST,
    CUSTOMER_IN_DEST,
)


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class StationSharingCustomerStrategyBehaviour(State):

    async def on_start(self):
        logger.debug(
            "Strategy {} started in station-sharing "
            "customer [{}]".format(
                type(self).__name__,
                self.agent.name
            )
        )

    async def request_station_candidates(self):

        fleetmanagers = (
            self.agent.get_fleetmanagers()
        )

        if not fleetmanagers:
            return

        content = {
            "request_type": "station_sharing_candidates",
            "origin": self.agent.get_position(),
            "dest": self.agent.customer_dest,
        }

        if self.agent.max_walking_dist is not None:
            content["max_walking_distance"] = (
                self.agent.max_walking_dist
            )

        for fleetmanager_id in fleetmanagers.keys():

            msg = Message()

            msg.to = str(
                fleetmanager_id
            )

            msg.set_metadata(
                "protocol",
                REQUEST_PROTOCOL
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
            "Agent[{}]: Requested station-sharing "
            "candidates.".format(
                self.agent.name
            )
        )

    def select_station_pair(self):
        """
        Default baseline policy.

        Origin:
            nearest feasible station to the customer.

        Destination:
            nearest feasible station to the final destination.

        This method is intentionally isolated so it can later be
        replaced by inference, LLM reasoning or another policy.
        """

        origin_candidates = (
            self.agent.get_origin_station_candidates()
        )

        destination_candidates = (
            self.agent.get_destination_station_candidates()
        )

        if (
            not origin_candidates
            or not destination_candidates
        ):
            return None, None

        origin_position = (
            self.agent.get_position()
        )

        destination_position = (
            self.agent.customer_dest
        )

        origin_candidates = [
            station
            for station in origin_candidates
            if station.get("available_bikes", 0) > 0
            and station.get("position") is not None
        ]

        destination_candidates = [
            station
            for station in destination_candidates
            if station.get("available_docks", 0) > 0
            and station.get("position") is not None
        ]

        if (
            not origin_candidates
            or not destination_candidates
        ):
            return None, None

        origin_station = min(
            origin_candidates,
            key=lambda station: distance_in_meters(
                origin_position,
                station["position"]
            )
        )

        destination_station = min(
            destination_candidates,
            key=lambda station: distance_in_meters(
                destination_position,
                station["position"]
            )
        )

        return (
            origin_station,
            destination_station
        )

    async def request_bike(self):

        station_id = (
            self.agent.get_origin_station_id()
        )

        if station_id is None:
            return

        content = {
            "service_name": self.agent.fleet_type,
            "object_type": "customer",
        }

        msg = Message()

        msg.to = str(
            station_id
        )

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
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
            "Agent[{}]: Requested a bike from "
            "station [{}].".format(
                self.agent.name,
                station_id
            )
        )

    async def start_transport_trip(
        self,
        transport_id
    ):

        destination_station = (
            self.agent.get_destination_station()
        )

        origin_station = (
            self.agent.get_origin_station()
        )

        if (
            origin_station is None
            or destination_station is None
        ):
            return False

        content = {
            "customer_id": str(self.agent.jid),
            "origin": origin_station.get(
                "position"
            ),
            "dest": destination_station.get(
                "position"
            ),
            "destination_station": (
                destination_station.get("jid")
            ),
        }

        msg = Message()

        msg.to = str(
            transport_id
        )

        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )

        msg.set_metadata(
            "performative",
            PROPOSE_PERFORMATIVE
        )

        msg.body = json.dumps(
            content
        )

        await self.send(
            msg
        )

        logger.info(
            "Agent[{}]: Started trip with transport [{}] "
            "to station [{}].".format(
                self.agent.name,
                transport_id,
                destination_station.get("jid")
            )
        )

        return True


# ==================================================================
# -------------------------End Behaviour----------------------------
# ==================================================================



################################################################
#                                                              #
#                  Station Transport Strategy                  #
#                                                              #
################################################################

class StationSharingCustomerWaitingState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = CUSTOMER_WAITING

        logger.debug(
            "{} in Station Sharing Customer "
            "Waiting State".format(
                self.agent.jid
            )
        )

    async def run(self):

        if not self.agent.get_fleetmanagers():

            fleetmanager_list = (
                await self.agent.get_list_agent_position(
                    self.agent.fleet_type,
                    self.agent.get_fleetmanagers()
                )
            )

            self.agent.set_fleetmanagers(
                fleetmanager_list
            )

            if not fleetmanager_list:
                await self.agent.sleep(
                    5
                )

            self.set_next_state(
                CUSTOMER_WAITING
            )

            return

        if (
            not self.agent.get_origin_station_candidates()
            or not self.agent.get_destination_station_candidates()
        ):

            await self.request_station_candidates()

            origin_stations = {}
            destination_stations = {}

            fleetmanagers = (
                self.agent.get_fleetmanagers()
            )

            responses = 0

            while responses < len(fleetmanagers):

                msg = await self.receive(
                    timeout=5
                )

                if not msg:
                    break

                if (
                    msg.get_metadata("protocol")
                    != REQUEST_PROTOCOL
                ):
                    continue

                if (
                    msg.get_metadata("performative")
                    != INFORM_PERFORMATIVE
                ):
                    continue

                try:
                    content = json.loads(
                        msg.body
                    )

                except (
                    json.JSONDecodeError,
                    TypeError
                ):
                    continue

                if (
                    content.get("request_type")
                    != "station_sharing_candidates"
                ):
                    continue

                responses += 1

                for station in content.get(
                    "origin_stations",
                    []
                ):

                    station_jid = station.get(
                        "jid"
                    )

                    if station_jid is not None:
                        origin_stations[
                            str(station_jid)
                        ] = station

                for station in content.get(
                    "destination_stations",
                    []
                ):

                    station_jid = station.get(
                        "jid"
                    )

                    if station_jid is not None:
                        destination_stations[
                            str(station_jid)
                        ] = station

            self.agent.set_origin_station_candidates(
                list(
                    origin_stations.values()
                )
            )

            self.agent.set_destination_station_candidates(
                list(
                    destination_stations.values()
                )
            )

        (
            origin_station,
            destination_station
        ) = self.select_station_pair()

        if (
            origin_station is None
            or destination_station is None
        ):

            logger.info(
                "Agent[{}]: No feasible station pair "
                "available.".format(
                    self.agent.name
                )
            )

            self.agent.clear_station_candidates()

            await self.agent.sleep(
                5
            )

            self.set_next_state(
                CUSTOMER_WAITING
            )

            return

        self.agent.set_origin_station(
            origin_station
        )

        self.agent.set_destination_station(
            destination_station
        )

        station_position = (
            origin_station.get(
                "position"
            )
        )

        logger.info(
            "Agent[{}]: Selected origin station [{}] "
            "and destination station [{}].".format(
                self.agent.name,
                origin_station.get("jid"),
                destination_station.get("jid")
            )
        )

        try:

            await self.agent.move_to(
                station_position
            )

        except AlreadyInDestination:

            await self.request_bike()

            self.agent.status = (
                CUSTOMER_IN_STATION
            )

            self.set_next_state(
                CUSTOMER_IN_STATION
            )

            return

        except PathRequestException:

            logger.error(
                "Agent[{}]: Could not calculate walking "
                "path to origin station [{}].".format(
                    self.agent.name,
                    origin_station.get("jid")
                )
            )

            self.agent.trip_failed = True
            self.agent.failure_reason = (
                "path_unavailable_to_origin_station"
            )

            self.set_next_state(
                CUSTOMER_IN_DEST
            )

            return

        self.agent.status = (
            CUSTOMER_MOVING_TO_TRANSPORT
        )

        self.set_next_state(
            CUSTOMER_MOVING_TO_TRANSPORT
        )


class StationSharingCustomerMovingToStationState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            CUSTOMER_MOVING_TO_TRANSPORT
        )

        logger.debug(
            "{} moving to sharing station".format(
                self.agent.jid
            )
        )

    async def run(self):

        if not self.agent.is_in_destination():

            self.set_next_state(
                CUSTOMER_MOVING_TO_TRANSPORT
            )

            await self.agent.sleep(
                1
            )

            return

        await self.request_bike()

        self.agent.status = (
            CUSTOMER_IN_STATION
        )

        self.set_next_state(
            CUSTOMER_IN_STATION
        )

class StationSharingCustomerInStationState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            CUSTOMER_IN_STATION
        )

        logger.debug(
            "{} waiting for a bike in sharing station".format(
                self.agent.jid
            )
        )

    async def run(self):

        station_id = (
            self.agent.get_origin_station_id()
        )

        if station_id is None:

            self.agent.trip_failed = True
            self.agent.failure_reason = (
                "origin_station_missing"
            )

            self.set_next_state(
                CUSTOMER_IN_DEST
            )

            return

        msg = await self.receive(
            timeout=60
        )

        if not msg:

            self.set_next_state(
                CUSTOMER_IN_STATION
            )

            return

        if (
            msg.get_metadata("protocol")
            != REQUEST_PROTOCOL
        ):

            self.set_next_state(
                CUSTOMER_IN_STATION
            )

            return

        if not self.agent.is_same_jid(
            msg.sender,
            station_id
        ):

            self.set_next_state(
                CUSTOMER_IN_STATION
            )

            return

        performative = msg.get_metadata(
            "performative"
        )

        if performative == ACCEPT_PERFORMATIVE:

            # QueueStationAgent has accepted the customer
            # into the service queue.
            #
            # The actual bike assignment will arrive later
            # as INFORM.
            self.set_next_state(
                CUSTOMER_IN_STATION
            )

            return

        if performative == REFUSE_PERFORMATIVE:

            logger.warning(
                "Agent[{}]: Origin station [{}] has "
                "no bike available.".format(
                    self.agent.name,
                    station_id
                )
            )

            self.agent.set_origin_station_candidates(
                [
                    station
                    for station in (
                        self.agent.get_origin_station_candidates()
                    )
                    if str(station.get("jid")) != str(station_id)
                ]
            )

            self.agent.clear_origin_station()

            self.set_next_state(
                CUSTOMER_WAITING
            )

            return

        if performative == INFORM_PERFORMATIVE:

            try:
                content = json.loads(
                    msg.body
                )

            except (
                json.JSONDecodeError,
                TypeError
            ):

                self.set_next_state(
                    CUSTOMER_IN_STATION
                )

                return

            transport_id = content.get(
                "transport_id"
            )

            if transport_id is None:

                self.set_next_state(
                    CUSTOMER_IN_STATION
                )

                return

            self.agent.set_current_transport(
                transport_id
            )

            started = await self.start_transport_trip(
                transport_id
            )

            if not started:

                self.agent.trip_failed = True
                self.agent.failure_reason = (
                    "transport_trip_not_started"
                )

                self.set_next_state(
                    CUSTOMER_IN_DEST
                )

                return

            self.agent.clear_station_candidates()

            self.agent.status = (
                CUSTOMER_IN_TRANSPORT
            )

            self.set_next_state(
                CUSTOMER_IN_TRANSPORT
            )

            return

        self.set_next_state(
            CUSTOMER_IN_STATION
        )


class StationSharingCustomerInTransportState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            CUSTOMER_IN_TRANSPORT
        )

        logger.debug(
            "{} in station-sharing transport".format(
                self.agent.jid
            )
        )

    async def run(self):

        transport_id = (
            self.agent.get_current_transport()
        )

        if transport_id is None:

            self.agent.trip_failed = True
            self.agent.failure_reason = (
                "transport_missing"
            )

            self.set_next_state(
                CUSTOMER_IN_DEST
            )

            return

        msg = await self.receive(
            timeout=60
        )

        if not msg:

            self.set_next_state(
                CUSTOMER_IN_TRANSPORT
            )

            return

        if (
            msg.get_metadata("protocol")
            != REQUEST_PROTOCOL
        ):

            self.set_next_state(
                CUSTOMER_IN_TRANSPORT
            )

            return

        if not self.agent.is_same_jid(
            msg.sender,
            transport_id
        ):

            self.set_next_state(
                CUSTOMER_IN_TRANSPORT
            )

            return

        performative = msg.get_metadata(
            "performative"
        )

        try:
            content = json.loads(
                msg.body
            )

        except (
            json.JSONDecodeError,
            TypeError
        ):
            content = {}

        if performative == REFUSE_PERFORMATIVE:

            reason = content.get(
                "reason"
            )

            if reason == "no_slots_available":

                logger.warning(
                    "Agent[{}]: Destination station [{}] "
                    "is full.".format(
                        self.agent.name,
                        self.agent.get_destination_station_id()
                    )
                )

                self.agent.set_trip_failure(
                    operation="dropoff",
                    reason="no_slots_available",
                    station=(
                        self.agent.get_destination_station()
                    )
                )

                self.agent.clear_current_transport()

                self.set_next_state(
                    CUSTOMER_IN_DEST
                )

                return

            self.agent.trip_failed = True
            self.agent.failure_reason = (
                reason or "transport_failure"
            )

            self.agent.clear_current_transport()

            self.set_next_state(
                CUSTOMER_IN_DEST
            )

            return

        if performative != INFORM_PERFORMATIVE:

            self.set_next_state(
                CUSTOMER_IN_TRANSPORT
            )

            return

        status = content.get(
            "status"
        )

        if status == CUSTOMER_IN_TRANSPORT:

            self.set_next_state(
                CUSTOMER_IN_TRANSPORT
            )

            return

        if status != CUSTOMER_IN_DEST:

            self.set_next_state(
                CUSTOMER_IN_TRANSPORT
            )

            return

        self.agent.clear_current_transport()

        logger.info(
            "Agent[{}]: Bike successfully registered "
            "in destination station [{}].".format(
                self.agent.name,
                self.agent.get_destination_station_id()
            )
        )

        try:

            await self.agent.move_to(
                self.agent.customer_dest
            )

        except AlreadyInDestination:

            self.set_next_state(
                CUSTOMER_IN_DEST
            )

            return

        except PathRequestException:

            logger.error(
                "Agent[{}]: Could not calculate final "
                "walking path.".format(
                    self.agent.name
                )
            )

            self.agent.trip_failed = True
            self.agent.failure_reason = (
                "path_unavailable_to_destination"
            )

            self.set_next_state(
                CUSTOMER_IN_DEST
            )

            return

        self.agent.status = (
            CUSTOMER_MOVING_TO_DEST
        )

        self.set_next_state(
            CUSTOMER_MOVING_TO_DEST
        )


class StationSharingCustomerMovingToDestinationState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            CUSTOMER_MOVING_TO_DEST
        )

        logger.debug(
            "{} walking to final destination".format(
                self.agent.jid
            )
        )

    async def run(self):

        if not self.agent.is_in_destination():

            self.set_next_state(
                CUSTOMER_MOVING_TO_DEST
            )

            await self.agent.sleep(
                1
            )

            return

        self.set_next_state(
            CUSTOMER_IN_DEST
        )


class StationSharingCustomerInDestState(
    StationSharingCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            CUSTOMER_IN_DEST
        )

        self.agent.clear_current_transport()
        self.agent.clear_station_candidates()

        if self.agent.trip_failed:

            logger.warning(
                "Agent[{}]: Station-sharing trip failed. "
                "Operation: {}. Reason: {}.".format(
                    self.agent.name,
                    self.agent.failure_operation,
                    self.agent.failure_reason
                )
            )

        else:

            logger.info(
                "Agent[{}]: Station-sharing trip "
                "completed successfully.".format(
                    self.agent.name
                )
            )

    async def run(self):

        if self.agent.trip_failed:
            await self.agent.stop()

        return


class FSMStationSharingCustomerStrategyBehaviour(
    FSMSimfleetBehaviour
):

    def setup(self):

        self.add_state(
            CUSTOMER_WAITING,
            StationSharingCustomerWaitingState(),
            initial=True
        )

        self.add_state(
            CUSTOMER_MOVING_TO_TRANSPORT,
            StationSharingCustomerMovingToStationState()
        )

        self.add_state(
            CUSTOMER_IN_STATION,
            StationSharingCustomerInStationState()
        )

        self.add_state(
            CUSTOMER_IN_TRANSPORT,
            StationSharingCustomerInTransportState()
        )

        self.add_state(
            CUSTOMER_MOVING_TO_DEST,
            StationSharingCustomerMovingToDestinationState()
        )

        self.add_state(
            CUSTOMER_IN_DEST,
            StationSharingCustomerInDestState()
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_MOVING_TO_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_IN_STATION
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_IN_DEST
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_TRANSPORT,
            CUSTOMER_MOVING_TO_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_TRANSPORT,
            CUSTOMER_IN_STATION
        )

        self.add_transition(
            CUSTOMER_IN_STATION,
            CUSTOMER_IN_STATION
        )

        self.add_transition(
            CUSTOMER_IN_STATION,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_IN_STATION,
            CUSTOMER_IN_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_IN_STATION,
            CUSTOMER_IN_DEST
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_IN_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_MOVING_TO_DEST
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_IN_DEST
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_DEST,
            CUSTOMER_MOVING_TO_DEST
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_DEST,
            CUSTOMER_IN_DEST
        )
