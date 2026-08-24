import json

from loguru import logger
from spade.behaviour import State
from spade.message import Message

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
)

from simfleet.utils.status import (
    CUSTOMER_WAITING,
    CUSTOMER_WAITING_TO_MOVE,
    CUSTOMER_WAITING_FOR_JOURNEY,
    CUSTOMER_MOVING_TO_DEST,
    CUSTOMER_IN_STOP,
    CUSTOMER_WAITING_FOR_APPROVAL,
    CUSTOMER_IN_TRANSPORT,
    CUSTOMER_IN_DEST,
    CUSTOMER_JOURNEY_FAILED,
)

from simfleet.utils.helpers import (
    AlreadyInDestination,
    PathRequestException,
)

from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class PublicTransportCustomerStrategyBehaviour(
    State
):
    """
    Base State used by the Public Transport Customer FSM.

    It centralizes the communication contracts used by
    the customer while keeping the concrete state
    transitions in specialized FSM states.
    """

    async def on_start(
        self
    ):

        logger.debug(
            "Strategy {} started in public transport customer {}".format(
                type(self).__name__,
                self.agent.name
            )
        )

    def get_stop_id(
        self,
        stop
    ):

        if stop is None:
            return None

        return stop.get(
            "id"
        )

    def get_stop_jid(
        self,
        stop
    ):

        if stop is None:
            return None

        return stop.get(
            "jid"
        )

    def select_journey(
        self,
        journeys=None
    ):
        """
        Selects a journey using the baseline
        lexicographic rule:

        1. fewer public transport transfers
        2. less walking distance
        3. fewer public transport stops
        """

        if journeys is None:

            journeys = (
                self.agent.get_journey_candidates()
            )

        if not journeys:
            return None

        return min(
            journeys,
            key=lambda journey: (
                journey.get(
                    "transfers",
                    float("inf")
                ),

                journey.get(
                    "walking_distance",
                    float("inf")
                ),

                journey.get(
                    "public_transport_stops",
                    float("inf")
                ),
            )
        )

    async def request_journeys(
        self,
        fleetmanager_id
    ):

        if fleetmanager_id is None:
            return False

        origin = (
            self.agent.get_position()
        )

        destination = (
            self.agent.get_target_position()
        )

        if (
            origin is None
            or destination is None
        ):

            logger.warning(
                "Public transport customer {} cannot request journeys "
                "without origin and destination.".format(
                    self.agent.name
                )
            )

            return False

        content = {
            "request_type":
                "public_transport_journeys",

            "origin":
                origin,

            "dest":
                destination,

            "max_access_walking_distance":
                self.agent.get_max_access_walking_distance(),

            "max_transfer_walking_distance":
                self.agent.get_max_transfer_walking_distance(),

            "max_transfers":
                self.agent.get_max_transfers(),
        }

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

        logger.debug(
            "Public transport customer {} requesting journeys from {}".format(
                self.agent.name,
                fleetmanager_id
            )
        )

        await self.send(
            msg
        )

        return True

    async def request_stop_queue(
        self,
        stop,
        pattern_id,
        destination_stop
    ):

        stop_jid = (
            self.get_stop_jid(
                stop
            )
        )

        stop_id = (
            self.get_stop_id(
                stop
            )
        )

        if (
            stop_jid is None
            or stop_id is None
            or pattern_id is None
            or destination_stop is None
        ):

            logger.warning(
                "Public transport customer {} cannot enter stop queue: "
                "invalid stop or pattern data.".format(
                    self.agent.name
                )
            )

            return False

        content = {
            "service_name":
                pattern_id,

            "object_type":
                "customer",

            "args": {
                "pattern_id":
                    pattern_id,

                "destination_stop":
                    destination_stop,
            },
        }

        msg = Message()

        msg.to = str(
            stop_jid
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

        logger.debug(
            "Public transport customer {} requesting queue {} at stop {}".format(
                self.agent.name,
                pattern_id,
                stop_id
            )
        )

        await self.send(
            msg
        )

        return True

    async def cancel_stop_queue(
        self,
        stop,
        pattern_id
    ):

        stop_jid = (
            self.get_stop_jid(
                stop
            )
        )

        if (
            stop_jid is None
            or pattern_id is None
        ):
            return False

        msg = Message()

        msg.to = str(
            stop_jid
        )

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
                "service_name":
                    pattern_id,
            }
        )

        await self.send(
            msg
        )

        return True

    async def request_boarding(
        self,
        vehicle_id,
        leg
    ):

        if (
            vehicle_id is None
            or leg is None
        ):
            return False

        pattern_id = leg.get(
            "pattern_id"
        )

        origin_stop = (
            self.get_stop_id(
                leg.get(
                    "origin_stop"
                )
            )
        )

        destination_stop = (
            self.get_stop_id(
                leg.get(
                    "destination_stop"
                )
            )
        )

        if (
            pattern_id is None
            or origin_stop is None
            or destination_stop is None
        ):

            logger.warning(
                "Public transport customer {} cannot request boarding: "
                "invalid public transport leg.".format(
                    self.agent.name
                )
            )

            return False

        content = {
            "request_type":
                "public_transport_board",

            "pattern_id":
                pattern_id,

            "origin_stop":
                origin_stop,

            "destination_stop":
                destination_stop,
        }

        msg = Message()

        msg.to = str(
            vehicle_id
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

        logger.debug(
            "Public transport customer {} requesting boarding to vehicle {} "
            "for pattern {} from {} to {}".format(
                self.agent.name,
                vehicle_id,
                pattern_id,
                origin_stop,
                destination_stop
            )
        )

        await self.send(
            msg
        )

        return True

    async def run(
        self
    ):

        raise NotImplementedError


# ==================================================================
# ------------------------------------------------------------------
# ==================================================================




class PublicTransportCustomerWaitingToMoveState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Main dispatcher state for the Public Transport customer.

    Responsibilities:

    - Discover Public Transport FleetManagers.
    - Request journey candidates when no journey exists.
    - Inspect the current journey leg.
    - Route execution to walking, stop waiting or final state.
    """

    async def on_start(
        self
    ):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_WAITING_TO_MOVE
        )

    async def run(
        self
    ):

        #
        # No journey has been selected yet.
        #

        if self.agent.get_journey() is None:

            fleetmanagers = (
                self.agent.get_fleetmanagers()
            )

            #
            # Discover FleetManagers only when they
            # are not already known.
            #

            if not fleetmanagers:

                logger.info(
                    "Public transport customer {} "
                    "looking for FleetManagers.".format(
                        self.agent.name
                    )
                )

                fleetmanagers = (
                    await self.agent
                    .get_list_agent_position(
                        self.agent.fleet_type,
                        self.agent.get_fleetmanagers()
                    )
                )

                self.agent.set_fleetmanagers(
                    fleetmanagers
                )

            #
            # No Public Transport FleetManager exists
            # yet. Retry later.
            #

            if not fleetmanagers:

                logger.warning(
                    "Public transport customer {} "
                    "found no FleetManagers.".format(
                        self.agent.name
                    )
                )

                await self.agent.sleep(
                    5
                )

                self.set_next_state(
                    CUSTOMER_WAITING_TO_MOVE
                )

                return

            #
            # Ask every Public Transport FleetManager.
            #
            # The Customer will combine all candidate
            # journeys before selecting one.
            #

            requests_sent = 0

            for fleetmanager_id in (
                fleetmanagers.keys()
            ):

                sent = (
                    await self.request_journeys(
                        fleetmanager_id
                    )
                )

                if sent:
                    requests_sent += 1

            if requests_sent == 0:

                logger.warning(
                    "Public transport customer {} "
                    "could not send any journey request.".format(
                        self.agent.name
                    )
                )

                await self.agent.sleep(
                    5
                )

                self.set_next_state(
                    CUSTOMER_WAITING_TO_MOVE
                )

                return

            self.set_next_state(
                CUSTOMER_WAITING_FOR_JOURNEY
            )

            return

        #
        # Journey already selected.
        #
        # This state now acts as the dispatcher for
        # the current leg.
        #

        if self.agent.is_journey_finished():

            self.set_next_state(
                CUSTOMER_IN_DEST
            )

            return

        leg = (
            self.agent.get_current_leg()
        )

        if leg is None:

            self.set_next_state(
                CUSTOMER_IN_DEST
            )

            return

        leg_type = leg.get(
            "type"
        )

        #
        # Walking leg.
        #

        if leg_type == "walking":

            self.set_next_state(
                CUSTOMER_MOVING_TO_DEST
            )

            return

        #
        # Public Transport leg.
        #

        if leg_type == (
            "public_transport"
        ):

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        #
        # Malformed / unsupported journey.
        #
        # Do not mark the journey as completed because
        # the Customer has not actually reached its
        # destination.
        #

        logger.error(
            "Public transport customer {} "
            "has unsupported journey leg type {}.".format(
                self.agent.name,
                leg_type
            )
        )

        await self.agent.sleep(
            5
        )

        self.set_next_state(
            CUSTOMER_WAITING_TO_MOVE
        )


class PublicTransportCustomerWaitingForJourneyState(
    PublicTransportCustomerStrategyBehaviour
):

    async def on_start(self):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_WAITING_FOR_JOURNEY
        )

    async def run(self):

        fleetmanagers = (
            self.agent.get_fleetmanagers()
        )

        if not fleetmanagers:

            #
            # Bootstrap case.
            #
            # No polling is performed here.
            # The Customer returns to WAITING_TO_MOVE
            # so the normal FleetManager discovery
            # mechanism can be executed.
            #

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        journey_candidates = []

        expected_responses = len(
            fleetmanagers
        )

        received_responses = 0

        while (
            received_responses
            < expected_responses
        ):

            msg = await self.receive(
                timeout=5
            )

            if not msg:

                #
                # One or more FleetManagers did not
                # answer inside the response window.
                #

                break

            protocol = msg.get_metadata(
                "protocol"
            )

            performative = msg.get_metadata(
                "performative"
            )

            if (
                protocol != REQUEST_PROTOCOL
                or performative
                != INFORM_PERFORMATIVE
            ):

                continue

            try:

                content = (
                    json.loads(msg.body)
                    if msg.body
                    else {}
                )

            except (
                json.JSONDecodeError,
                TypeError
            ):

                logger.warning(
                    "Public transport customer {} "
                    "received an invalid journey "
                    "response.".format(
                        self.agent.name
                    )
                )

                continue

            if (
                content.get("request_type")
                != "public_transport_journeys"
            ):

                continue

            received_responses += 1

            journeys = content.get(
                "journeys",
                []
            )

            if not isinstance(
                journeys,
                list
            ):
                continue

            journey_candidates.extend(
                journeys
            )

        self.agent.set_journey_candidates(
            journey_candidates
        )

        selected_journey = (
            self.select_journey(
                journey_candidates
            )
        )

        if selected_journey is None:

            logger.warning(
                "Public transport customer {} "
                "could not find a feasible journey.".format(
                    self.agent.name
                )
            )

            self.agent.clear_journey()

            self.set_next_state(
                CUSTOMER_JOURNEY_FAILED
            )

            return

        #
        # From this point the Journey becomes the
        # Customer's runtime source of truth.
        #

        self.agent.set_journey(
            selected_journey
        )

        #
        # Public Transport Statistics
        #
        # This is the beginning of EXECUTION of the
        # selected Journey, not the beginning of
        # journey planning.
        #
        # StatisticsStore supplies the event timestamp.
        #
        # Therefore:
        #
        # pt_journey_completed.timestamp
        # -
        # pt_journey_started.timestamp
        #
        # gives total_journey_time.
        #

        self.agent.events_store.emit(
            event_type="pt_journey_started",
            details={
                "transfers":
                    selected_journey.get(
                        "transfers",
                        0
                    ),

                "walking_distance":
                    selected_journey.get(
                        "walking_distance",
                        0
                    ),

                "public_transport_stops":
                    selected_journey.get(
                        "public_transport_stops",
                        0
                    ),

                "legs":
                    len(
                        selected_journey.get(
                            "legs",
                            []
                        )
                    ),
            }
        )

        logger.info(
            "Public transport customer {} "
            "selected journey with {} transfers, "
            "{} walking distance and {} "
            "public transport stops.".format(
                self.agent.name,

                selected_journey.get(
                    "transfers",
                    0
                ),

                selected_journey.get(
                    "walking_distance",
                    0
                ),

                selected_journey.get(
                    "public_transport_stops",
                    0
                )
            )
        )

        self.set_next_state(
            CUSTOMER_WAITING_TO_MOVE
        )

class PublicTransportCustomerJourneyFailedState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Final state for a Customer whose requested
    journey could not be completed.

    No next state is configured.
    Therefore the Public Transport Customer
    FSM finishes here.
    """

    async def on_start(self):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_JOURNEY_FAILED
        )

        logger.warning(
            "Public transport customer {} "
            "finished with an incomplete journey "
            "at position {}.".format(
                self.agent.name,
                self.agent.get_position()
            )
        )

        #
        # Public Transport Statistics
        #

        self.agent.events_store.emit(
            event_type="pt_journey_failed",
            details={
                "position":
                    self.agent.get_position(),

                "leg_index":
                    self.agent.current_leg_index,
            }
        )

    async def run(self):

        self.agent.clear_current_vehicle()

        self.agent.clear_waiting_pattern_id()

        self.agent.pedestrian_dest = None

        #
        # Intentionally no set_next_state().
        #
        # CUSTOMER_JOURNEY_FAILED remains a real
        # terminal state.
        #

        return

class PublicTransportCustomerMovingToDestState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Executes a walking leg of a multimodal journey.

    The same state is used for:

    - access walking:
        Customer -> Public Transport Stop

    - transfer walking:
        Public Transport Stop -> Public Transport Stop

    - final walking:
        Public Transport Stop -> final destination
    """

    async def on_start(
        self
    ):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_MOVING_TO_DEST
        )

    def complete_walking_leg(
        self,
        leg
    ):
        """
        Completes the current walking leg and updates
        the logical Stop state when appropriate.
        """

        destination_stop = (
            leg.get(
                "destination_stop"
            )
        )

        #
        # Access or transfer walking:
        #
        # the walking leg finishes at a Public
        # Transport Stop.
        #

        if destination_stop is not None:

            destination_stop_id = (
                self.get_stop_id(
                    destination_stop
                )
            )

            if destination_stop_id is not None:

                self.agent.set_current_stop(
                    destination_stop_id
                )

        #
        # Final walking:
        #
        # the Customer is no longer located at a
        # Public Transport Stop.
        #

        else:

            self.agent.clear_current_stop()

        #
        # pedestrian_dest is only used while the
        # physical walking movement is active.
        #

        self.agent.pedestrian_dest = None

        #
        # The walking leg has been completed.
        #

        self.agent.advance_leg()

    async def run(
        self
    ):

        leg = (
            self.agent.get_current_leg()
        )

        #
        # Defensive check.
        #
        # Normally this state is entered only when the
        # dispatcher has identified a walking leg.
        #

        if leg is None:

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if leg.get(
            "type"
        ) != "walking":

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        destination = (
            leg.get(
                "destination"
            )
        )

        if destination is None:

            logger.error(
                "Public transport customer {} "
                "has a walking leg without destination.".format(
                    self.agent.name
                )
            )

            await self.agent.sleep(
                5
            )

            self.set_next_state(
                CUSTOMER_MOVING_TO_DEST
            )

            return

        #
        # Keep the generic PedestrianAgent runtime
        # attribute synchronized.
        #

        self.agent.pedestrian_dest = (
            destination
        )

        #
        # The Customer may already be physically at
        # the destination.
        #

        if self.agent.get_position() == (
            destination
        ):

            self.complete_walking_leg(
                leg
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        #
        # A movement towards this exact destination is
        # already active.
        #
        # Do not request another route.
        #

        if self.agent.dest == (
            destination
        ):

            if self.agent.is_in_destination():

                self.complete_walking_leg(
                    leg
                )

                self.set_next_state(
                    CUSTOMER_WAITING_TO_MOVE
                )

                return

            await self.agent.sleep(
                1
            )

            self.set_next_state(
                CUSTOMER_MOVING_TO_DEST
            )

            return

        #
        # No movement towards the current walking-leg
        # destination is active.
        #
        # self.agent.dest may still contain the
        # destination of a previous walking leg.
        #

        try:

            await self.agent.move_to(
                destination
            )

        except AlreadyInDestination:

            #
            # Synchronize MovableMixin state.
            #

            self.agent.dest = (
                destination
            )

            self.complete_walking_leg(
                leg
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        except PathRequestException:

            logger.error(
                "Public transport customer {} "
                "could not obtain a walking route to {}.".format(
                    self.agent.name,
                    destination
                )
            )

            await self.agent.sleep(
                2
            )

            self.set_next_state(
                CUSTOMER_MOVING_TO_DEST
            )

            return

        #
        # move_to() creates MovingBehaviour
        # asynchronously.
        #
        # The FSM only monitors its completion.
        #

        await self.agent.sleep(
            1
        )

        self.set_next_state(
            CUSTOMER_MOVING_TO_DEST
        )


class PublicTransportCustomerInStopState(
    PublicTransportCustomerStrategyBehaviour
):

    async def on_start(self):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_IN_STOP
        )

    def _bare_jid(self, jid):

        if jid is None:
            return None

        return str(jid).split("/")[0]

    async def run(self):

        leg = self.agent.get_current_leg()

        if leg is None:

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if (
            leg.get("type")
            != "public_transport"
        ):

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        pattern_id = (
            leg.get("pattern_id")
        )

        origin_stop = (
            leg.get("origin_stop")
        )

        destination_stop = (
            leg.get("destination_stop")
        )

        origin_stop_id = (
            self.get_stop_id(
                origin_stop
            )
        )

        origin_stop_jid = (
            self.get_stop_jid(
                origin_stop
            )
        )

        destination_stop_id = (
            self.get_stop_id(
                destination_stop
            )
        )

        if (
            pattern_id is None
            or origin_stop_id is None
            or origin_stop_jid is None
            or destination_stop_id is None
        ):

            logger.error(
                "Public transport customer {} "
                "has an invalid public transport leg "
                "while entering a stop.".format(
                    self.agent.name
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        #
        # current_stop may legitimately be None when
        # the Journey begins directly at a Stop.
        #
        # In that case QueueStationAgent performs the
        # physical proximity check before accepting
        # the Customer into the queue.
        #
        # We only reject the operation when the
        # Customer explicitly knows that it is at a
        # DIFFERENT logical Stop.
        #

        current_stop = (
            self.agent.get_current_stop()
        )

        if (
            current_stop is not None
            and current_stop
            != origin_stop_id
        ):

            logger.error(
                "Public transport customer {} "
                "cannot enter queue at stop {} "
                "because current logical stop "
                "is {}.".format(
                    self.agent.name,
                    origin_stop_id,
                    current_stop
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        sent = await self.request_stop_queue(
            origin_stop,
            pattern_id,
            destination_stop_id
        )

        if not sent:

            logger.warning(
                "Public transport customer {} "
                "could not request queue {} "
                "at stop {}.".format(
                    self.agent.name,
                    pattern_id,
                    origin_stop_id
                )
            )

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        msg = await self.receive(
            timeout=30
        )

        if not msg:

            #
            # QueueStationAgent does not deduplicate
            # queue entries.
            #
            # If ACCEPT was lost, retrying directly
            # could enqueue the same Customer twice.
            #
            # Therefore CANCEL before retrying.
            #

            await self.cancel_stop_queue(
                origin_stop,
                pattern_id
            )

            logger.warning(
                "Public transport customer {} "
                "did not receive queue confirmation "
                "from stop {} for pattern {}. "
                "Cancelling before retry.".format(
                    self.agent.name,
                    origin_stop_id,
                    pattern_id
                )
            )

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        protocol = msg.get_metadata(
            "protocol"
        )

        performative = msg.get_metadata(
            "performative"
        )

        sender = self._bare_jid(
            msg.sender
        )

        expected_sender = self._bare_jid(
            origin_stop_jid
        )

        if (
            protocol != REQUEST_PROTOCOL
            or sender != expected_sender
        ):

            await self.cancel_stop_queue(
                origin_stop,
                pattern_id
            )

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        try:

            content = (
                json.loads(msg.body)
                if msg.body
                else {}
            )

        except (
            json.JSONDecodeError,
            TypeError
        ):

            await self.cancel_stop_queue(
                origin_stop,
                pattern_id
            )

            logger.warning(
                "Public transport customer {} "
                "received invalid queue response "
                "from stop {}.".format(
                    self.agent.name,
                    origin_stop_id
                )
            )

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        if (
            performative
            == ACCEPT_PERFORMATIVE
        ):

            station_id = self._bare_jid(
                content.get(
                    "station_id"
                )
            )

            if (
                station_id
                != expected_sender
            ):

                await self.cancel_stop_queue(
                    origin_stop,
                    pattern_id
                )

                logger.warning(
                    "Public transport customer {} "
                    "received queue acceptance for "
                    "unexpected station {}.".format(
                        self.agent.name,
                        station_id
                    )
                )

                self.set_next_state(
                    CUSTOMER_IN_STOP
                )

                return

            #
            # Only now do we consider the Customer
            # logically located at the Stop.
            #

            self.agent.set_current_stop(
                origin_stop_id
            )

            self.agent.set_waiting_pattern_id(
                pattern_id
            )

            self.agent.clear_current_vehicle()

            #
            # Public Transport Statistics
            #
            # Stop has confirmed that the Customer
            # was actually inserted into its queue.
            #

            self.agent.events_store.emit(
                event_type="pt_queue_entered",
                details={
                    "pattern_id":
                        pattern_id,

                    "route_id":
                        leg.get(
                            "route_id"
                        ),

                    "mode":
                        leg.get(
                            "mode"
                        ),

                    "stop_id":
                        origin_stop_id,

                    "destination_stop":
                        destination_stop_id,

                    "leg_index":
                        self.agent.current_leg_index,
                }
            )

            logger.info(
                "Public transport customer {} "
                "registered at stop {} waiting "
                "for pattern {} to destination {}.".format(
                    self.agent.name,
                    origin_stop_id,
                    pattern_id,
                    destination_stop_id
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING
            )

            return

        if (
            performative
            == REFUSE_PERFORMATIVE
        ):

            self.agent.clear_waiting_pattern_id()

            logger.info(
                "Public transport customer {} "
                "was refused queue access at stop {} "
                "for pattern {}.".format(
                    self.agent.name,
                    origin_stop_id,
                    pattern_id
                )
            )

            self.set_next_state(
                CUSTOMER_IN_STOP
            )

            return

        await self.cancel_stop_queue(
            origin_stop,
            pattern_id
        )

        self.set_next_state(
            CUSTOMER_IN_STOP
        )


class PublicTransportCustomerWaitingState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Waits reactively at a Public Transport Stop.

    The Customer is already registered in the queue
    associated with the directional Pattern of the
    current Public Transport leg.

    No polling is performed.

    The Customer remains blocked until the Stop informs
    that a compatible Vehicle is available.
    """

    async def on_start(
        self
    ):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_WAITING
        )

    async def run(
        self
    ):

        leg = (
            self.agent.get_current_leg()
        )

        #
        # Defensive validation.
        #

        if leg is None:

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if leg.get(
            "type"
        ) != (
            "public_transport"
        ):

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        pattern_id = leg.get(
            "pattern_id"
        )

        origin_stop = leg.get(
            "origin_stop"
        )

        destination_stop = leg.get(
            "destination_stop"
        )

        origin_stop_id = (
            self.get_stop_id(
                origin_stop
            )
        )

        origin_stop_jid = (
            self.get_stop_jid(
                origin_stop
            )
        )

        destination_stop_id = (
            self.get_stop_id(
                destination_stop
            )
        )

        if (
            pattern_id is None
            or origin_stop_id is None
            or origin_stop_jid is None
            or destination_stop_id is None
        ):

            logger.error(
                "Public transport customer {} "
                "has an invalid public transport leg "
                "while waiting at stop.".format(
                    self.agent.name
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if (
            self.agent.get_current_stop()
            != origin_stop_id
        ):

            logger.error(
                "Public transport customer {} "
                "is waiting at logical stop {}, "
                "but current leg starts at {}.".format(
                    self.agent.name,
                    self.agent.get_current_stop(),
                    origin_stop_id
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if (
            self.agent.get_waiting_pattern_id()
            != pattern_id
        ):

            logger.error(
                "Public transport customer {} "
                "is waiting for pattern {}, "
                "but current leg requires {}.".format(
                    self.agent.name,
                    self.agent.get_waiting_pattern_id(),
                    pattern_id
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        expected_sender = str(
            origin_stop_jid
        ).split(
            "/"
        )[0]

        #
        # Reactive wait.
        #
        # SPADE receive() without timeout is
        # non-blocking.
        #
        # A positive timeout is required to actually
        # suspend the behaviour while waiting.
        #

        while True:

            msg = await self.receive(
                timeout=3600
            )

            #
            # Defensive wake-up only.
            #
            # No polling or external query is performed.
            #

            if not msg:
                continue

            protocol = (
                msg.get_metadata(
                    "protocol"
                )
            )

            performative = (
                msg.get_metadata(
                    "performative"
                )
            )

            if (
                protocol != REQUEST_PROTOCOL
                or performative != INFORM_PERFORMATIVE
            ):
                continue

            try:

                content = (
                    json.loads(
                        msg.body
                    )
                    if msg.body
                    else {}
                )

            except (
                json.JSONDecodeError,
                TypeError,
            ):

                logger.warning(
                    "Public transport customer {} "
                    "received an invalid message while "
                    "waiting at stop {}.".format(
                        self.agent.name,
                        origin_stop_id
                    )
                )

                continue

            if content.get(
                "request_type"
            ) != (
                "public_transport_vehicle_available"
            ):

                continue

            vehicle_id = content.get(
                "vehicle_id"
            )

            informed_pattern_id = (
                content.get(
                    "pattern_id"
                )
            )

            informed_stop_id = (
                content.get(
                    "stop"
                )
            )

            sender = str(
                msg.sender
            ).split(
                "/"
            )[0]

            if sender != (
                expected_sender
            ):

                logger.warning(
                    "Public transport customer {} "
                    "ignored vehicle notification from "
                    "unexpected stop {}.".format(
                        self.agent.name,
                        sender
                    )
                )

                continue

            if informed_stop_id != (
                origin_stop_id
            ):

                logger.warning(
                    "Public transport customer {} "
                    "ignored vehicle notification for "
                    "stop {} while waiting at {}.".format(
                        self.agent.name,
                        informed_stop_id,
                        origin_stop_id
                    )
                )

                continue

            if informed_pattern_id != (
                pattern_id
            ):

                logger.debug(
                    "Public transport customer {} "
                    "ignored vehicle for pattern {} "
                    "while waiting for {}.".format(
                        self.agent.name,
                        informed_pattern_id,
                        pattern_id
                    )
                )

                continue

            if vehicle_id is None:

                logger.warning(
                    "Public transport customer {} "
                    "received vehicle availability "
                    "without vehicle_id at stop {}.".format(
                        self.agent.name,
                        origin_stop_id
                    )
                )

                continue

            sent = (
                await self.request_boarding(
                    vehicle_id,
                    leg
                )
            )

            if not sent:

                logger.warning(
                    "Public transport customer {} "
                    "could not request boarding to "
                    "vehicle {}.".format(
                        self.agent.name,
                        vehicle_id
                    )
                )

                continue

            self.agent.set_current_vehicle(
                vehicle_id
            )

            logger.info(
                "Public transport customer {} "
                "requesting boarding to vehicle {} "
                "at stop {} for pattern {} "
                "towards {}.".format(
                    self.agent.name,
                    vehicle_id,
                    origin_stop_id,
                    pattern_id,
                    destination_stop_id
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_FOR_APPROVAL
            )

            return


class PublicTransportCustomerWaitingForApprovalState(
    PublicTransportCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            CUSTOMER_WAITING_FOR_APPROVAL
        )

    def _bare_jid(self, jid):
        if jid is None:
            return None

        return str(jid).split("/")[0]

    async def run(self):

        leg = self.agent.get_current_leg()

        if leg is None:

            self.agent.clear_current_vehicle()

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if leg.get("type") != "public_transport":

            self.agent.clear_current_vehicle()

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        vehicle_id = (
            self.agent.get_current_vehicle()
        )

        if vehicle_id is None:

            self.set_next_state(
                CUSTOMER_WAITING
            )

            return

        pattern_id = (
            leg.get("pattern_id")
        )

        origin_stop_id = self.get_stop_id(
            leg.get("origin_stop")
        )

        destination_stop_id = self.get_stop_id(
            leg.get("destination_stop")
        )

        if (
            pattern_id is None
            or origin_stop_id is None
            or destination_stop_id is None
        ):

            logger.error(
                "Public transport customer {} "
                "has an invalid public transport leg "
                "while waiting for boarding approval.".format(
                    self.agent.name
                )
            )

            self.agent.clear_current_vehicle()

            self.set_next_state(
                CUSTOMER_WAITING
            )

            return

        expected_vehicle = self._bare_jid(
            vehicle_id
        )

        while True:

            msg = await self.receive(
                timeout=3600
            )

            if not msg:
                continue

            protocol = msg.get_metadata(
                "protocol"
            )

            performative = msg.get_metadata(
                "performative"
            )

            sender = self._bare_jid(
                msg.sender
            )

            if (
                protocol != REQUEST_PROTOCOL
                or sender != expected_vehicle
            ):
                continue

            if performative not in (
                ACCEPT_PERFORMATIVE,
                REFUSE_PERFORMATIVE,
            ):
                continue

            try:

                content = (
                    json.loads(msg.body)
                    if msg.body
                    else {}
                )

            except (
                json.JSONDecodeError,
                TypeError
            ):

                logger.error(
                    "Public transport customer {} "
                    "received an invalid boarding "
                    "response from vehicle {}.".format(
                        self.agent.name,
                        expected_vehicle
                    )
                )

                continue

            if (
                content.get("request_type")
                != "public_transport_board"
            ):
                continue

            if (
                content.get("pattern_id")
                != pattern_id

                or content.get("origin_stop")
                != origin_stop_id

                or content.get("destination_stop")
                != destination_stop_id
            ):

                logger.warning(
                    "Public transport customer {} "
                    "received a boarding response from "
                    "{} that does not match the current "
                    "leg.".format(
                        self.agent.name,
                        expected_vehicle
                    )
                )

                continue

            if (
                performative
                == ACCEPT_PERFORMATIVE
            ):

                logger.info(
                    "Public transport customer {} "
                    "boarded vehicle {} at stop {} "
                    "on pattern {} towards {}.".format(
                        self.agent.name,
                        expected_vehicle,
                        origin_stop_id,
                        pattern_id,
                        destination_stop_id
                    )
                )

                #
                # Public Transport Statistics
                #
                # Customer-side confirmation of boarding.
                #
                # This event is emitted only after receiving
                # ACCEPT from the Vehicle.
                #

                self.agent.events_store.emit(
                    event_type="pt_boarded",
                    details={
                        "vehicle_id":
                            expected_vehicle,

                        "pattern_id":
                            pattern_id,

                        "route_id":
                            leg.get("route_id"),

                        "mode":
                            leg.get("mode"),

                        "origin_stop":
                            origin_stop_id,

                        "destination_stop":
                            destination_stop_id,

                        "leg_index":
                            self.agent.current_leg_index,
                    }
                )

                self.agent.set_current_vehicle(
                    vehicle_id
                )

                self.agent.clear_waiting_pattern_id()

                self.agent.clear_current_stop()

                self.set_next_state(
                    CUSTOMER_IN_TRANSPORT
                )

                return

            if (
                performative
                == REFUSE_PERFORMATIVE
            ):

                logger.info(
                    "Public transport customer {} "
                    "was refused boarding by vehicle {} "
                    "at stop {} on pattern {}.".format(
                        self.agent.name,
                        expected_vehicle,
                        origin_stop_id,
                        pattern_id
                    )
                )

                self.agent.clear_current_vehicle()

                self.set_next_state(
                    CUSTOMER_WAITING
                )

                return


class PublicTransportCustomerInTransportState(
    PublicTransportCustomerStrategyBehaviour
):

    async def on_start(self):
        await super().on_start()

        self.agent.status = (
            CUSTOMER_IN_TRANSPORT
        )

    def _bare_jid(self, jid):
        if jid is None:
            return None

        return str(jid).split("/")[0]

    async def run(self):

        leg = self.agent.get_current_leg()

        if leg is None:

            logger.error(
                "Public transport customer {} "
                "has no current leg while travelling.".format(
                    self.agent.name
                )
            )

            self.agent.clear_current_vehicle()

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if leg.get("type") != "public_transport":

            logger.error(
                "Public transport customer {} "
                "is in transport but current leg "
                "type is {}.".format(
                    self.agent.name,
                    leg.get("type")
                )
            )

            self.agent.clear_current_vehicle()

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        vehicle_id = (
            self.agent.get_current_vehicle()
        )

        if vehicle_id is None:

            logger.error(
                "Public transport customer {} "
                "is in transport without "
                "current_vehicle.".format(
                    self.agent.name
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        pattern_id = (
            leg.get("pattern_id")
        )

        origin_stop = (
            leg.get("origin_stop")
        )

        destination_stop = (
            leg.get("destination_stop")
        )

        origin_stop_id = self.get_stop_id(
            origin_stop
        )

        destination_stop_id = self.get_stop_id(
            destination_stop
        )

        if (
            pattern_id is None
            or origin_stop_id is None
            or destination_stop_id is None
        ):

            logger.error(
                "Public transport customer {} "
                "has an invalid public transport leg "
                "while travelling.".format(
                    self.agent.name
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        expected_vehicle = self._bare_jid(
            vehicle_id
        )

        while True:

            msg = await self.receive(
                timeout=3600
            )

            if not msg:
                continue

            protocol = msg.get_metadata(
                "protocol"
            )

            performative = msg.get_metadata(
                "performative"
            )

            sender = self._bare_jid(
                msg.sender
            )

            if (
                protocol != REQUEST_PROTOCOL
                or performative
                != INFORM_PERFORMATIVE
            ):
                continue

            if sender != expected_vehicle:
                continue

            try:

                content = (
                    json.loads(msg.body)
                    if msg.body
                    else {}
                )

            except (
                json.JSONDecodeError,
                TypeError
            ):

                logger.warning(
                    "Public transport customer {} "
                    "received invalid transport message "
                    "from vehicle {}.".format(
                        self.agent.name,
                        expected_vehicle
                    )
                )

                continue

            if (
                content.get("request_type")
                != "public_transport_arrival"
            ):
                continue

            informed_vehicle = (
                self._bare_jid(
                    content.get("vehicle_id")
                )
            )

            informed_pattern = (
                content.get("pattern_id")
            )

            informed_stop = (
                content.get("stop")
            )

            if (
                informed_vehicle
                != expected_vehicle
            ):

                logger.warning(
                    "Public transport customer {} "
                    "received arrival for vehicle {} "
                    "while travelling in {}.".format(
                        self.agent.name,
                        informed_vehicle,
                        expected_vehicle
                    )
                )

                continue

            if (
                informed_pattern
                != pattern_id
            ):

                logger.warning(
                    "Public transport customer {} "
                    "received arrival for pattern {} "
                    "while travelling on {}.".format(
                        self.agent.name,
                        informed_pattern,
                        pattern_id
                    )
                )

                continue

            if (
                informed_stop
                != destination_stop_id
            ):

                logger.debug(
                    "Public transport customer {} "
                    "ignored arrival at stop {} while "
                    "travelling towards {}.".format(
                        self.agent.name,
                        informed_stop,
                        destination_stop_id
                    )
                )

                continue

            destination_position = (
                destination_stop.get(
                    "position"
                )
            )

            if (
                destination_position
                is not None
            ):

                await self.agent.set_position(
                    destination_position
                )

            self.agent.set_current_stop(
                destination_stop_id
            )

            #
            # Public Transport Statistics
            #
            # Customer-side confirmation that the
            # Public Transport leg has finished.
            #
            # IMPORTANT:
            # current_leg_index is recorded BEFORE
            # advance_leg().
            #

            self.agent.events_store.emit(
                event_type="pt_alighted",
                details={
                    "vehicle_id":
                        expected_vehicle,

                    "pattern_id":
                        pattern_id,

                    "route_id":
                        leg.get("route_id"),

                    "mode":
                        leg.get("mode"),

                    "origin_stop":
                        origin_stop_id,

                    "destination_stop":
                        destination_stop_id,

                    "leg_index":
                        self.agent.current_leg_index,
                }
            )

            self.agent.clear_current_vehicle()

            self.agent.clear_waiting_pattern_id()

            self.agent.advance_leg()

            logger.info(
                "Public transport customer {} "
                "left vehicle {} at stop {} "
                "after completing pattern {}.".format(
                    self.agent.name,
                    expected_vehicle,
                    destination_stop_id,
                    pattern_id
                )
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return


class PublicTransportCustomerInDestState(
    PublicTransportCustomerStrategyBehaviour
):
    """
    Final state for a successfully completed
    Public Transport journey.

    No next state is configured.
    Therefore the Public Transport Customer
    FSM finishes here.
    """

    async def on_start(self):

        await super().on_start()

        self.agent.status = (
            CUSTOMER_IN_DEST
        )

        journey = (
            self.agent.get_journey()
            or {}
        )

        logger.info(
            "Public transport customer {} "
            "completed its journey at {}.".format(
                self.agent.name,
                self.agent.get_position()
            )
        )

        #
        # Public Transport Statistics
        #
        # This event is emitted exactly once because
        # CUSTOMER_IN_DEST is a real terminal FSM state.
        #

        self.agent.events_store.emit(
            event_type="pt_journey_completed",
            details={
                "final_position":
                    self.agent.get_position(),

                "transfers":
                    journey.get(
                        "transfers",
                        0
                    ),

                "walking_distance":
                    journey.get(
                        "walking_distance",
                        0
                    ),

                "public_transport_stops":
                    journey.get(
                        "public_transport_stops",
                        0
                    ),

                "legs":
                    len(
                        journey.get(
                            "legs",
                            []
                        )
                    ),
            }
        )

    async def run(self):

        self.agent.clear_current_vehicle()

        self.agent.clear_waiting_pattern_id()

        self.agent.pedestrian_dest = None

        #
        # Intentionally no set_next_state().
        #
        # CUSTOMER_IN_DEST remains a real terminal
        # state and the FSM finishes here.
        #

        return


class FSMPublicTransportCustomerStrategyBehaviour(
    FSMSimfleetBehaviour
):
    """
    Finite-state strategy for multimodal Public Transport
    customers.
    """

    def setup(
        self
    ):

        #
        # States
        #

        self.add_state(
            CUSTOMER_WAITING_TO_MOVE,
            PublicTransportCustomerWaitingToMoveState(),
            initial=True
        )

        self.add_state(
            CUSTOMER_WAITING_FOR_JOURNEY,
            PublicTransportCustomerWaitingForJourneyState()
        )

        self.add_state(
            CUSTOMER_MOVING_TO_DEST,
            PublicTransportCustomerMovingToDestState()
        )

        self.add_state(
            CUSTOMER_IN_STOP,
            PublicTransportCustomerInStopState()
        )

        self.add_state(
            CUSTOMER_WAITING,
            PublicTransportCustomerWaitingState()
        )

        self.add_state(
            CUSTOMER_WAITING_FOR_APPROVAL,
            PublicTransportCustomerWaitingForApprovalState()
        )

        self.add_state(
            CUSTOMER_IN_TRANSPORT,
            PublicTransportCustomerInTransportState()
        )

        self.add_state(
            CUSTOMER_IN_DEST,
            PublicTransportCustomerInDestState()
        )

        self.add_state(
            CUSTOMER_JOURNEY_FAILED,
            PublicTransportCustomerJourneyFailedState()
        )

        #
        # CUSTOMER_WAITING_TO_MOVE
        #

        self.add_transition(
            CUSTOMER_WAITING_TO_MOVE,
            CUSTOMER_WAITING_TO_MOVE
        )

        self.add_transition(
            CUSTOMER_WAITING_TO_MOVE,
            CUSTOMER_WAITING_FOR_JOURNEY
        )

        self.add_transition(
            CUSTOMER_WAITING_TO_MOVE,
            CUSTOMER_MOVING_TO_DEST
        )

        self.add_transition(
            CUSTOMER_WAITING_TO_MOVE,
            CUSTOMER_IN_STOP
        )

        self.add_transition(
            CUSTOMER_WAITING_TO_MOVE,
            CUSTOMER_IN_DEST
        )

        #
        # CUSTOMER_WAITING_FOR_JOURNEY
        #

        self.add_transition(
            CUSTOMER_WAITING_FOR_JOURNEY,
            CUSTOMER_WAITING_TO_MOVE
        )

        self.add_transition(
            CUSTOMER_WAITING_FOR_JOURNEY,
            CUSTOMER_JOURNEY_FAILED
        )

        #
        # CUSTOMER_MOVING_TO_DEST
        #

        self.add_transition(
            CUSTOMER_MOVING_TO_DEST,
            CUSTOMER_MOVING_TO_DEST
        )

        self.add_transition(
            CUSTOMER_MOVING_TO_DEST,
            CUSTOMER_WAITING_TO_MOVE
        )

        #
        # CUSTOMER_IN_STOP
        #

        self.add_transition(
            CUSTOMER_IN_STOP,
            CUSTOMER_IN_STOP
        )

        self.add_transition(
            CUSTOMER_IN_STOP,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_IN_STOP,
            CUSTOMER_WAITING_TO_MOVE
        )

        #
        # CUSTOMER_WAITING
        #

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING_FOR_APPROVAL
        )

        self.add_transition(
            CUSTOMER_WAITING,
            CUSTOMER_WAITING_TO_MOVE
        )

        #
        # CUSTOMER_WAITING_FOR_APPROVAL
        #

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
            CUSTOMER_IN_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_WAITING_FOR_APPROVAL,
            CUSTOMER_WAITING_TO_MOVE
        )

        #
        # CUSTOMER_IN_TRANSPORT
        #

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_IN_TRANSPORT
        )

        self.add_transition(
            CUSTOMER_IN_TRANSPORT,
            CUSTOMER_WAITING_TO_MOVE
        )

        #
        # Successful terminal state
        #

        self.add_transition(
            CUSTOMER_IN_DEST,
            CUSTOMER_IN_DEST
        )

        #
        # Failed terminal state
        #

        self.add_transition(
            CUSTOMER_JOURNEY_FAILED,
            CUSTOMER_JOURNEY_FAILED
        )
