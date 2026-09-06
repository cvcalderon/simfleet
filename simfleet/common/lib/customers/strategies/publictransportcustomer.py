import math
from uuid import uuid4
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


    METRICS_MODALITY = "public_transport"
    _METRICS_SERVICE_CONTEXT_ATTR = "_metrics_service_context"
    _METRICS_PENDING_MOVEMENT_ATTR = "_metrics_pending_movement"
    _METRICS_MOVEMENT_PHASES = {"approach", "service", "auxiliary"}

    def _metrics_modality(self):
        modality = self.METRICS_MODALITY
        if modality is None:
            raise ValueError(
                "A concrete metrics strategy must declare METRICS_MODALITY."
            )
        return modality

    def get_service_context(self):
        return getattr(
            self.agent,
            self._METRICS_SERVICE_CONTEXT_ATTR,
            None,
        )

    def create_service_context(
        self,
        service_id=None,
        user_id=None,
        transport_id=None,
        origin=None,
        destination=None,
        emit_requested=False,
    ):
        current = self.get_service_context()
        if current is not None:
            requested_id = str(service_id) if service_id is not None else None
            if (
                current.get("terminal_status") is None
                and (
                    requested_id is None
                    or current.get("service_id") == requested_id
                )
            ):
                return current
            logger.warning(
                "Agent[{}]: Refusing to replace metrics service context [{}] "
                "with [{}] before it is cleared.".format(
                    self.agent.name,
                    current.get("service_id"),
                    requested_id,
                )
            )
            return None

        if service_id is None:
            if not emit_requested:
                logger.warning(
                    "Agent[{}]: A transport-side metrics context requires "
                    "the customer-created service_id.".format(self.agent.name)
                )
                return None
            service_id = str(uuid4())
        else:
            service_id = str(service_id)

        if not emit_requested and user_id is None:
            logger.warning(
                "Agent[{}]: A transport-side metrics context requires user_id.".format(
                    self.agent.name
                )
            )
            return None

        context = {
            "service_id": service_id,
            "modality": self._metrics_modality(),
            "user_id": self.agent.bare_jid(
                user_id if user_id is not None else self.agent.jid
            ),
            "transport_id": self.agent.bare_jid(transport_id),
            "origin": origin,
            "destination": destination,
            "requested": True,
            "request_emitted": False,
            "assigned": False,
            "transport_ids": [],
            "assignment_keys": set(),
            "started": False,
            "terminal_status": None,
            "pending_movement": None,
        }
        setattr(
            self.agent,
            self._METRICS_SERVICE_CONTEXT_ATTR,
            context,
        )

        if emit_requested:
            details = self._service_event_details(context)
            details["origin"] = origin
            details["destination"] = destination
            self.agent.events_store.emit(
                event_type="service_requested",
                details=details,
            )
            context["request_emitted"] = True

        return context

    def get_or_create_service_context(self, **kwargs):
        context = self.get_service_context()
        service_id = kwargs.get("service_id")
        if context is not None:
            if (
                service_id is None
                or context.get("service_id") == str(service_id)
            ):
                return context
            logger.warning(
                "Agent[{}]: Message/service context mismatch: [{}] != [{}].".format(
                    self.agent.name,
                    context.get("service_id"),
                    service_id,
                )
            )
            return None
        return self.create_service_context(**kwargs)

    def clear_service_context(self):
        context = self.get_service_context()
        if context is None:
            return True
        if context.get("terminal_status") is None:
            logger.warning(
                "Agent[{}]: Refusing to clear unfinished metrics service [{}].".format(
                    self.agent.name,
                    context.get("service_id"),
                )
            )
            return False
        if context.get("pending_movement") is not None:
            logger.warning(
                "Agent[{}]: Refusing to clear metrics service [{}] with a "
                "pending movement.".format(
                    self.agent.name,
                    context.get("service_id"),
                )
            )
            return False
        setattr(
            self.agent,
            self._METRICS_SERVICE_CONTEXT_ATTR,
            None,
        )
        return True

    def _service_event_details(self, context=None):
        context = context or self.get_service_context()
        if context is None:
            return None
        return {
            "modality": context["modality"],
            "service_id": context["service_id"],
            "user_id": context.get("user_id"),
            "transport_id": context.get("transport_id"),
        }

    def add_service_identifiers(
        self,
        content=None,
        context=None,
        transport_id=None,
    ):
        context = context or self.get_service_context()
        if context is None:
            raise ValueError("Cannot propagate identifiers without a service context.")
        result = dict(content or {})
        if transport_id is not None:
            context["transport_id"] = self.agent.bare_jid(transport_id)
        result.update(self._service_event_details(context))
        return result

    def message_matches_service(self, content, context=None):
        context = context or self.get_service_context()
        if context is None or not isinstance(content, dict):
            return False

        for key in ("service_id", "modality", "user_id"):
            if content.get(key) is None:
                return False

        if str(content["service_id"]) != context["service_id"]:
            return False
        if content["modality"] != context["modality"]:
            return False
        if self.agent.bare_jid(content["user_id"]) != context.get("user_id"):
            return False

        expected_transport = context.get("transport_id")
        received_transport = content.get("transport_id")
        if expected_transport is not None:
            if received_transport is None:
                return False
            if self.agent.bare_jid(received_transport) != expected_transport:
                return False
        return True

    def mark_service_assigned(
        self,
        transport_id,
        boarding_key=None,
    ):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False

        transport_id = self.agent.bare_jid(
            transport_id
        )
        if transport_id is None:
            return False

        key = str(
            boarding_key
            if boarding_key is not None
            else transport_id
        )

        if key in context["assignment_keys"]:
            context["transport_id"] = transport_id
            return False

        context["assignment_keys"].add(
            key
        )

        if transport_id not in context["transport_ids"]:
            context["transport_ids"].append(
                transport_id
            )

        context["transport_id"] = transport_id
        context["assigned"] = True
        return True

    def clear_candidate_transport(self, transport_id=None):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        if transport_id is not None:
            transport_id = self.agent.bare_jid(transport_id)
            if context.get("transport_id") != transport_id:
                return False
        context["transport_id"] = None
        return True

    def mark_service_started(self, transport_id=None):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        if transport_id is not None:
            transport_id = self.agent.bare_jid(transport_id)
            if (
                context.get("transport_id") is not None
                and context.get("transport_id") != transport_id
            ):
                return False
            context["transport_id"] = transport_id
        if context.get("transport_id") is None:
            return False
        context["assigned"] = True
        context["started"] = True
        return True

    def assign_service(self, transport_id, extra_details=None):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        transport_id = self.agent.bare_jid(transport_id)
        if transport_id is None:
            return False
        if context.get("assigned"):
            return False

        context["transport_id"] = transport_id
        context["assigned"] = True
        details = self._service_event_details(context)
        details.update(extra_details or {})
        self.agent.events_store.emit(
            event_type="service_assigned",
            details=details,
        )
        return True

    def start_service(self, transport_id=None, extra_details=None):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        if context.get("started"):
            return False
        if transport_id is not None:
            context["transport_id"] = self.agent.bare_jid(transport_id)
        if context.get("transport_id") is None:
            return False

        context["started"] = True
        details = self._service_event_details(context)
        details.update(extra_details or {})
        self.agent.events_store.emit(
            event_type="service_started",
            details=details,
        )
        return True

    def complete_service(self, extra_details=None):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        if not context.get("started"):
            logger.warning(
                "Agent[{}]: Refusing to complete service [{}] before it starts.".format(
                    self.agent.name,
                    context.get("service_id"),
                )
            )
            return False

        context["terminal_status"] = "completed"
        details = self._service_event_details(context)
        details.update(extra_details or {})
        self.agent.events_store.emit(
            event_type="service_completed",
            details=details,
        )
        return True

    def fail_service(self, failure_reason=None, extra_details=None):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False

        context["terminal_status"] = "failed"
        details = self._service_event_details(context)
        if failure_reason is not None:
            details["failure_reason"] = failure_reason
        details.update(extra_details or {})
        self.agent.events_store.emit(
            event_type="service_failed",
            details=details,
        )
        return True

    def set_pending_movement(
        self,
        phase,
        distance_m,
        extra_details=None,
        require_service=True,
    ):
        if phase not in self._METRICS_MOVEMENT_PHASES:
            raise ValueError("Invalid metrics movement phase: {}".format(phase))
        if (
            isinstance(distance_m, bool)
            or not isinstance(distance_m, (int, float))
            or not math.isfinite(distance_m)
            or distance_m < 0
        ):
            raise ValueError("distance_m must be a finite non-negative number.")

        context = self.get_service_context()
        if require_service and context is None:
            return False
        if getattr(self.agent, self._METRICS_PENDING_MOVEMENT_ATTR, None) is not None:
            logger.warning(
                "Agent[{}]: Refusing to overwrite a pending metrics movement.".format(
                    self.agent.name
                )
            )
            return False

        details = {
            "modality": self._metrics_modality(),
            "transport_id": None,
            "user_id": None,
            "service_id": None,
            "phase": phase,
            "distance_m": float(distance_m),
        }
        if context is not None:
            details.update(self._service_event_details(context))
        else:
            details["transport_id"] = self.agent.bare_jid(self.agent.jid)
        details.update(extra_details or {})

        pending = {"details": details}
        setattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            pending,
        )
        if context is not None:
            context["pending_movement"] = pending
        return True

    def complete_pending_movement(self):
        pending = getattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            None,
        )
        if pending is None:
            return False

        self.agent.events_store.emit(
            event_type="movement_completed",
            details=dict(pending["details"]),
        )
        context = self.get_service_context()
        if context is not None and context.get("pending_movement") is pending:
            context["pending_movement"] = None
        setattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            None,
        )
        return True

    def discard_pending_movement(self):
        pending = getattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            None,
        )
        if pending is None:
            return False
        context = self.get_service_context()
        if context is not None and context.get("pending_movement") is pending:
            context["pending_movement"] = None
        setattr(
            self.agent,
            self._METRICS_PENDING_MOVEMENT_ATTR,
            None,
        )
        return True

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

        context = self.get_service_context()

        if context is None:
            context = self.create_service_context(
                origin=origin,
                destination=destination,
                emit_requested=True,
            )

        if context is None:
            return False

        content = self.add_service_identifiers(
            {
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
            },
            context=context,
        )

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

        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
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

        boarding_key = "{}:{}:{}:{}".format(
            self.agent.current_leg_index,
            pattern_id,
            origin_stop,
            destination_stop,
        )

        content = self.add_service_identifiers(
            {
                "request_type":
                    "public_transport_board",

                "pattern_id":
                    pattern_id,

                "origin_stop":
                    origin_stop,

                "destination_stop":
                    destination_stop,

                "leg_index":
                    self.agent.current_leg_index,

                "boarding_key":
                    boarding_key,
            },
            context=context,
            transport_id=vehicle_id,
        )

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

        self.discard_pending_movement()

        self.fail_service(
            "no_feasible_journey",
            extra_details={
                "position": self.agent.get_position(),
                "leg_index": self.agent.current_leg_index,
            },
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

        context = self.get_service_context()

        if context is None:
            logger.error(
                "Public transport customer {} has no metrics service "
                "context while executing a walking leg.".format(
                    self.agent.name
                )
            )
            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )
            return

        phase = (
            "service"
            if context.get("started")
            else "approach"
        )

        movement_details = {
            "transport_id": None,
            "leg_index": self.agent.current_leg_index,
            "movement_mode": "walking",
        }

        self.agent.pedestrian_dest = (
            destination
        )

        if self.agent.get_position() == (
            destination
        ):

            if not self.complete_pending_movement():
                self.set_pending_movement(
                    phase,
                    0,
                    extra_details=movement_details,
                )
                self.complete_pending_movement()

            self.complete_walking_leg(
                leg
            )

            self.set_next_state(
                CUSTOMER_WAITING_TO_MOVE
            )

            return

        if self.agent.dest == (
            destination
        ):

            if self.agent.is_in_destination():

                if not self.complete_pending_movement():
                    self.set_pending_movement(
                        phase,
                        0,
                        extra_details=movement_details,
                    )
                    self.complete_pending_movement()

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

        try:

            distance, _, _ = await self.agent.move_to(
                destination
            )

            self.set_pending_movement(
                phase,
                distance,
                extra_details=movement_details,
            )

        except AlreadyInDestination:

            self.agent.dest = (
                destination
            )

            if not self.complete_pending_movement():
                self.set_pending_movement(
                    phase,
                    0,
                    extra_details=movement_details,
                )
                self.complete_pending_movement()

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

            self.discard_pending_movement()

            await self.agent.sleep(
                2
            )

            self.set_next_state(
                CUSTOMER_MOVING_TO_DEST
            )

            return

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
            self.clear_candidate_transport()

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

            if not self.message_matches_service(
                content
            ):
                logger.warning(
                    "Public transport customer {} ignored a stale or "
                    "uncorrelated boarding response from {}.".format(
                        self.agent.name,
                        expected_vehicle,
                    )
                )
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

                boarding_key = content.get(
                    "boarding_key"
                )

                self.mark_service_assigned(
                    expected_vehicle,
                    boarding_key=boarding_key,
                )

                self.start_service(
                    expected_vehicle,
                    extra_details={
                        "pattern_id": pattern_id,
                        "route_id": leg.get("route_id"),
                        "mode": leg.get("mode"),
                        "origin_stop": origin_stop_id,
                        "destination_stop": destination_stop_id,
                        "leg_index": self.agent.current_leg_index,
                    },
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

                self.clear_candidate_transport(
                    expected_vehicle
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

            if not self.message_matches_service(
                content
            ):
                logger.warning(
                    "Public transport customer {} ignored a stale or "
                    "uncorrelated arrival from {}.".format(
                        self.agent.name,
                        expected_vehicle,
                    )
                )
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

            if informed_vehicle != expected_vehicle:
                continue

            if informed_pattern != pattern_id:
                continue

            if informed_stop != destination_stop_id:
                continue

            destination_position = (
                destination_stop.get(
                    "position"
                )
            )

            if destination_position is not None:
                await self.agent.set_position(
                    destination_position
                )

            self.agent.set_current_stop(
                destination_stop_id
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

        self.complete_pending_movement()

        self.complete_service(
            extra_details={
                "final_position": self.agent.get_position(),
                "transfers": journey.get("transfers", 0),
                "walking_distance_m": journey.get("walking_distance", 0),
                "public_transport_stops": journey.get("public_transport_stops", 0),
                "legs": len(journey.get("legs", [])),
            },
        )

    async def run(self):

        self.agent.clear_current_vehicle()

        self.agent.clear_waiting_pattern_id()

        self.agent.pedestrian_dest = None

        self.clear_service_context()

        return


class FSMPublicTransportCustomerStrategyBehaviour(
    FSMSimfleetBehaviour
):
    """
    Finite-state strategy for multimodal Public Transport
    customers.
    """

    async def on_end(self):
        """
        Finalize the PublicTransport strategy and notify customer orchestration.
        """
        await super().on_end()
        self.agent.notify_modal_completion()

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
