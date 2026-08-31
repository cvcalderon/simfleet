import math
from uuid import uuid4
import json

from loguru import logger
from simfleet.utils.abstractstrategies import FSMSimfleetBehaviour

from spade.behaviour import State
from spade.message import Message

#from simfleet.common.lib.transports.models.electrictaxi import ElectricTaxiStrategyBehaviour
from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    REQUEST_PERFORMATIVE,
    INFORM_PERFORMATIVE,
    CANCEL_PERFORMATIVE,
    ACCEPT_PERFORMATIVE,
    REFUSE_PERFORMATIVE,
    PROPOSE_PERFORMATIVE
)

from simfleet.utils.helpers import (
    PathRequestException,
    AlreadyInDestination
)
from simfleet.utils.status import TRANSPORT_WAITING, TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_MOVING_TO_CUSTOMER, \
    TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_IN_CUSTOMER_PLACE, TRANSPORT_MOVING_TO_DESTINATION, \
    TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_MOVING_TO_STATION, TRANSPORT_IN_STATION_PLACE, \
    TRANSPORT_IN_WAITING_LIST, TRANSPORT_NEEDS_CHARGING, TRANSPORT_CHARGING, CUSTOMER_IN_TRANSPORT, CUSTOMER_IN_DEST, \
    TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_MOVING_TO_RETURN


# ==================================================================
# ---------------------- Strategy Behaviour ------------------------
# ==================================================================

class ElectricTaxiStrategyBehaviour(State):
    """
    Base class to define the transport strategy for an electric taxi.
    This class should be inherited and extended to create custom strategies.
    Subclasses must override the `run` coroutine to define specific behaviors.

    Methods:
        async on_start():
            Logs the beginning of the strategy execution.
        async on_end():
            Logs the end of the strategy execution.
        async go_to_the_station(station_id, dest):
            Directs the taxi to a specific station and updates autonomy based on distance.
        check_and_decrease_autonomy(customer_orig, customer_dest):
            Checks if there is enough autonomy for a trip and decreases it if possible.
        async drop_station():
            Resets the current station assignment for the taxi.
        async request_access_station(station_id, content):
            Sends a request to a station for access.
        async send_proposal(customer_id, content=None):
            Sends a transport proposal to a customer.
        async cancel_proposal(agent_id, content=None):
            Cancels a previously sent proposal to a customer.
        async run():
            Abstract method that must be implemented by subclasses.
    """


    METRICS_MODALITY = "electric_taxi"
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

    def mark_service_assigned(self, transport_id):
        context = self.get_service_context()
        if context is None or context.get("terminal_status") is not None:
            return False
        transport_id = self.agent.bare_jid(transport_id)
        if transport_id is None:
            return False
        if context.get("assigned"):
            return context.get("transport_id") == transport_id
        context["transport_id"] = transport_id
        context["assigned"] = True
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

    def mark_service_completed(self):
        """Mirror the successful terminal event emitted by the customer."""
        context = self.get_service_context()
        if context is None:
            return False
        if context.get("terminal_status") is not None:
            return context.get("terminal_status") == "completed"
        if not context.get("started"):
            return False
        context["terminal_status"] = "completed"
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


    _METRICS_CHARGING_CONTEXT_ATTR = "_metrics_charging_context"

    def get_charging_context(self):
        return getattr(
            self.agent,
            self._METRICS_CHARGING_CONTEXT_ATTR,
            None,
        )

    def create_charging_context(self, station_id, charging_id=None):
        current = self.get_charging_context()
        if current is not None:
            requested_id = str(charging_id) if charging_id is not None else None
            if (
                not current.get("completed")
                and (
                    requested_id is None
                    or current.get("charging_id") == requested_id
                )
            ):
                return current
            logger.warning(
                "Agent[{}]: Refusing to replace charging context [{}] before "
                "it is cleared.".format(
                    self.agent.name,
                    current.get("charging_id"),
                )
            )
            return None
        if station_id is None:
            return None
        context = {
            "charging_id": str(charging_id) if charging_id is not None else str(uuid4()),
            "modality": self.METRICS_MODALITY,
            "transport_id": self.agent.bare_jid(self.agent.jid),
            "station_id": self.agent.bare_jid(station_id),
            "arrived": False,
            "started": False,
            "completed": False,
        }
        setattr(
            self.agent,
            self._METRICS_CHARGING_CONTEXT_ATTR,
            context,
        )
        return context

    def clear_charging_context(self):
        context = self.get_charging_context()
        if context is None:
            return True
        if not context.get("completed"):
            logger.warning(
                "Agent[{}]: Refusing to clear unfinished charging session [{}].".format(
                    self.agent.name,
                    context.get("charging_id"),
                )
            )
            return False
        setattr(
            self.agent,
            self._METRICS_CHARGING_CONTEXT_ATTR,
            None,
        )
        return True

    def abandon_charging_context(self):
        """Drop an unfinished internal charging context without inventing a public failure.

        Any already-emitted charging milestones remain in the log and will therefore
        be reconstructed as an unfinished charging session. A later station attempt
        receives a new charging_id.
        """
        context = self.get_charging_context()
        if context is None:
            return True
        setattr(self.agent, self._METRICS_CHARGING_CONTEXT_ATTR, None)
        return True

    def _charging_event_details(self, context=None):
        context = context or self.get_charging_context()
        if context is None:
            return None
        return {
            "modality": context["modality"],
            "charging_id": context["charging_id"],
            "transport_id": context["transport_id"],
            "station_id": context["station_id"],
        }

    def charging_message_matches_context(
        self,
        content,
        sender=None
    ):
        """Validate that a charging message belongs to the active station."""

        context = self.get_charging_context()

        if context is None or not isinstance(content, dict):
            return False

        expected_station = context.get("station_id")

        #
        # The XMPP sender is the authoritative station identity.
        #
        if sender is not None:
            return (
                self.agent.bare_jid(sender)
                == expected_station
            )

        #
        # Backward-compatible fallback for messages where
        # sender is not available to the caller.
        #
        station_id = content.get("station_id")

        if station_id is None:
            return True

        return (
            self.agent.bare_jid(station_id)
            == expected_station
        )

    def charging_arrived(self):
        context = self.get_charging_context()
        if context is None or context.get("arrived"):
            return False
        context["arrived"] = True
        self.agent.events_store.emit(
            event_type="charging_arrived",
            details=self._charging_event_details(context),
        )
        return True

    def charging_started(self):
        context = self.get_charging_context()
        if context is None or context.get("started") or not context.get("arrived"):
            return False
        context["started"] = True
        self.agent.events_store.emit(
            event_type="charging_started",
            details=self._charging_event_details(context),
        )
        return True

    def charging_completed(self):
        context = self.get_charging_context()
        if context is None or context.get("completed") or not context.get("started"):
            return False
        context["completed"] = True
        self.agent.events_store.emit(
            event_type="charging_completed",
            details=self._charging_event_details(context),
        )
        return True


    _METRICS_PENDING_OFFER_ATTR = "_metrics_pending_offer"
    _METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR = "_metrics_active_message_context"

    def _validate_metrics_service_request(self, content):
        """Validate a strict schema-1.0 Taxi-like request before proposing."""
        if not isinstance(content, dict):
            return False
        for key in (
            "service_id",
            "modality",
            "user_id",
            "customer_id",
            "origin",
            "dest",
        ):
            if content.get(key) is None:
                return False
        if content.get("modality") != self._metrics_modality():
            return False
        if self.agent.bare_jid(content.get("user_id")) != self.agent.bare_jid(
            content.get("customer_id")
        ):
            return False
        if content.get("transport_id") is not None:
            return False
        return True

    def store_pending_offer(self, content):
        if not self._validate_metrics_service_request(content):
            return None
        pending = {
            "service_id": str(content["service_id"]),
            "modality": content["modality"],
            "user_id": self.agent.bare_jid(content["user_id"]),
            "customer_id": self.agent.bare_jid(content["customer_id"]),
            "transport_id": self.agent.bare_jid(self.agent.jid),
            "origin": content["origin"],
            "destination": content["dest"],
        }
        setattr(self.agent, self._METRICS_PENDING_OFFER_ATTR, pending)
        return pending

    def get_pending_offer(self):
        return getattr(self.agent, self._METRICS_PENDING_OFFER_ATTR, None)

    def clear_pending_offer(self):
        setattr(self.agent, self._METRICS_PENDING_OFFER_ATTR, None)
        return True

    def pending_offer_message_details(self):
        pending = self.get_pending_offer()
        if pending is None:
            return None
        return {
            "service_id": pending["service_id"],
            "modality": pending["modality"],
            "user_id": pending["user_id"],
            "transport_id": pending["transport_id"],
        }

    def message_matches_pending_offer(self, content):
        pending = self.get_pending_offer()
        if pending is None or not isinstance(content, dict):
            return False
        for key in ("service_id", "modality", "user_id", "transport_id", "customer_id"):
            if content.get(key) is None:
                return False
        return (
            str(content["service_id"]) == pending["service_id"]
            and content["modality"] == pending["modality"]
            and self.agent.bare_jid(content["user_id"]) == pending["user_id"]
            and self.agent.bare_jid(content["transport_id"]) == pending["transport_id"]
            and self.agent.bare_jid(content["customer_id"]) == pending["customer_id"]
        )

    def activate_pending_offer(self, content):
        if not self.message_matches_pending_offer(content):
            return None
        pending = dict(self.get_pending_offer())
        setattr(self.agent, self._METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR, pending)
        self.clear_pending_offer()
        return pending

    def get_active_message_context(self):
        return getattr(
            self.agent,
            self._METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR,
            None,
        )

    def active_service_message_details(self):
        active = self.get_active_message_context()
        if active is None:
            return None
        return {
            "service_id": active["service_id"],
            "modality": active["modality"],
            "user_id": active["user_id"],
            "transport_id": active["transport_id"],
        }

    def add_active_service_identifiers(self, content=None):
        details = self.active_service_message_details()
        if details is None:
            return dict(content or {})
        result = dict(content or {})
        result.update(details)
        return result

    def message_matches_active_service(self, content):
        active = self.get_active_message_context()
        if active is None or not isinstance(content, dict):
            return False
        for key in ("service_id", "modality", "user_id", "transport_id"):
            if content.get(key) is None:
                return False
        return (
            str(content["service_id"]) == active["service_id"]
            and content["modality"] == active["modality"]
            and self.agent.bare_jid(content["user_id"]) == active["user_id"]
            and self.agent.bare_jid(content["transport_id"]) == active["transport_id"]
        )

    def clear_active_message_context(self):
        setattr(self.agent, self._METRICS_ACTIVE_MESSAGE_CONTEXT_ATTR, None)
        return True

    async def on_start(self):
        """
                Logs the beginning of the strategy execution.
                """
        # await super().on_start()
        logger.debug(
            "Agent[{}]: Strategy {} started.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def on_end(self):
        """
                Logs the end of the strategy execution.
                """
        # await super().on_start()
        logger.debug(
            "Agent[{}]: Strategy {} finished.".format(
                self.agent.name, type(self).__name__
            )
        )

    async def go_to_the_station(self, station_id, dest):
        """
                Directs the taxi to a specific station and updates autonomy based on the distance.

                Args:
                    station_id (str): The ID of the destination station.
                    dest (list): The coordinates of the station (x, y).
                """
        logger.info(
            "Agent[{}]: On route to station [{}]".format(
                self.agent.name,
                station_id
            )
        )

        self.agent.set_current_station(
            station_id
        )

    def check_and_decrease_autonomy(
        self,
        customer_orig,
        customer_dest
    ):

        travel_km = self.agent.calculate_service_km(
            customer_orig,
            customer_dest
        )

        if not self.agent.has_enough_autonomy_km(
            travel_km
        ):
            return False

        self.agent.decrease_autonomy_km(
            travel_km
        )

        return True

    async def drop_station(self):
        """
        Resets the current station assignment for the transport.
        """

        logger.debug(
            "Agent[{}]: The agent has dropped the station [{}].".format(
                self.agent.agent_id,
                self.agent.get_current_station()
            )
        )

        self.agent.clear_current_station()
        self.agent.clear_nearby_station()

    async def request_access_station(self, station_id, content):

        """
                Sends a request to a station for access.

                Args:
                    station_id (str): The ID of the station to request access from.
                    content (dict): Additional information to include in the request.
                """

        if content is None:
            content = {}
        reply = Message()
        reply.to = station_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", REQUEST_PERFORMATIVE)
        reply.body = json.dumps(content)
        logger.debug(
            "Agent[{}]: The agent requesting access to [{}]".format(
                self.agent.name,
                station_id,
                reply.body
            )
        )
        await self.send(reply)

    async def send_proposal(self, customer_id, content=None):
        """
        Sends a proposal to a customer offering transport.

        Args:
            customer_id (str): The ID of the customer.
            content (dict, optional): Additional content for the proposal. Defaults to None.
        """
        if content is None:
            content = {}
        logger.info(
            "Agent[{}]: The agent sent proposal to agent [{}]".format(self.agent.name, customer_id)
        )
        reply = Message()
        reply.to = customer_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", PROPOSE_PERFORMATIVE)
        reply.body = json.dumps(content)
        await self.send(reply)

    async def cancel_proposal(self, agent_id, content=None):
        """
        Cancels a previously sent proposal.

        Args:
            agent_id (str): The ID of the customer.
            content (dict, optional): Additional content for the cancellation. Defaults to None.
        """
        if content is None:
            content = {}
        content = self.add_active_service_identifiers(content)
        logger.info(
            "Agent[{}]: The agent sent cancel proposal to agent [{}]".format(
                self.agent.name, agent_id
            )
        )
        reply = Message()
        reply.to = agent_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", CANCEL_PERFORMATIVE)
        reply.body = json.dumps(content)
        await self.send(reply)

    async def inform_customer(self, customer_id, status, data=None):
        """
        Sends a message to inform the customer of the transport's new status.

        Args:
            customer_id (str): The ID of the customer.
            status (int): The new status code.
            data (dict, optional): Additional information about the status.
        """
        if data is None:
            data = {}
        data = self.add_active_service_identifiers(data)
        msg = Message()
        msg.to = customer_id
        msg.set_metadata("protocol", REQUEST_PROTOCOL)
        msg.set_metadata("performative", INFORM_PERFORMATIVE)
        data["status"] = status
        msg.body = json.dumps(data)
        await self.send(msg)

    async def cancel_customer(self, customer_id, data=None):
        """
        Cancels the assignment of a customer and informs them via a message.

        Args:
            customer_id (str): The ID of the customer.
            data (dict, optional): Additional cancellation-related information.
        """
        logger.error(
            "Agent[{}]: The agent could not get a path to customer [{}].".format(
                self.agent.agent_id, self.agent.get("current_customer")
            )
        )
        if data is None:
            data = {}
        data = self.add_active_service_identifiers(data)
        reply = Message()
        reply.to = customer_id
        reply.set_metadata("protocol", REQUEST_PROTOCOL)
        reply.set_metadata("performative", CANCEL_PERFORMATIVE)
        reply.body = json.dumps(data)
        logger.debug(
            "Agent[{}]: The agent sent cancel proposal to customer [{}]".format(
                self.agent.agent_id, customer_id
            )
        )
        await self.send(reply)

    async def request_return_position(self):
        fleetmanager = self.agent.get_registration_fleet()

        if not fleetmanager:
            logger.warning(
                "Agent[{}]: No fleet manager configured for electric taxi return.".format(
                    self.agent.name
                )
            )
            return

        msg = Message()

        msg.to = str(fleetmanager)
        msg.set_metadata(
            "protocol",
            REQUEST_PROTOCOL
        )
        msg.set_metadata(
            "performative",
            REQUEST_PERFORMATIVE
        )

        msg.body = json.dumps(
            {
                "request_type": "taxi_return",
                "position": self.agent.get_position(),
            }
        )

        logger.debug(
            "Agent[{}]: Requesting return point from [{}] at position {}.".format(
                self.agent.name,
                fleetmanager,
                self.agent.get_position()
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
#              Point Of Return Electric Taxi Strategy          #
#                                                              #
################################################################

class ElectricTaxiWaitingState(ElectricTaxiStrategyBehaviour):
    """
        Represents the 'Waiting' state for the electric taxi. The taxi is waiting to receive a transport request.

        Methods:
            on_start(): Sets the initial state to 'TRANSPORT_WAITING' and logs the state.
            run(): Handles incoming messages, processes transport requests, and transitions to the next state.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING)
            return
        logger.debug("Agent[{}]: The agent received: {}".format(self.agent.jid, msg.body))
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")
        if performative == REQUEST_PERFORMATIVE:
            if self.store_pending_offer(content) is None:
                logger.warning(
                    "Agent[{}]: Ignoring request without valid schema-1.0 identifiers.".format(
                        self.agent.name
                    )
                )
                self.set_next_state(TRANSPORT_WAITING)
                return


            if not self.agent.has_enough_autonomy_for_service(
                content["origin"],
                content["dest"]
            ):


                await self.cancel_proposal(
                    content["customer_id"],
                    self.pending_offer_message_details(),
                )
                self.clear_pending_offer()
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return
            else:


                await self.send_proposal(
                    content["customer_id"],
                    self.pending_offer_message_details(),
                )
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
                return
        else:
            self.set_next_state(TRANSPORT_WAITING)
            return

class ElectricTaxiNeedsChargingState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_NEEDS_CHARGING
        self.agent.set_busy()

    async def run(self):
        if self.agent.get_stations() is None or self.agent.get_number_stations() < 1:
            logger.info(
                "Agent[{}]: The agent looking for a station.".format(
                    self.agent.name
                )
            )
            stations = await self.agent.get_list_agent_position(
                self.agent.service_type,
                self.agent.get_stations()
            )
            self.agent.set_stations(stations)

            if not stations:
                await self.agent.sleep(1)

            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

        nearby_station_dest = self.agent.nearst_agent(
            self.agent.get_stations(),
            self.agent.get_position()
        )

        if nearby_station_dest is None:
            logger.warning(
                "Agent[{}]: No charging station available.".format(
                    self.agent.name
                )
            )

            await self.agent.sleep(1)

            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

        self.agent.set_nearby_station(nearby_station_dest)
        station_id = self.agent.get_nearby_station_id()
        station_position = self.agent.get_nearby_station_position()
        logger.info("Agent[{}]: The agent selected station [{}].".format(self.agent.name, station_id))

        # A charging_id is independent from service_id. It is created before travel
        # so the same session is used for physical arrival/start/completion.
        if self.get_charging_context() is None:
            if self.create_charging_context(station_id) is None:
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return

        try:
            await self.go_to_the_station(station_id, station_position)
            travel_km = self.agent.calculate_distance_km(
                self.agent.get_position(), station_position
            )
            try:
                distance, _osrm_duration, _speed_based_duration = await self.agent.move_to(
                    station_position
                )
                if not self.set_pending_movement(
                    "auxiliary", distance, require_service=False
                ):
                    raise RuntimeError("Unable to register charging-station movement.")
                self.agent.decrease_autonomy_km(travel_km)
                self.agent.status = TRANSPORT_MOVING_TO_STATION
                self.set_next_state(TRANSPORT_MOVING_TO_STATION)
                return
            except AlreadyInDestination:
                self.set_pending_movement("auxiliary", 0, require_service=False)
                self.complete_pending_movement()
                self.charging_arrived()
                arguments = {
                    "transport_need": self.agent.max_autonomy_km - self.agent.current_autonomy_km
                }
                await self.request_access_station(
                    station_id,
                    {"service_name": self.agent.service_type, "object_type": "transport", "args": arguments},
                )
                self.agent.status = TRANSPORT_IN_STATION_PLACE
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
        except PathRequestException:
            logger.error("Agent[{}]: The agent could not get a path to station [{}].".format(self.agent.name, station_id))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name, e))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

class ElectricTaxiMovingToStationState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_STATION

    async def _arrive_and_request(self):
        station_id = self.agent.get_current_station()
        self.complete_pending_movement()
        if self.get_charging_context() is None:
            if self.create_charging_context(station_id) is None:
                return False
        self.charging_arrived()
        arguments = {
            "transport_need": self.agent.max_autonomy_km - self.agent.current_autonomy_km
        }
        await self.request_access_station(
            station_id,
            {"service_name": self.agent.service_type, "object_type": "transport", "args": arguments},
        )
        self.agent.status = TRANSPORT_IN_STATION_PLACE
        self.set_next_state(TRANSPORT_IN_STATION_PLACE)
        return True

    async def run(self):
        try:
            if not self.agent.is_in_destination():
                await self.agent.sleep(1)
                self.set_next_state(TRANSPORT_MOVING_TO_STATION)
                return
            await self._arrive_and_request()
            return
        except AlreadyInDestination:
            if not self.complete_pending_movement():
                self.set_pending_movement("auxiliary", 0, require_service=False)
            await self._arrive_and_request()
            return
        except PathRequestException:
            logger.error("Agent[{}]: The agent could not complete the path to charging station.".format(self.agent.name))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        except Exception as e:
            logger.error("Unexpected charging-route error in [{}]: {}".format(self.agent.name, e))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

class ElectricTaxiInStationState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_IN_STATION_PLACE

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_IN_STATION_PLACE)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_IN_STATION_PLACE)
            return
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                logger.warning("Agent[{}]: Ignoring charging-station ACCEPT with mismatched station.".format(self.agent.name))
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
            self.agent.status = TRANSPORT_IN_WAITING_LIST
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        if performative == REFUSE_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
            # The already-emitted arrival remains an unfinished charging session.
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        self.set_next_state(TRANSPORT_IN_STATION_PLACE)

class ElectricTaxiInWaitingListState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_IN_WAITING_LIST

    async def run(self):
        msg = await self.receive(timeout=5)
        if not msg:
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        performative = msg.get_metadata("performative")
        if performative == INFORM_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                return
            if content.get("serving"):
                if not self.charging_started():
                    logger.warning("Agent[{}]: Charging start milestone could not be emitted.".format(self.agent.name))
                    self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                    return
                self.agent.status = TRANSPORT_CHARGING
                self.set_next_state(TRANSPORT_CHARGING)
                return
        elif performative == REFUSE_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                return
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        self.set_next_state(TRANSPORT_IN_WAITING_LIST)

class ElectricTaxiChargingState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_CHARGING

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_CHARGING)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_CHARGING)
            return
        protocol = msg.get_metadata("protocol")
        performative = msg.get_metadata("performative")
        if protocol == REQUEST_PROTOCOL and performative == INFORM_PERFORMATIVE and content.get("charged"):
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_CHARGING)
                return
            if not self.charging_completed():
                self.set_next_state(TRANSPORT_CHARGING)
                return
            self.agent.increase_full_autonomy_km()
            await self.drop_station()
            self.clear_charging_context()
            if self.agent.has_return_position():
                logger.info("Agent[{}]: Charging completed. Continuing pending return to {}.".format(self.agent.name, self.agent.get_return_position()))
                self.agent.status = TRANSPORT_WAITING_FOR_RETURN
                self.agent.set_busy()
                self.set_next_state(TRANSPORT_WAITING_FOR_RETURN)
                return
            self.agent.status = TRANSPORT_WAITING
            self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING)
            return
        self.set_next_state(TRANSPORT_CHARGING)

class ElectricTaxiWaitingForApprovalState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING_FOR_APPROVAL

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            if not self.message_matches_pending_offer(content):
                logger.warning("Agent[{}]: Ignoring stale or mismatched acceptance.".format(self.agent.name))
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
                return
            active = self.activate_pending_offer(content)
            customer_id = content["customer_id"]

            travel_km = self.agent.calculate_service_km(
                content["origin"],
                content["dest"],
            )

            if not self.agent.has_enough_autonomy_km(travel_km):
                await self.cancel_proposal(customer_id)
                self.clear_active_message_context()
                self.agent.set_busy()
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return
            try:
                context = self.create_service_context(
                    service_id=active["service_id"], user_id=active["user_id"],
                    transport_id=active["transport_id"], origin=active["origin"],
                    destination=active["destination"], emit_requested=False,
                )
                if context is None or not self.assign_service(self.agent.jid):
                    raise RuntimeError("Unable to establish Electric Taxi metrics service context.")
                self.agent.add_assigned_customer(
                    customer_id=customer_id, origin=content["origin"], dest=content["dest"]
                )
                self.agent.set_busy()
                await self.inform_customer(customer_id=customer_id, status=TRANSPORT_MOVING_TO_CUSTOMER)
                distance, _osrm_duration, _speed_based_duration = (
                    await self.agent.move_to(content["origin"])
                )

                self.agent.decrease_autonomy_km(travel_km)

                if not self.set_pending_movement("approach", distance):
                    raise RuntimeError(
                        "Unable to register Electric Taxi approach movement."
                    )

                self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER
                self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
                return
            except AlreadyInDestination:
                self.agent.decrease_autonomy_km(travel_km)

                self.set_pending_movement("approach", 0)
                self.complete_pending_movement()

                await self.inform_customer(
                    customer_id=customer_id,
                    status=TRANSPORT_IN_CUSTOMER_PLACE,
                )

                self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
                self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
                return
            except PathRequestException:
                self.fail_service("approach_route_failed")
                self.discard_pending_movement()
                await self.cancel_proposal(customer_id, {"terminal_status":"failed","failure_reason":"approach_route_failed"})
                self.clear_service_context(); self.clear_active_message_context()
                if self.agent.get("assigned_customer"):
                    self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING; self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING); return
            except Exception as e:
                logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name, e))
                self.fail_service("approach_unexpected_error")
                self.discard_pending_movement()
                await self.cancel_proposal(customer_id, {"terminal_status":"failed","failure_reason":"approach_unexpected_error"})
                self.clear_service_context(); self.clear_active_message_context()
                if self.agent.get("assigned_customer"):
                    self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING; self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING); return
        elif performative == REFUSE_PERFORMATIVE:
            if not self.message_matches_pending_offer(content):
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL); return
            self.clear_pending_offer(); self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)

class ElectricTaxiMovingToCustomerState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start(); self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER

    async def run(self):
        customers = self.get("assigned_customer")
        if not customers:
            self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        customer_id = next(iter(customers.items()))[0]
        try:
            if not self.agent.is_in_destination():
                msg = await self.receive(timeout=2)
                if msg and msg.get_metadata("performative") == REFUSE_PERFORMATIVE:
                    try: content=json.loads(msg.body)
                    except (json.JSONDecodeError,TypeError):
                        self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
                    if not self.message_matches_active_service(content):
                        self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
                    self.fail_service("customer_cancelled_before_pickup")
                    self.discard_pending_movement()
                    await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"customer_cancelled_before_pickup"})
                    self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
                    self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
                self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
            self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=TRANSPORT_IN_CUSTOMER_PLACE)
            self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER; self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        except AlreadyInDestination:
            if not self.complete_pending_movement():
                self.set_pending_movement("approach",0); self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=TRANSPORT_IN_CUSTOMER_PLACE)
            self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER; self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        except PathRequestException:
            self.fail_service("approach_route_failed"); self.discard_pending_movement()
            await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"approach_route_failed"})
            self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
            self.fail_service("approach_unexpected_error"); self.discard_pending_movement()
            await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"approach_unexpected_error"})
            self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return

class ElectricTaxiArrivedAtCustomerState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER

    async def run(self):
        msg=await self.receive(timeout=60)
        if not msg: self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        try: content=json.loads(msg.body)
        except (json.JSONDecodeError,TypeError): self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        performative=msg.get_metadata("performative")
        if performative in (INFORM_PERFORMATIVE,CANCEL_PERFORMATIVE) and not self.message_matches_active_service(content):
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        if performative==INFORM_PERFORMATIVE and content.get("status")==CUSTOMER_IN_TRANSPORT:
            customers=self.get("assigned_customer")
            if not customers: self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
            customer_id = next(iter(customers.items()))[0]
            dest = next(iter(customers.items()))[1]["destination"]

            service_km = self.agent.calculate_distance_km(
                self.agent.get_position(),
                dest,
            )

            try:
                self.agent.add_customer_in_transport(
                    customer_id=customer_id,
                    dest=dest,
                )

                if not self.start_service(self.agent.jid):
                    raise RuntimeError(
                        "Unable to emit Electric Taxi service start."
                    )

                self.agent.remove_assigned_customer()

                distance, _osrm_duration, _speed_based_duration = (
                    await self.agent.move_to(dest)
                )

                if not self.set_pending_movement("service", distance):
                    raise RuntimeError(
                        "Unable to register Electric Taxi service movement."
                    )

                self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
                self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)
                return

            except AlreadyInDestination:
                self.set_pending_movement("service", 0)
                self.complete_pending_movement()

                await self.inform_customer(
                    customer_id=customer_id,
                    status=CUSTOMER_IN_DEST,
                )

                self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION
                self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
                return

            except PathRequestException:
                # The service leg never started physically. Restore the
                # autonomy that had been reserved for that unexecuted leg.
                self.agent.increase_autonomy_km(service_km)

                self.fail_service("service_route_failed")
                self.discard_pending_movement()

                await self.cancel_customer(
                    customer_id,
                    {
                        "terminal_status": "failed",
                        "failure_reason": "service_route_failed",
                    },
                )

                self.clear_service_context()
                self.clear_active_message_context()

                if customer_id in self.agent.get("current_customer"):
                    self.agent.remove_customer_in_transport(customer_id)

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING)
                return
            except Exception as e:
                logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
                self.fail_service("service_unexpected_error"); self.discard_pending_movement()
                await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_unexpected_error"})
                self.clear_service_context(); self.clear_active_message_context()
                if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
                self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        elif performative==CANCEL_PERFORMATIVE:
            self.fail_service("customer_cancelled_at_pickup"); self.discard_pending_movement()
            self.clear_service_context(); self.clear_active_message_context()
            if self.agent.get("assigned_customer"): self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)

class ElectricTaxiMovingToCustomerDestState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_MOVING_TO_DESTINATION

    async def run(self):
        customers=self.get("current_customer")
        if not customers: self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        customer_id=next(iter(customers.items()))[0]
        try:
            if not self.agent.is_in_destination():
                await self.agent.sleep(1); self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION); return
            self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=CUSTOMER_IN_DEST)
            self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION; self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        except AlreadyInDestination:
            if not self.complete_pending_movement(): self.set_pending_movement("service",0); self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=CUSTOMER_IN_DEST)
            self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION; self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        except PathRequestException:
            self.fail_service("service_route_failed"); self.discard_pending_movement()
            await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_route_failed"})
            self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
            self.fail_service("service_unexpected_error"); self.discard_pending_movement()
            await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_unexpected_error"})
            self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return

class ElectricTaxiArrivedAtCustomerDestState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION

    async def run(self):
        customers=self.get("current_customer")
        if not customers: self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        customer_id=next(iter(customers.items()))[0]
        msg=await self.receive(timeout=60)
        if not msg: self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        try: content=json.loads(msg.body)
        except (json.JSONDecodeError,TypeError): self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        performative=msg.get_metadata("performative")
        if performative in (INFORM_PERFORMATIVE,CANCEL_PERFORMATIVE) and not self.message_matches_active_service(content):
            self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        if performative==INFORM_PERFORMATIVE and content.get("status")==CUSTOMER_IN_DEST:
            if not self.mark_service_completed(): logger.warning("Agent[{}]: Could not mirror completed Electric Taxi service.".format(self.agent.name))
            self.agent.remove_customer_in_transport(customer_id); self.clear_active_message_context(); self.agent.increment_completed_assignments()
            self.agent.status=TRANSPORT_WAITING_FOR_RETURN; self.agent.set_busy(); self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
        if performative==CANCEL_PERFORMATIVE:
            self.fail_service("customer_cancelled_at_destination"); self.discard_pending_movement(); self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)

class ElectricTaxiWaitingForReturnState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_WAITING_FOR_RETURN; self.return_requested=False

    async def run(self):
        if not self.agent.has_return_position():
            if not self.return_requested:
                await self.request_return_position(); self.return_requested=True
            msg=await self.receive(timeout=5)
            if not msg: self.return_requested=False; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
            if msg.get_metadata("performative")!=INFORM_PERFORMATIVE: self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
            try: content=json.loads(msg.body)
            except (json.JSONDecodeError,TypeError): self.return_requested=False; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
            if content.get("request_type")!="taxi_return": self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
            return_position=content.get("return_position")
            if return_position is None: self.return_requested=False; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
            self.agent.set_return_position(return_position)
        return_position=self.agent.get_return_position()
        if return_position is None: self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
        return_km=self.agent.calculate_distance_km(self.agent.get_position(),return_position)
        if not self.agent.has_enough_autonomy_km(return_km):
            self.agent.status=TRANSPORT_NEEDS_CHARGING; self.agent.set_busy(); self.set_next_state(TRANSPORT_NEEDS_CHARGING); return
        try:
            distance,_osrm_duration,_speed_based_duration=await self.agent.move_to(return_position)
            if not self.set_pending_movement("auxiliary",distance,require_service=False): raise RuntimeError("Unable to register Electric Taxi return movement.")
            self.agent.decrease_autonomy_km(return_km)
            self.agent.status=TRANSPORT_MOVING_TO_RETURN; self.set_next_state(TRANSPORT_MOVING_TO_RETURN); return
        except AlreadyInDestination:
            self.set_pending_movement("auxiliary",0,require_service=False); self.complete_pending_movement(); self.clear_service_context()
            self.agent.clear_return_position(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        except PathRequestException:
            self.discard_pending_movement(); await self.agent.sleep(1); self.agent.status=TRANSPORT_WAITING_FOR_RETURN; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
        except Exception as e:
            logger.error("Unexpected error returning electric taxi [{}]: {}".format(self.agent.name,e)); self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING_FOR_RETURN; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return

class ElectricTaxiMovingToReturnState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_MOVING_TO_RETURN

    async def run(self):
        return_position=self.agent.get_return_position()
        if return_position is None:
            self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING_FOR_RETURN; self.set_next_state(TRANSPORT_WAITING_FOR_RETURN); return
        if not self.agent.is_in_destination():
            await self.agent.sleep(1); self.set_next_state(TRANSPORT_MOVING_TO_RETURN); return
        self.complete_pending_movement(); self.clear_service_context(); self.agent.clear_return_position(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return

class FSMElectricTaxiBehaviour(FSMSimfleetBehaviour):
    """
    Represents the Finite State Machine (FSM) strategy for the electric taxi agent.
    This class manages the different states and transitions for the taxi based on its behavior,
    including waiting for customers, moving to charging stations, and traveling to destinations.

    Methods:
        setup(): Initializes all states and defines transitions between them.
    """

    def setup(self):
        """
        Sets up the FSM by adding states and defining transitions.
        This method creates the states the electric taxi can be in and
        specifies the valid transitions between these states.
        """

        # Add states to the FSM
        self.add_state(TRANSPORT_WAITING, ElectricTaxiWaitingState(), initial=True)
        self.add_state(TRANSPORT_NEEDS_CHARGING, ElectricTaxiNeedsChargingState())
        self.add_state(TRANSPORT_WAITING_FOR_APPROVAL, ElectricTaxiWaitingForApprovalState())
        self.add_state(TRANSPORT_MOVING_TO_CUSTOMER, ElectricTaxiMovingToCustomerState())
        self.add_state(TRANSPORT_ARRIVED_AT_CUSTOMER, ElectricTaxiArrivedAtCustomerState())
        self.add_state(TRANSPORT_MOVING_TO_DESTINATION, ElectricTaxiMovingToCustomerDestState())
        self.add_state(TRANSPORT_ARRIVED_AT_DESTINATION, ElectricTaxiArrivedAtCustomerDestState())
        self.add_state(TRANSPORT_MOVING_TO_STATION, ElectricTaxiMovingToStationState())
        self.add_state(TRANSPORT_IN_STATION_PLACE, ElectricTaxiInStationState())
        self.add_state(TRANSPORT_IN_WAITING_LIST, ElectricTaxiInWaitingListState())
        self.add_state(TRANSPORT_CHARGING, ElectricTaxiChargingState())
        self.add_state(TRANSPORT_WAITING_FOR_RETURN, ElectricTaxiWaitingForReturnState())
        self.add_state(TRANSPORT_MOVING_TO_RETURN,ElectricTaxiMovingToReturnState())

        # Define transitions between states

        # Transitions related to the 'Waiting' state
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING)  # Remains in waiting if no new action
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_WAITING_FOR_APPROVAL)  # When a customer accepts a proposal
        self.add_transition(TRANSPORT_WAITING, TRANSPORT_NEEDS_CHARGING)  # If the taxi needs charging

        # Transitions from 'Waiting For Approval' state
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_WAITING_FOR_APPROVAL)  # Keep waiting for approval
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_WAITING)  # If the proposal is refused
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_MOVING_TO_CUSTOMER)  # If the customer accepts
        self.add_transition(TRANSPORT_WAITING_FOR_APPROVAL, TRANSPORT_ARRIVED_AT_CUSTOMER)  # Direct arrival scenario

        # Transitions from 'Moving To Customer' state
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_MOVING_TO_CUSTOMER)  # Still moving
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_WAITING)  # Encounter an issue, go back to waiting
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_ARRIVED_AT_CUSTOMER)  # Successfully arrive

        # Transitions from 'Arrived At Customer' state
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_ARRIVED_AT_CUSTOMER)  # Waiting at customer's location
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_MOVING_TO_DESTINATION)  # Begin journey to destination
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_ARRIVED_AT_DESTINATION)  # Direct destination arrival
        self.add_transition(TRANSPORT_ARRIVED_AT_CUSTOMER, TRANSPORT_WAITING)  # Cancel and return to waiting

        # Transitions from 'Moving To Destination' state
        self.add_transition(TRANSPORT_MOVING_TO_DESTINATION, TRANSPORT_MOVING_TO_DESTINATION)  # Still moving to destination
        self.add_transition(TRANSPORT_MOVING_TO_DESTINATION, TRANSPORT_WAITING)  # An issue encountered, return to waiting
        self.add_transition(TRANSPORT_MOVING_TO_DESTINATION, TRANSPORT_ARRIVED_AT_DESTINATION)  # Arrival at destination

        # Transitions from 'Arrived At Destination' state
        self.add_transition(TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_ARRIVED_AT_DESTINATION)  # Stay at destination
        self.add_transition(TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_WAITING)  # Drop customer and return to waiting

        # Transitions related to the 'Needs Charging' state
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_NEEDS_CHARGING)  # Continue searching for a station
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_WAITING)  # Issue finding station, return to waiting
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_MOVING_TO_STATION)  # Successfully heading to station
        self.add_transition(TRANSPORT_NEEDS_CHARGING, TRANSPORT_IN_STATION_PLACE)  # Arrives at the station

        # Transitions from 'Moving To Station' state
        self.add_transition(TRANSPORT_MOVING_TO_STATION, TRANSPORT_MOVING_TO_STATION)  # Still heading to the station
        self.add_transition(TRANSPORT_MOVING_TO_STATION, TRANSPORT_IN_STATION_PLACE)  # Arrives at station
        self.add_transition(TRANSPORT_MOVING_TO_STATION, TRANSPORT_NEEDS_CHARGING)

        # Transitions from 'In Station Place' state
        self.add_transition(TRANSPORT_IN_STATION_PLACE, TRANSPORT_IN_STATION_PLACE)  # Waiting in station queue
        self.add_transition(TRANSPORT_IN_STATION_PLACE, TRANSPORT_NEEDS_CHARGING)  # Transition if refused service
        self.add_transition(TRANSPORT_IN_STATION_PLACE, TRANSPORT_IN_WAITING_LIST)  # Moved to waiting list for service

        # Transitions from 'In Waiting List' state
        self.add_transition(TRANSPORT_IN_WAITING_LIST, TRANSPORT_IN_WAITING_LIST)  # Remain in queue
        self.add_transition(TRANSPORT_IN_WAITING_LIST, TRANSPORT_CHARGING)  # Begin charging process
        self.add_transition(TRANSPORT_IN_WAITING_LIST, TRANSPORT_NEEDS_CHARGING)

        # Transitions from 'Charging' state
        self.add_transition(TRANSPORT_CHARGING, TRANSPORT_CHARGING)  # Continue charging
        self.add_transition(TRANSPORT_CHARGING, TRANSPORT_WAITING)  # Finish charging and return to waiting

        # Additional transitions for customer movement and destination states
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_MOVING_TO_CUSTOMER)  # Still en route to customer
        self.add_transition(TRANSPORT_MOVING_TO_CUSTOMER, TRANSPORT_WAITING)  # Return to waiting if issue arises

        self.add_transition(TRANSPORT_ARRIVED_AT_DESTINATION, TRANSPORT_WAITING_FOR_RETURN)

        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_MOVING_TO_RETURN)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_NEEDS_CHARGING)
        self.add_transition(TRANSPORT_WAITING_FOR_RETURN, TRANSPORT_WAITING)

        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_MOVING_TO_RETURN)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_WAITING_FOR_RETURN)
        self.add_transition(TRANSPORT_MOVING_TO_RETURN, TRANSPORT_WAITING)

        self.add_transition(TRANSPORT_CHARGING, TRANSPORT_WAITING_FOR_RETURN)


################################################################
#                                                              #
#           NO Point Of Return Electric Taxi Strategy          #
#                                                              #
################################################################

class NRPElectricTaxiWaitingState(ElectricTaxiStrategyBehaviour):
    """
        Represents the 'Waiting' state for the electric taxi. The taxi is waiting to receive a transport request.

        Methods:
            on_start(): Sets the initial state to 'TRANSPORT_WAITING' and logs the state.
            run(): Handles incoming messages, processes transport requests, and transitions to the next state.
        """
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING)
            return
        logger.debug("Agent[{}]: The agent received: {}".format(self.agent.jid, msg.body))
        content = json.loads(msg.body)
        performative = msg.get_metadata("performative")
        if performative == REQUEST_PERFORMATIVE:
            if self.store_pending_offer(content) is None:
                logger.warning(
                    "Agent[{}]: Ignoring request without valid schema-1.0 identifiers.".format(
                        self.agent.name
                    )
                )
                self.set_next_state(TRANSPORT_WAITING)
                return


            if not self.agent.has_enough_autonomy_for_service(
                content["origin"],
                content["dest"]
            ):


                await self.cancel_proposal(
                    content["customer_id"],
                    self.pending_offer_message_details(),
                )
                self.clear_pending_offer()
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return
            else:


                await self.send_proposal(
                    content["customer_id"],
                    self.pending_offer_message_details(),
                )
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
                return
        else:
            self.set_next_state(TRANSPORT_WAITING)
            return


class NPRElectricTaxiWaitingForApprovalState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_WAITING_FOR_APPROVAL

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
            return
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            if not self.message_matches_pending_offer(content):
                logger.warning("Agent[{}]: Ignoring stale or mismatched acceptance.".format(self.agent.name))
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)
                return
            active = self.activate_pending_offer(content)
            customer_id = content["customer_id"]
            if not self.agent.has_enough_autonomy_km(self.agent.calculate_service_km(content["origin"], content["dest"])):
                await self.cancel_proposal(customer_id)
                self.clear_active_message_context()
                self.agent.set_busy()
                self.agent.status = TRANSPORT_NEEDS_CHARGING
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return
            try:
                context = self.create_service_context(
                    service_id=active["service_id"], user_id=active["user_id"],
                    transport_id=active["transport_id"], origin=active["origin"],
                    destination=active["destination"], emit_requested=False,
                )
                if context is None or not self.assign_service(self.agent.jid):
                    raise RuntimeError("Unable to establish Electric Taxi metrics service context.")
                self.agent.add_assigned_customer(
                    customer_id=customer_id, origin=content["origin"], dest=content["dest"]
                )
                self.agent.set_busy()
                await self.inform_customer(customer_id=customer_id, status=TRANSPORT_MOVING_TO_CUSTOMER)
                distance, _osrm_duration, _speed_based_duration = await self.agent.move_to(content["origin"])
                self.agent.decrease_autonomy_km(self.agent.calculate_service_km(content["origin"], content["dest"]))
                if not self.set_pending_movement("approach", distance):
                    raise RuntimeError("Unable to register Electric Taxi approach movement.")
                self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER
                self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER)
                return
            except AlreadyInDestination:
                self.agent.decrease_autonomy_km(
                    self.agent.calculate_service_km(
                        content["origin"],
                        content["dest"]
                    )
                )

                self.set_pending_movement("approach", 0)
                self.complete_pending_movement()

                await self.inform_customer(
                    customer_id=customer_id,
                    status=TRANSPORT_IN_CUSTOMER_PLACE
                )

                self.agent.status = TRANSPORT_ARRIVED_AT_CUSTOMER
                self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)
                return
            except PathRequestException:
                self.fail_service("approach_route_failed")
                self.discard_pending_movement()
                await self.cancel_proposal(customer_id, {"terminal_status":"failed","failure_reason":"approach_route_failed"})
                self.clear_service_context(); self.clear_active_message_context()
                if self.agent.get("assigned_customer"):
                    self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING; self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING); return
            except Exception as e:
                logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name, e))
                self.fail_service("approach_unexpected_error")
                self.discard_pending_movement()
                await self.cancel_proposal(customer_id, {"terminal_status":"failed","failure_reason":"approach_unexpected_error"})
                self.clear_service_context(); self.clear_active_message_context()
                if self.agent.get("assigned_customer"):
                    self.agent.remove_assigned_customer()
                self.agent.status = TRANSPORT_WAITING; self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING); return
        elif performative == REFUSE_PERFORMATIVE:
            if not self.message_matches_pending_offer(content):
                self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL); return
            self.clear_pending_offer(); self.agent.set_available()
            self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_WAITING_FOR_APPROVAL)

class NPRElectricTaxiMovingToCustomerState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start(); self.agent.status = TRANSPORT_MOVING_TO_CUSTOMER

    async def run(self):
        customers = self.get("assigned_customer")
        if not customers:
            self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        customer_id = next(iter(customers.items()))[0]
        try:
            if not self.agent.is_in_destination():
                msg = await self.receive(timeout=2)
                if msg and msg.get_metadata("performative") == REFUSE_PERFORMATIVE:
                    try: content=json.loads(msg.body)
                    except (json.JSONDecodeError,TypeError):
                        self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
                    if not self.message_matches_active_service(content):
                        self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
                    self.fail_service("customer_cancelled_before_pickup")
                    self.discard_pending_movement()
                    await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"customer_cancelled_before_pickup"})
                    self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
                    self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
                self.set_next_state(TRANSPORT_MOVING_TO_CUSTOMER); return
            self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=TRANSPORT_IN_CUSTOMER_PLACE)
            self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER; self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        except AlreadyInDestination:
            if not self.complete_pending_movement():
                self.set_pending_movement("approach",0); self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=TRANSPORT_IN_CUSTOMER_PLACE)
            self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER; self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        except PathRequestException:
            self.fail_service("approach_route_failed"); self.discard_pending_movement()
            await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"approach_route_failed"})
            self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
            self.fail_service("approach_unexpected_error"); self.discard_pending_movement()
            await self.cancel_proposal(customer_id,{"terminal_status":"failed","failure_reason":"approach_unexpected_error"})
            self.clear_service_context(); self.clear_active_message_context(); self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return

class NPRElectricTaxiArrivedAtCustomerState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_ARRIVED_AT_CUSTOMER

    async def run(self):
        msg=await self.receive(timeout=60)
        if not msg: self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        try: content=json.loads(msg.body)
        except (json.JSONDecodeError,TypeError): self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        performative=msg.get_metadata("performative")
        if performative in (INFORM_PERFORMATIVE,CANCEL_PERFORMATIVE) and not self.message_matches_active_service(content):
            self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
        if performative==INFORM_PERFORMATIVE and content.get("status")==CUSTOMER_IN_TRANSPORT:
            customers=self.get("assigned_customer")
            if not customers: self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER); return
            customer_id = next(iter(customers.items()))[0]
            dest = next(iter(customers.items()))[1]["destination"]

            service_km = self.agent.calculate_distance_km(
                self.agent.get_position(),
                dest,
            )

            try:
                self.agent.add_customer_in_transport(
                    customer_id=customer_id,
                    dest=dest,
                )

                if not self.start_service(self.agent.jid):
                    raise RuntimeError(
                        "Unable to emit Electric Taxi service start."
                    )

                self.agent.remove_assigned_customer()

                distance, _osrm_duration, _speed_based_duration = (
                    await self.agent.move_to(dest)
                )

                if not self.set_pending_movement("service", distance):
                    raise RuntimeError(
                        "Unable to register Electric Taxi service movement."
                    )

                self.agent.status = TRANSPORT_MOVING_TO_DESTINATION
                self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION)
                return

            except AlreadyInDestination:
                self.set_pending_movement("service", 0)
                self.complete_pending_movement()

                await self.inform_customer(
                    customer_id=customer_id,
                    status=CUSTOMER_IN_DEST,
                )

                self.agent.status = TRANSPORT_ARRIVED_AT_DESTINATION
                self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)
                return

            except PathRequestException:
                # The service leg never started physically. Restore the
                # autonomy that had been reserved for that unexecuted leg.
                self.agent.increase_autonomy_km(service_km)

                self.fail_service("service_route_failed")
                self.discard_pending_movement()

                await self.cancel_customer(
                    customer_id,
                    {
                        "terminal_status": "failed",
                        "failure_reason": "service_route_failed",
                    },
                )

                self.clear_service_context()
                self.clear_active_message_context()

                if customer_id in self.agent.get("current_customer"):
                    self.agent.remove_customer_in_transport(customer_id)

                self.agent.status = TRANSPORT_WAITING
                self.agent.set_available()
                self.set_next_state(TRANSPORT_WAITING)
                return
            except Exception as e:
                logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
                self.fail_service("service_unexpected_error"); self.discard_pending_movement()
                await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_unexpected_error"})
                self.clear_service_context(); self.clear_active_message_context()
                if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
                self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        elif performative==CANCEL_PERFORMATIVE:
            self.fail_service("customer_cancelled_at_pickup"); self.discard_pending_movement()
            self.clear_service_context(); self.clear_active_message_context()
            if self.agent.get("assigned_customer"): self.agent.remove_assigned_customer()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_ARRIVED_AT_CUSTOMER)

class NPRElectricTaxiMovingToCustomerDestState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_MOVING_TO_DESTINATION

    async def run(self):
        customers=self.get("current_customer")
        if not customers: self.discard_pending_movement(); self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        customer_id=next(iter(customers.items()))[0]
        try:
            if not self.agent.is_in_destination():
                await self.agent.sleep(1); self.set_next_state(TRANSPORT_MOVING_TO_DESTINATION); return
            self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=CUSTOMER_IN_DEST)
            self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION; self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        except AlreadyInDestination:
            if not self.complete_pending_movement(): self.set_pending_movement("service",0); self.complete_pending_movement()
            await self.inform_customer(customer_id=customer_id,status=CUSTOMER_IN_DEST)
            self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION; self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        except PathRequestException:
            self.fail_service("service_route_failed"); self.discard_pending_movement()
            await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_route_failed"})
            self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name,e))
            self.fail_service("service_unexpected_error"); self.discard_pending_movement()
            await self.cancel_customer(customer_id,{"terminal_status":"failed","failure_reason":"service_unexpected_error"})
            self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return

class NPRElectricTaxiArrivedAtCustomerDestState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_ARRIVED_AT_DESTINATION

    async def run(self):
        customers=self.get("current_customer")
        if not customers: self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        customer_id=next(iter(customers.items()))[0]
        msg=await self.receive(timeout=60)
        if not msg: self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        try: content=json.loads(msg.body)
        except (json.JSONDecodeError,TypeError): self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        performative=msg.get_metadata("performative")
        if performative in (INFORM_PERFORMATIVE,CANCEL_PERFORMATIVE) and not self.message_matches_active_service(content):
            self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION); return
        if performative==INFORM_PERFORMATIVE and content.get("status")==CUSTOMER_IN_DEST:
            if not self.mark_service_completed(): logger.warning("Agent[{}]: Could not mirror completed NRP Electric Taxi service.".format(self.agent.name))
            self.agent.remove_customer_in_transport(customer_id); self.clear_active_message_context(); self.agent.increment_completed_assignments(); self.clear_service_context()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        if performative==CANCEL_PERFORMATIVE:
            self.fail_service("customer_cancelled_at_destination"); self.discard_pending_movement(); self.clear_service_context(); self.clear_active_message_context()
            if customer_id in self.agent.get("current_customer"): self.agent.remove_customer_in_transport(customer_id)
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_ARRIVED_AT_DESTINATION)

class NPRElectricTaxiNeedsChargingState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_NEEDS_CHARGING
        self.agent.set_busy()

    async def run(self):
        if self.agent.get_stations() is None or self.agent.get_number_stations() < 1:
            logger.info(
                "Agent[{}]: The agent looking for a station.".format(
                    self.agent.name
                )
            )
            stations = await self.agent.get_list_agent_position(
                self.agent.service_type,
                self.agent.get_stations()
            )
            self.agent.set_stations(stations)

            if not stations:
                await self.agent.sleep(1)

            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

        nearby_station_dest = self.agent.nearst_agent(
            self.agent.get_stations(),
            self.agent.get_position()
        )

        if nearby_station_dest is None:
            logger.warning(
                "Agent[{}]: No charging station available.".format(
                    self.agent.name
                )
            )

            await self.agent.sleep(1)

            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

        self.agent.set_nearby_station(nearby_station_dest)
        station_id = self.agent.get_nearby_station_id()
        station_position = self.agent.get_nearby_station_position()
        logger.info("Agent[{}]: The agent selected station [{}].".format(self.agent.name, station_id))

        # A charging_id is independent from service_id. It is created before travel
        # so the same session is used for physical arrival/start/completion.
        if self.get_charging_context() is None:
            if self.create_charging_context(station_id) is None:
                self.set_next_state(TRANSPORT_NEEDS_CHARGING)
                return

        try:
            await self.go_to_the_station(station_id, station_position)
            travel_km = self.agent.calculate_distance_km(
                self.agent.get_position(), station_position
            )
            try:
                distance, _osrm_duration, _speed_based_duration = await self.agent.move_to(
                    station_position
                )
                if not self.set_pending_movement(
                    "auxiliary", distance, require_service=False
                ):
                    raise RuntimeError("Unable to register charging-station movement.")
                self.agent.decrease_autonomy_km(travel_km)
                self.agent.status = TRANSPORT_MOVING_TO_STATION
                self.set_next_state(TRANSPORT_MOVING_TO_STATION)
                return
            except AlreadyInDestination:
                self.set_pending_movement("auxiliary", 0, require_service=False)
                self.complete_pending_movement()
                self.charging_arrived()
                arguments = {
                    "transport_need": self.agent.max_autonomy_km - self.agent.current_autonomy_km
                }
                await self.request_access_station(
                    station_id,
                    {"service_name": self.agent.service_type, "object_type": "transport", "args": arguments},
                )
                self.agent.status = TRANSPORT_IN_STATION_PLACE
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
        except PathRequestException:
            logger.error("Agent[{}]: The agent could not get a path to station [{}].".format(self.agent.name, station_id))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        except Exception as e:
            logger.error("Unexpected error in transport [{}]: {}".format(self.agent.name, e))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

class NPRElectricTaxiMovingToStationState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_MOVING_TO_STATION

    async def _arrive_and_request(self):
        station_id = self.agent.get_current_station()
        self.complete_pending_movement()
        if self.get_charging_context() is None:
            if self.create_charging_context(station_id) is None:
                return False
        self.charging_arrived()
        arguments = {
            "transport_need": self.agent.max_autonomy_km - self.agent.current_autonomy_km
        }
        await self.request_access_station(
            station_id,
            {"service_name": self.agent.service_type, "object_type": "transport", "args": arguments},
        )
        self.agent.status = TRANSPORT_IN_STATION_PLACE
        self.set_next_state(TRANSPORT_IN_STATION_PLACE)
        return True

    async def run(self):
        try:
            if not self.agent.is_in_destination():
                await self.agent.sleep(1)
                self.set_next_state(TRANSPORT_MOVING_TO_STATION)
                return
            await self._arrive_and_request()
            return
        except AlreadyInDestination:
            if not self.complete_pending_movement():
                self.set_pending_movement("auxiliary", 0, require_service=False)
            await self._arrive_and_request()
            return
        except PathRequestException:
            logger.error("Agent[{}]: The agent could not complete the path to charging station.".format(self.agent.name))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        except Exception as e:
            logger.error("Unexpected charging-route error in [{}]: {}".format(self.agent.name, e))
            self.discard_pending_movement()
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return

class NPRElectricTaxiInStationState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_IN_STATION_PLACE

    async def run(self):
        msg = await self.receive(timeout=60)
        if not msg:
            self.set_next_state(TRANSPORT_IN_STATION_PLACE)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_IN_STATION_PLACE)
            return
        performative = msg.get_metadata("performative")
        if performative == ACCEPT_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                logger.warning("Agent[{}]: Ignoring charging-station ACCEPT with mismatched station.".format(self.agent.name))
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
            self.agent.status = TRANSPORT_IN_WAITING_LIST
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        if performative == REFUSE_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_STATION_PLACE)
                return
            # The already-emitted arrival remains an unfinished charging session.
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        self.set_next_state(TRANSPORT_IN_STATION_PLACE)

class NPRElectricTaxiInWaitingListState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start()
        self.agent.status = TRANSPORT_IN_WAITING_LIST

    async def run(self):
        msg = await self.receive(timeout=5)
        if not msg:
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        try:
            content = json.loads(msg.body)
        except (json.JSONDecodeError, TypeError):
            self.set_next_state(TRANSPORT_IN_WAITING_LIST)
            return
        performative = msg.get_metadata("performative")
        if performative == INFORM_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                return
            if content.get("serving"):
                if not self.charging_started():
                    logger.warning("Agent[{}]: Charging start milestone could not be emitted.".format(self.agent.name))
                    self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                    return
                self.agent.status = TRANSPORT_CHARGING
                self.set_next_state(TRANSPORT_CHARGING)
                return
        elif performative == REFUSE_PERFORMATIVE:
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ):
                self.set_next_state(TRANSPORT_IN_WAITING_LIST)
                return
            self.abandon_charging_context()
            await self.drop_station()
            self.agent.status = TRANSPORT_NEEDS_CHARGING
            self.set_next_state(TRANSPORT_NEEDS_CHARGING)
            return
        self.set_next_state(TRANSPORT_IN_WAITING_LIST)

class NPRElectricTaxiChargingState(ElectricTaxiStrategyBehaviour):
    async def on_start(self):
        await super().on_start(); self.agent.status=TRANSPORT_CHARGING

    async def run(self):
        msg=await self.receive(timeout=60)
        if not msg: self.set_next_state(TRANSPORT_CHARGING); return
        try: content=json.loads(msg.body)
        except (json.JSONDecodeError,TypeError): self.set_next_state(TRANSPORT_CHARGING); return
        if msg.get_metadata("protocol")==REQUEST_PROTOCOL and msg.get_metadata("performative")==INFORM_PERFORMATIVE and content.get("charged"):
            if not self.charging_message_matches_context(
                content,
                msg.sender
            ): self.set_next_state(TRANSPORT_CHARGING); return
            if not self.charging_completed(): self.set_next_state(TRANSPORT_CHARGING); return
            self.agent.increase_full_autonomy_km(); await self.drop_station(); self.clear_charging_context()
            self.agent.status=TRANSPORT_WAITING; self.agent.set_available(); self.set_next_state(TRANSPORT_WAITING); return
        self.set_next_state(TRANSPORT_CHARGING)

class FSMNPRElectricTaxiBehaviour(FSMSimfleetBehaviour):

    def setup(self):

        # ============================================================
        # States
        # ============================================================

        self.add_state(
            TRANSPORT_WAITING,
            NRPElectricTaxiWaitingState(),
            initial=True
        )

        self.add_state(
            TRANSPORT_WAITING_FOR_APPROVAL,
            NPRElectricTaxiWaitingForApprovalState()
        )

        self.add_state(
            TRANSPORT_MOVING_TO_CUSTOMER,
            NPRElectricTaxiMovingToCustomerState()
        )

        self.add_state(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            NPRElectricTaxiArrivedAtCustomerState()
        )

        self.add_state(
            TRANSPORT_MOVING_TO_DESTINATION,
            NPRElectricTaxiMovingToCustomerDestState()
        )

        self.add_state(
            TRANSPORT_ARRIVED_AT_DESTINATION,
            NPRElectricTaxiArrivedAtCustomerDestState()
        )

        self.add_state(
            TRANSPORT_NEEDS_CHARGING,
            NPRElectricTaxiNeedsChargingState()
        )

        self.add_state(
            TRANSPORT_MOVING_TO_STATION,
            NPRElectricTaxiMovingToStationState()
        )

        self.add_state(
            TRANSPORT_IN_STATION_PLACE,
            NPRElectricTaxiInStationState()
        )

        self.add_state(
            TRANSPORT_IN_WAITING_LIST,
            NPRElectricTaxiInWaitingListState()
        )

        self.add_state(
            TRANSPORT_CHARGING,
            NPRElectricTaxiChargingState()
        )

        # ============================================================
        # WAITING
        # ============================================================

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_WAITING_FOR_APPROVAL
        )

        self.add_transition(
            TRANSPORT_WAITING,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # WAITING FOR APPROVAL
        # ============================================================

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_WAITING_FOR_APPROVAL
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_WAITING
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_NEEDS_CHARGING
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_MOVING_TO_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_WAITING_FOR_APPROVAL,
            TRANSPORT_ARRIVED_AT_CUSTOMER
        )

        # ============================================================
        # MOVING TO CUSTOMER
        # ============================================================

        self.add_transition(
            TRANSPORT_MOVING_TO_CUSTOMER,
            TRANSPORT_MOVING_TO_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_CUSTOMER,
            TRANSPORT_ARRIVED_AT_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_CUSTOMER,
            TRANSPORT_WAITING
        )

        # ============================================================
        # ARRIVED AT CUSTOMER
        # ============================================================

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_ARRIVED_AT_CUSTOMER
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_ARRIVED_AT_DESTINATION
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_CUSTOMER,
            TRANSPORT_WAITING
        )

        # ============================================================
        # MOVING TO DESTINATION
        # ============================================================

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_MOVING_TO_DESTINATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_ARRIVED_AT_DESTINATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_DESTINATION,
            TRANSPORT_WAITING
        )

        # ============================================================
        # ARRIVED AT DESTINATION
        # ============================================================

        self.add_transition(
            TRANSPORT_ARRIVED_AT_DESTINATION,
            TRANSPORT_ARRIVED_AT_DESTINATION
        )

        self.add_transition(
            TRANSPORT_ARRIVED_AT_DESTINATION,
            TRANSPORT_WAITING
        )

        # ============================================================
        # NEEDS CHARGING
        # ============================================================

        self.add_transition(
            TRANSPORT_NEEDS_CHARGING,
            TRANSPORT_NEEDS_CHARGING
        )

        self.add_transition(
            TRANSPORT_NEEDS_CHARGING,
            TRANSPORT_MOVING_TO_STATION
        )

        self.add_transition(
            TRANSPORT_NEEDS_CHARGING,
            TRANSPORT_IN_STATION_PLACE
        )

        # ============================================================
        # MOVING TO STATION
        # ============================================================

        self.add_transition(
            TRANSPORT_MOVING_TO_STATION,
            TRANSPORT_MOVING_TO_STATION
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_STATION,
            TRANSPORT_IN_STATION_PLACE
        )

        self.add_transition(
            TRANSPORT_MOVING_TO_STATION,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # IN STATION PLACE
        # ============================================================

        self.add_transition(
            TRANSPORT_IN_STATION_PLACE,
            TRANSPORT_IN_STATION_PLACE
        )

        self.add_transition(
            TRANSPORT_IN_STATION_PLACE,
            TRANSPORT_IN_WAITING_LIST
        )

        self.add_transition(
            TRANSPORT_IN_STATION_PLACE,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # IN WAITING LIST
        # ============================================================

        self.add_transition(
            TRANSPORT_IN_WAITING_LIST,
            TRANSPORT_IN_WAITING_LIST
        )

        self.add_transition(
            TRANSPORT_IN_WAITING_LIST,
            TRANSPORT_CHARGING
        )

        self.add_transition(
            TRANSPORT_IN_WAITING_LIST,
            TRANSPORT_NEEDS_CHARGING
        )

        # ============================================================
        # CHARGING
        # ============================================================

        self.add_transition(
            TRANSPORT_CHARGING,
            TRANSPORT_CHARGING
        )

        self.add_transition(
            TRANSPORT_CHARGING,
            TRANSPORT_WAITING
        )
