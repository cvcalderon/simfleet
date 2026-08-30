"""Canonical public mobility statistics for SimFleet schema 1.0.

The processor is intentionally read-only: it consumes the completed event Log and
builds API-oriented JSON documents from canonical strategy events only.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from simfleet.metrics.basestatistics import BaseStatisticsClass
from simfleet.utils.statistics import Log

logger = logging.getLogger(__name__)


class MobilityStatisticsClass(BaseStatisticsClass):
    SCHEMA_VERSION = "1.0"

    MODALITIES = (
        "taxi",
        "delivery",
        "electric_taxi",
        "public_transport",
        "sharing",
        "station_sharing",
    )

    TRANSPORT_MODALITIES = {
        "taxi",
        "delivery",
        "electric_taxi",
        "public_transport",
    }

    CANONICAL_EVENTS = {
        "service_requested",
        "service_assigned",
        "service_started",
        "service_completed",
        "service_failed",
        "movement_completed",
        "charging_arrived",
        "charging_started",
        "charging_completed",
    }

    SERVICE_EVENTS = {
        "service_requested",
        "service_assigned",
        "service_started",
        "service_completed",
        "service_failed",
    }

    CHARGING_EVENTS = {
        "charging_arrived",
        "charging_started",
        "charging_completed",
    }

    MOVEMENT_PHASES = {"approach", "service", "auxiliary"}

    OUTPUT_FILES = {
        "taxi": "simfleet_metrics_taxi.json",
        "delivery": "simfleet_metrics_delivery.json",
        "electric_taxi": "simfleet_metrics_electrictaxi.json",
        "public_transport": "simfleet_metrics_publictransport.json",
        "sharing": "simfleet_metrics_sharing.json",
        "station_sharing": "simfleet_metrics_stationsharing.json",
    }

    def __init__(self, simulation_id: Optional[str] = None, output_dir: str = "."):
        self.simulation_id = None if simulation_id is None else str(simulation_id)
        self.output_dir = Path(output_dir)
        self.data_quality_warnings: List[str] = []
        self.results: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, events_log: Log) -> None:
        """Generate one schema-1.0 JSON file for every supported modality."""
        self.data_quality_warnings = []
        canonical_events = self._canonical_events(events_log)
        simulation_id = self._resolve_simulation_id(canonical_events)

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results = {}

        for modality in self.MODALITIES:
            events = [
                event
                for event in canonical_events
                if self._details(event).get("modality") == modality
            ]
            try:
                result = self._build_modality_result(
                    modality=modality,
                    events=events,
                    simulation_id=simulation_id,
                )
            except Exception as exc:  # keep other modality exports alive on bad data
                self._warn(
                    "Failed to process modality {}: {}. Exporting an empty result.".format(
                        modality, exc
                    )
                )
                logger.exception("Unexpected mobility-statistics processing error")
                result = self._build_modality_result(
                    modality=modality,
                    events=[],
                    simulation_id=simulation_id,
                )
            self.results[modality] = result
            try:
                self.export_to_json(
                    result,
                    str(self.output_dir / self.OUTPUT_FILES[modality]),
                )
            except OSError as exc:
                self._warn(
                    "Could not export modality {} to JSON: {}.".format(modality, exc)
                )

        self.print_stats()

    def export_to_json(self, json_data: dict, file_path: str) -> None:
        """Write JSON-native deterministic output and reject NaN/Infinity."""
        with open(file_path, "w", encoding="utf-8") as file_handle:
            json.dump(
                json_data,
                file_handle,
                indent=4,
                ensure_ascii=False,
                allow_nan=False,
            )
            file_handle.write("\n")

    def print_stats(self) -> None:
        """Print a compact schema-1.0 summary for interactive simulator runs."""
        if not self.results:
            return
        print("Simulation Results:")
        for modality in self.MODALITIES:
            summary = self.results[modality]["summary"]
            print(
                "{}: services={} completed={} failed={} unfinished={}".format(
                    modality,
                    summary["total_services"],
                    summary["completed_services"],
                    summary["failed_services"],
                    summary["unfinished_services"],
                )
            )

    # ------------------------------------------------------------------
    # Canonical filtering / normalization
    # ------------------------------------------------------------------

    def _warn(self, message: str) -> None:
        self.data_quality_warnings.append(message)
        logger.warning(message)

    @staticmethod
    def _details(event) -> Dict[str, Any]:
        return event.details if isinstance(getattr(event, "details", None), dict) else {}

    @staticmethod
    def _bare_jid(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value)
        return text.split("/", 1)[0]

    def _timestamp_seconds(self, value: Any, context: str) -> Optional[float]:
        if isinstance(value, bool):
            self._warn("{} has a boolean timestamp; event ignored for time metrics.".format(context))
            return None
        if isinstance(value, (int, float)):
            number = float(value)
        elif isinstance(value, datetime):
            number = float(value.timestamp())
        else:
            try:
                number = float(value)
            except (TypeError, ValueError):
                self._warn("{} has an unsupported timestamp {!r}.".format(context, value))
                return None
        if not math.isfinite(number):
            self._warn("{} has a non-finite timestamp.".format(context))
            return None
        return number

    def _event_sort_key(self, indexed_event: Tuple[int, Any]) -> Tuple[float, int]:
        index, event = indexed_event
        timestamp = self._timestamp_seconds(
            getattr(event, "timestamp", None),
            "event {}".format(getattr(event, "event_type", "unknown")),
        )
        if timestamp is None:
            timestamp = math.inf
        return timestamp, index

    def _canonical_events(self, events_log: Log) -> List[Any]:
        raw_events = list(getattr(events_log, "events", []) or [])
        canonical: List[Tuple[int, Any]] = []

        for index, event in enumerate(raw_events):
            event_type = getattr(event, "event_type", None)
            if event_type not in self.CANONICAL_EVENTS:
                continue

            details = self._details(event)
            modality = details.get("modality")
            if modality not in self.MODALITIES:
                self._warn(
                    "Canonical event {} from {} has missing/invalid modality {!r}; ignored.".format(
                        event_type,
                        getattr(event, "name", None),
                        modality,
                    )
                )
                continue

            if event_type in self.SERVICE_EVENTS:
                if details.get("service_id") is None or details.get("user_id") is None:
                    self._warn(
                        "{} event for modality {} lacks service_id/user_id; ignored.".format(
                            event_type, modality
                        )
                    )
                    continue
                if event_type == "service_requested" and (
                    details.get("origin") is None or details.get("destination") is None
                ):
                    self._warn(
                        "service_requested for modality {} lacks origin/destination; ignored.".format(
                            modality
                        )
                    )
                    continue
                if event_type in {"service_assigned", "service_started"} and (
                    details.get("transport_id") is None
                ):
                    self._warn(
                        "{} for modality {} lacks transport_id; ignored.".format(
                            event_type, modality
                        )
                    )
                    continue

            if event_type == "movement_completed":
                phase = details.get("phase")
                if phase not in self.MOVEMENT_PHASES:
                    self._warn(
                        "movement_completed for modality {} has invalid phase {!r}; ignored.".format(
                            modality, phase
                        )
                    )
                    continue
                if self._non_negative_number(details.get("distance_m")) is None:
                    self._warn(
                        "movement_completed for modality {} has invalid distance_m; ignored.".format(
                            modality
                        )
                    )
                    continue

            if event_type in self.CHARGING_EVENTS:
                if modality != "electric_taxi":
                    self._warn(
                        "Charging event {} has modality {}; ignored.".format(event_type, modality)
                    )
                    continue
                required = ("charging_id", "transport_id", "station_id")
                if any(details.get(field) is None for field in required):
                    self._warn(
                        "Charging event {} lacks charging_id/transport_id/station_id; ignored.".format(
                            event_type
                        )
                    )
                    continue

            canonical.append((index, event))

        canonical.sort(key=self._event_sort_key)
        ordered = [event for _, event in canonical]

        service_modalities: Dict[str, str] = {}
        for event in ordered:
            details = self._details(event)
            if event.event_type not in self.SERVICE_EVENTS:
                continue
            service_id = str(details.get("service_id"))
            modality = details.get("modality")
            previous = service_modalities.setdefault(service_id, modality)
            if previous != modality:
                self._warn(
                    "service_id {} appears in multiple modalities: {} and {}.".format(
                        service_id, previous, modality
                    )
                )
        return ordered

    def _resolve_simulation_id(self, events: Iterable[Any]) -> Optional[str]:
        if self.simulation_id is not None:
            return self.simulation_id

        values = []
        for event in events:
            value = self._details(event).get("simulation_id")
            if value is not None:
                values.append(str(value))

        if not values:
            return None

        selected = values[0]
        if any(value != selected for value in values[1:]):
            self._warn(
                "Conflicting simulation_id values found in canonical events; first value {!r} used.".format(
                    selected
                )
            )
        return selected

    @staticmethod
    def _non_negative_number(value: Any) -> Optional[float]:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        number = float(value)
        if not math.isfinite(number) or number < 0:
            return None
        return number

    def _safe_json_value(self, value: Any, context: str) -> Any:
        if value is None or isinstance(value, (str, bool, int)):
            return value
        if isinstance(value, float):
            if math.isfinite(value):
                return value
            self._warn("{} contains a non-finite number; replaced by null.".format(context))
            return None
        if isinstance(value, datetime):
            return self._timestamp_seconds(value, context)
        if isinstance(value, (list, tuple)):
            return [
                self._safe_json_value(item, "{}[]".format(context))
                for item in value
            ]
        if isinstance(value, dict):
            return {
                str(key): self._safe_json_value(item, "{}.{}".format(context, key))
                for key, item in value.items()
            }
        try:
            scalar = value.item()
        except (AttributeError, ValueError, TypeError):
            self._warn(
                "{} contains unsupported value {!r}; converted to string.".format(
                    context, value
                )
            )
            return str(value)
        return self._safe_json_value(scalar, context)

    def _safe_delta(
        self,
        start: Optional[float],
        end: Optional[float],
        context: str,
    ) -> Optional[float]:
        if start is None or end is None:
            return None
        value = end - start
        if value < 0:
            self._warn("Negative duration detected for {}; exported as null.".format(context))
            return None
        return float(value)

    @staticmethod
    def _mean(values: Iterable[Optional[float]]) -> Optional[float]:
        valid = [float(value) for value in values if value is not None]
        if not valid:
            return None
        return float(sum(valid) / len(valid))

    # ------------------------------------------------------------------
    # Modality assembly
    # ------------------------------------------------------------------

    def _build_modality_result(
        self,
        modality: str,
        events: List[Any],
        simulation_id: Optional[str],
    ) -> Dict[str, Any]:
        user_services = self._build_user_services(modality, events)
        service_index = {record["service_id"]: record for record in user_services}

        if modality in self.TRANSPORT_MODALITIES:
            transports = self._build_transports(modality, events, service_index)
        else:
            transports = []

        result: Dict[str, Any] = {
            "schema_version": self.SCHEMA_VERSION,
            "simulation_id": simulation_id,
            "modality": modality,
            "summary": self._build_summary(modality, user_services, transports),
            "user_services": user_services,
            "transports": transports,
        }

        if modality == "electric_taxi":
            charging_sessions = self._build_charging_sessions(events)
            collisions = sorted(
                set(service_index).intersection(
                    session["charging_id"] for session in charging_sessions
                )
            )
            if collisions:
                self._warn(
                    "Electric Taxi charging_id/service_id collision(s): {}.".format(collisions)
                )
            result["charging_sessions"] = charging_sessions
            charging_count_by_transport: Dict[str, int] = {}
            for session in charging_sessions:
                transport_id = session.get("transport_id")
                if transport_id is not None:
                    charging_count_by_transport[transport_id] = (
                        charging_count_by_transport.get(transport_id, 0) + 1
                    )
            for transport in result["transports"]:
                transport["charging_sessions"] = int(
                    charging_count_by_transport.get(transport["transport_id"], 0)
                )

        return result

    # ------------------------------------------------------------------
    # User services
    # ------------------------------------------------------------------

    def _build_user_services(self, modality: str, events: List[Any]) -> List[Dict[str, Any]]:
        service_events: Dict[str, List[Any]] = {}
        movement_events: Dict[str, List[Any]] = {}

        for event in events:
            details = self._details(event)
            service_id = details.get("service_id")
            if service_id is None:
                continue
            service_id = str(service_id)
            if event.event_type in self.SERVICE_EVENTS:
                service_events.setdefault(service_id, []).append(event)
            elif event.event_type == "movement_completed":
                movement_events.setdefault(service_id, []).append(event)

        records = []
        for service_id in sorted(service_events):
            lifecycle = service_events[service_id]
            requests = [event for event in lifecycle if event.event_type == "service_requested"]
            if not requests:
                self._warn(
                    "Service {} ({}) has lifecycle events but no service_requested; not published.".format(
                        service_id, modality
                    )
                )
                continue
            if len(requests) != 1:
                self._warn(
                    "Service {} ({}) contains {} service_requested events; first is authoritative.".format(
                        service_id, modality, len(requests)
                    )
                )

            request = requests[0]
            request_details = self._details(request)
            user_id = self._bare_jid(request_details.get("user_id"))

            for event in lifecycle:
                event_user = self._bare_jid(self._details(event).get("user_id"))
                if event_user is not None and user_id is not None and event_user != user_id:
                    self._warn(
                        "Service {} ({}) contains conflicting user_id values.".format(
                            service_id, modality
                        )
                    )
                    break

            assigned_events = [event for event in lifecycle if event.event_type == "service_assigned"]
            started_events = [event for event in lifecycle if event.event_type == "service_started"]
            completed_events = [event for event in lifecycle if event.event_type == "service_completed"]
            failed_events = [event for event in lifecycle if event.event_type == "service_failed"]

            if len(started_events) > 1:
                self._warn(
                    "Service {} ({}) contains duplicate service_started events.".format(
                        service_id, modality
                    )
                )
            if len(completed_events) > 1 or len(failed_events) > 1:
                self._warn(
                    "Service {} ({}) contains duplicate terminal events.".format(
                        service_id, modality
                    )
                )
            if completed_events and failed_events:
                self._warn(
                    "Service {} ({}) contains both completed and failed terminals; earliest wins.".format(
                        service_id, modality
                    )
                )

            terminal_candidates = completed_events + failed_events
            terminal = terminal_candidates[0] if terminal_candidates else None
            if terminal_candidates:
                terminal = min(
                    terminal_candidates,
                    key=lambda event: self._timestamp_seconds(
                        event.timestamp, "terminal {}".format(service_id)
                    )
                    if self._timestamp_seconds(event.timestamp, "terminal {}".format(service_id)) is not None
                    else math.inf,
                )

            status = "unfinished"
            if terminal is not None:
                status = "completed" if terminal.event_type == "service_completed" else "failed"

            requested_at = self._timestamp_seconds(request.timestamp, "request {}".format(service_id))
            assigned_at = self._first_timestamp(assigned_events, "assignment {}".format(service_id))
            started_at = self._first_timestamp(started_events, "start {}".format(service_id))
            ended_at = (
                self._timestamp_seconds(terminal.timestamp, "terminal {}".format(service_id))
                if terminal is not None
                else None
            )

            if assigned_at is not None and requested_at is not None and assigned_at < requested_at:
                self._warn("Service {} assignment precedes request.".format(service_id))
            if started_at is not None and requested_at is not None and started_at < requested_at:
                self._warn("Service {} start precedes request.".format(service_id))
            if status == "completed" and ended_at is not None and started_at is not None and ended_at < started_at:
                self._warn("Completed service {} terminates before start.".format(service_id))

            service_movements = movement_events.get(service_id, [])
            distances = self._distance_totals(service_movements)

            transport_ids = set()
            for event in lifecycle + service_movements:
                transport_id = self._bare_jid(self._details(event).get("transport_id"))
                if transport_id is not None:
                    transport_ids.add(transport_id)

            if modality != "public_transport" and len(transport_ids) > 1:
                self._warn(
                    "Service {} ({}) references multiple transports: {}.".format(
                        service_id, modality, sorted(transport_ids)
                    )
                )

            terminal_details = self._details(terminal) if terminal is not None else {}
            record: Dict[str, Any] = {
                "service_id": service_id,
                "user_id": user_id,
                "status": status,
                "transport_ids": sorted(transport_ids),
                "requested_at": requested_at,
                "assigned_at": assigned_at,
                "started_at": started_at,
                "ended_at": ended_at,
                "waiting_time_s": self._safe_delta(
                    requested_at, started_at, "service {} waiting_time_s".format(service_id)
                ),
                "service_time_s": self._safe_delta(
                    started_at, ended_at, "service {} service_time_s".format(service_id)
                ),
                "total_time_s": self._safe_delta(
                    requested_at, ended_at, "service {} total_time_s".format(service_id)
                ),
                "failure_reason": terminal_details.get("failure_reason") if status == "failed" else None,
                "origin": self._safe_json_value(
                    request_details.get("origin"), "service {} origin".format(service_id)
                ),
                "destination": self._safe_json_value(
                    request_details.get("destination"), "service {} destination".format(service_id)
                ),
                "total_recorded_distance_m": distances["total_distance_m"],
                "approach_distance_m": distances["approach_distance_m"],
                "service_distance_m": distances["service_distance_m"],
                "auxiliary_distance_m": distances["auxiliary_distance_m"],
            }

            if modality == "sharing":
                if any(
                    self._details(event).get("station_id") is not None
                    or self._details(event).get("failure_operation") is not None
                    for event in lifecycle
                ):
                    self._warn(
                        "Free-floating sharing service {} contains station-specific fields; omitted.".format(
                            service_id
                        )
                    )

            if modality == "public_transport":
                record.update(
                    self._public_transport_user_fields(
                        lifecycle=lifecycle,
                        movements=service_movements,
                        terminal_details=terminal_details,
                    )
                )
            elif modality == "station_sharing":
                failure_operation = (
                    terminal_details.get("failure_operation") if status == "failed" else None
                )
                station_id = (
                    self._bare_jid(terminal_details.get("station_id")) if status == "failed" else None
                )
                if (
                    status == "failed"
                    and terminal_details.get("failure_reason") == "station_operation_failed"
                ):
                    if failure_operation not in {"pick", "drop"}:
                        self._warn(
                            "Station-sharing service {} has invalid/missing failure_operation {!r}.".format(
                                service_id, failure_operation
                            )
                        )
                    if station_id is None:
                        self._warn(
                            "Station-sharing service {} has a station operation failure without station_id.".format(
                                service_id
                            )
                        )
                record["failure_operation"] = failure_operation
                record["station_id"] = station_id

            records.append(record)

        return records

    def _first_timestamp(self, events: List[Any], context: str) -> Optional[float]:
        timestamps = [
            self._timestamp_seconds(event.timestamp, context)
            for event in events
        ]
        valid = [value for value in timestamps if value is not None]
        return min(valid) if valid else None

    def _distance_totals(self, movements: Iterable[Any]) -> Dict[str, float]:
        totals = {phase: 0.0 for phase in self.MOVEMENT_PHASES}
        for event in movements:
            details = self._details(event)
            phase = details.get("phase")
            distance = self._non_negative_number(details.get("distance_m"))
            if phase in totals and distance is not None:
                totals[phase] += distance
        return {
            "total_distance_m": float(sum(totals.values())),
            "approach_distance_m": float(totals["approach"]),
            "service_distance_m": float(totals["service"]),
            "auxiliary_distance_m": float(totals["auxiliary"]),
        }

    def _public_transport_user_fields(
        self,
        lifecycle: List[Any],
        movements: List[Any],
        terminal_details: Dict[str, Any],
    ) -> Dict[str, Any]:
        assignments = [event for event in lifecycle if event.event_type == "service_assigned"]
        boardings = len(assignments)

        walking_distance = 0.0
        leg_indexes = set()
        stops = set()
        for event in lifecycle + movements:
            details = self._details(event)
            if details.get("leg_index") is not None:
                leg_indexes.add(str(details.get("leg_index")))
            for field in ("origin_stop", "destination_stop"):
                value = details.get(field)
                if value is not None:
                    stops.add(self._bare_jid(value) or str(value))
            if (
                event.event_type == "movement_completed"
                and details.get("movement_mode") == "walking"
            ):
                distance = self._non_negative_number(details.get("distance_m"))
                if distance is not None:
                    walking_distance += distance

        transfers = terminal_details.get("transfers")
        if isinstance(transfers, bool) or not isinstance(transfers, int) or transfers < 0:
            transfers = max(boardings - 1, 0)

        public_transport_stops = terminal_details.get("public_transport_stops")
        if (
            isinstance(public_transport_stops, bool)
            or not isinstance(public_transport_stops, int)
            or public_transport_stops < 0
        ):
            public_transport_stops = len(stops)

        legs = terminal_details.get("legs")
        if isinstance(legs, bool) or not isinstance(legs, int) or legs < 0:
            legs = len(leg_indexes)

        return {
            "boardings": int(boardings),
            "transfers": int(transfers),
            "walking_distance_m": float(walking_distance),
            "public_transport_stops": int(public_transport_stops),
            "legs": int(legs),
        }

    # ------------------------------------------------------------------
    # Transport records
    # ------------------------------------------------------------------

    def _build_transports(
        self,
        modality: str,
        events: List[Any],
        service_index: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        transport_ids = set()
        for event in events:
            details = self._details(event)
            transport_id = self._bare_jid(details.get("transport_id"))
            if transport_id is not None:
                transport_ids.add(transport_id)
            if event.event_type == "movement_completed":
                emitter = self._bare_jid(getattr(event, "name", None))
                if transport_id is not None and emitter is not None:
                    transport_ids.add(transport_id)

        records = []
        for transport_id in sorted(transport_ids):
            emitted = [
                event
                for event in events
                if self._bare_jid(getattr(event, "name", None)) == transport_id
            ]
            related = [
                event
                for event in events
                if self._bare_jid(self._details(event).get("transport_id")) == transport_id
            ]
            transport_movements = [
                event
                for event in related
                if event.event_type == "movement_completed"
                and self._bare_jid(getattr(event, "name", None)) == transport_id
            ]

            service_ids = {
                str(self._details(event).get("service_id"))
                for event in related
                if self._details(event).get("service_id") is not None
            }
            known_services = [
                service_index[service_id]
                for service_id in sorted(service_ids)
                if service_id in service_index
            ]

            assignments = sum(
                1
                for event in emitted
                if event.event_type == "service_assigned"
                and self._bare_jid(self._details(event).get("transport_id")) == transport_id
            )
            distances = self._distance_totals(transport_movements)

            class_type = None
            for event in emitted:
                class_type = getattr(event, "class_type", None)
                if class_type:
                    break

            record: Dict[str, Any] = {
                "transport_id": transport_id,
                "class_type": class_type,
                "assignments": int(assignments),
                "started_services": int(
                    sum(1 for service in known_services if service.get("started_at") is not None)
                ),
                "completed_services": int(
                    sum(1 for service in known_services if service.get("status") == "completed")
                ),
                "failed_services": int(
                    sum(1 for service in known_services if service.get("status") == "failed")
                ),
                "unique_services": int(len(service_ids)),
                "movement_count": int(len(transport_movements)),
                "total_distance_m": distances["total_distance_m"],
                "approach_distance_m": distances["approach_distance_m"],
                "service_distance_m": distances["service_distance_m"],
                "auxiliary_distance_m": distances["auxiliary_distance_m"],
            }

            if modality == "public_transport":
                record.update(self._public_transport_transport_fields(transport_movements, assignments))

            records.append(record)

        return records

    def _public_transport_transport_fields(
        self,
        movements: List[Any],
        assignments: int,
    ) -> Dict[str, Any]:
        distance_with_users = 0.0
        distance_without_users = 0.0
        weighted_onboard = 0.0
        total_distance = 0.0
        weighted_capacity = 0.0

        for event in movements:
            details = self._details(event)
            distance = self._non_negative_number(details.get("distance_m"))
            if distance is None:
                continue
            onboard = details.get("onboard")
            capacity = details.get("capacity")

            try:
                onboard_number = float(onboard)
            except (TypeError, ValueError):
                onboard_number = None
            try:
                capacity_number = float(capacity)
            except (TypeError, ValueError):
                capacity_number = None

            if onboard_number is not None and math.isfinite(onboard_number) and onboard_number >= 0:
                if onboard_number > 0:
                    distance_with_users += distance
                else:
                    distance_without_users += distance
                weighted_onboard += onboard_number * distance
                total_distance += distance
            else:
                self._warn("Public transport movement has invalid onboard value; occupancy ignores it.")

            if (
                capacity_number is not None
                and math.isfinite(capacity_number)
                and capacity_number > 0
                and onboard_number is not None
                and math.isfinite(onboard_number)
                and onboard_number >= 0
            ):
                if onboard_number > capacity_number:
                    self._warn("Public transport movement has onboard > capacity.")
                weighted_capacity += capacity_number * distance

        average_occupancy = (
            float(weighted_onboard / total_distance) if total_distance > 0 else None
        )
        occupancy_rate = (
            float(weighted_onboard / weighted_capacity) if weighted_capacity > 0 else None
        )

        return {
            "boardings": int(assignments),
            "distance_with_users_m": float(distance_with_users),
            "distance_without_users_m": float(distance_without_users),
            "average_occupancy": average_occupancy,
            "occupancy_rate": occupancy_rate,
        }

    # ------------------------------------------------------------------
    # Electric charging
    # ------------------------------------------------------------------

    def _build_charging_sessions(self, events: List[Any]) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[Any]] = {}
        for event in events:
            if event.event_type not in self.CHARGING_EVENTS:
                continue
            charging_id = str(self._details(event)["charging_id"])
            grouped.setdefault(charging_id, []).append(event)

        sessions = []
        for charging_id in sorted(grouped):
            session_events = grouped[charging_id]
            arrived = [event for event in session_events if event.event_type == "charging_arrived"]
            started = [event for event in session_events if event.event_type == "charging_started"]
            completed = [event for event in session_events if event.event_type == "charging_completed"]

            for event_type, matches in (
                ("charging_arrived", arrived),
                ("charging_started", started),
                ("charging_completed", completed),
            ):
                if len(matches) > 1:
                    self._warn(
                        "Charging session {} contains duplicate {} events.".format(
                            charging_id, event_type
                        )
                    )

            first_event = session_events[0]
            base_details = self._details(first_event)
            transport_id = self._bare_jid(base_details.get("transport_id"))
            station_id = self._bare_jid(base_details.get("station_id"))

            for event in session_events:
                details = self._details(event)
                if self._bare_jid(details.get("transport_id")) != transport_id:
                    self._warn("Charging session {} has conflicting transport_id.".format(charging_id))
                if self._bare_jid(details.get("station_id")) != station_id:
                    self._warn("Charging session {} has conflicting station_id.".format(charging_id))

            arrived_at = self._first_timestamp(arrived, "charging {} arrived".format(charging_id))
            started_at = self._first_timestamp(started, "charging {} started".format(charging_id))
            ended_at = self._first_timestamp(completed, "charging {} completed".format(charging_id))

            if started_at is not None and arrived_at is not None and started_at < arrived_at:
                self._warn("Charging session {} starts before arrival.".format(charging_id))
            if ended_at is not None and started_at is not None and ended_at < started_at:
                self._warn("Charging session {} completes before start.".format(charging_id))

            sessions.append(
                {
                    "charging_id": charging_id,
                    "transport_id": transport_id,
                    "station_id": station_id,
                    "status": "completed" if completed else "unfinished",
                    "arrived_at": arrived_at,
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "waiting_time_s": self._safe_delta(
                        arrived_at, started_at, "charging {} waiting_time_s".format(charging_id)
                    ),
                    "charging_time_s": self._safe_delta(
                        started_at, ended_at, "charging {} charging_time_s".format(charging_id)
                    ),
                    "total_time_s": self._safe_delta(
                        arrived_at, ended_at, "charging {} total_time_s".format(charging_id)
                    ),
                }
            )

        return sessions

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def _build_summary(
        self,
        modality: str,
        user_services: List[Dict[str, Any]],
        transports: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        total_services = len(user_services)
        completed = [service for service in user_services if service["status"] == "completed"]
        failed = [service for service in user_services if service["status"] == "failed"]
        unfinished = [service for service in user_services if service["status"] == "unfinished"]

        if total_services:
            completion_rate = float(len(completed) / total_services)
            failure_rate = float(len(failed) / total_services)
        else:
            completion_rate = None
            failure_rate = None

        if modality in {"sharing", "station_sharing"}:
            distance_source = user_services
            total_distance_field = "total_recorded_distance_m"
        else:
            distance_source = transports
            total_distance_field = "total_distance_m"

        return {
            "total_users": int(
                len({service["user_id"] for service in user_services if service["user_id"] is not None})
            ),
            "total_transports": int(len(transports)),
            "total_services": int(total_services),
            "completed_services": int(len(completed)),
            "failed_services": int(len(failed)),
            "unfinished_services": int(len(unfinished)),
            "completion_rate": completion_rate,
            "failure_rate": failure_rate,
            "average_waiting_time_s": self._mean(
                service["waiting_time_s"] for service in completed
            ),
            "average_service_time_s": self._mean(
                service["service_time_s"] for service in completed
            ),
            "average_total_time_s": self._mean(
                service["total_time_s"] for service in completed
            ),
            "total_distance_m": float(
                sum(float(record.get(total_distance_field, 0.0)) for record in distance_source)
            ),
            "approach_distance_m": float(
                sum(float(record.get("approach_distance_m", 0.0)) for record in distance_source)
            ),
            "service_distance_m": float(
                sum(float(record.get("service_distance_m", 0.0)) for record in distance_source)
            ),
            "auxiliary_distance_m": float(
                sum(float(record.get("auxiliary_distance_m", 0.0)) for record in distance_source)
            ),
        }
