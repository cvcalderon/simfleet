import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Callable


class Event:
    """
    Represent one timestamped event emitted by a SimFleet agent.

    An Event stores the emitting agent name, canonical or application-specific
    event type, emitting agent class, timestamp, and an extensible ``details``
    mapping.

    Events are collected locally by StatisticsStore and later merged by the
    Simulator into a global Log for metrics processing.
    """

    def __init__(self, name: str, event_type: str, class_type: str, timestamp: Optional[str] = None, details: Optional[Dict] = None):
        """
        Initialize one statistics event.

        Args:
            name (str): Name or JID of the emitting agent.
            event_type (str): Event identifier.
            class_type: Class object of the emitting agent. Its ``__name__`` is
                stored in the event.
            timestamp (str | None): Optional ISO-formatted timestamp. When omitted,
                the current local datetime is used.
            details (dict | None): Event-specific payload.
        """
        self.name = name  # Corresponds to "name" in your logs
        self.event_type = event_type  # Corresponds to "event" in your logs
        self.class_type = class_type.__name__  # New parameter to store the type of class
        self.timestamp = datetime.fromisoformat(timestamp) if timestamp else datetime.now()
        self.details = details if details else {}  # Corresponds to "details" in your logs

    def to_dict(self) -> Dict:
        """
        Serialize the event into a dictionary.

        The timestamp object is preserved as stored; this method does not perform
        JSON conversion or timestamp normalization.

        Returns:
            dict: Event fields and details payload.
        """
        return {
            "name": self.name,
            "timestamp": self.timestamp,
            "event_type": self.event_type,
            "class_type": self.class_type,
            "details": self.details
        }


class StatisticsStore:
    """
    Store events emitted by one SimFleet agent.

    Every SimfleetAgent owns one StatisticsStore. Events remain local to that
    agent during simulation execution and are merged into the Simulator global
    Log when statistics are generated.

    StatisticsStore performs collection only; it does not interpret event
    semantics or calculate KPIs.
    """

    def __init__(self, agent_name: str, class_type: str):
        """
        Initialize an empty per-agent event store.

        Args:
            agent_name (str): Name or JID associated with emitted events.
            class_type: Class object representing the emitting agent type.
        """
        self.store = []
        self.agent_name = agent_name
        self.class_type = class_type  # Store the type of the agent for use in events

    def get_agent_name(self) -> str:
        """
        Return the name associated with this event store.

        Returns:
            str: Agent name or JID.
        """
        return self.agent_name

    def emit(self, event_type: str, details: Optional[Dict] = None, timestamp: Optional[str] = None) -> None:
        """
        Append one event to this agent's statistics store.

        The method records the event exactly as supplied. It does not validate
        canonical event names or modality-specific payload fields; that validation
        belongs to downstream statistics processors.

        Args:
            event_type (str): Event identifier.
            details (dict | None): Event-specific payload.
            timestamp (str | None): Optional ISO-formatted event timestamp.
        """
        event = Event(name=self.agent_name, event_type=event_type, class_type=self.class_type, timestamp=timestamp, details=details)
        self.store.append(event)

    def all(self, limit: Optional[int] = None) -> List[Event]:
        """
        Return events stored by this agent in insertion order.

        Args:
            limit (int | None): Maximum number of events to return. ``None``
                returns the complete store.

        Returns:
            list[Event]: Stored events up to the requested limit.
        """
        return self.store[:limit]

    def all_events(self) -> List[Dict]:
        """
        Return all locally stored events serialized as dictionaries.

        Returns:
            list[dict]: Serialized events in insertion order.
        """
        return [event.to_dict() for event in self.store]

    def generate_partial_log(self) -> 'Log':
        """
        Build a Log containing all events emitted by this agent.

        The Simulator uses these partial logs to assemble its global event Log
        after simulation execution.

        Returns:
            Log: Log containing this store's current events.
        """
        return Log(self.all())


class Log:
    """
    Collection of SimFleet events supporting aggregation and analysis.

    Log is the common exchange object between per-agent StatisticsStore
    instances, Simulator event aggregation, and statistics processors.

    It provides filtering, event merging, timestamp normalization, ordering,
    field removal, dictionary serialization, and DataFrame conversion.

    Most filtering operations return a new Log. ``drop()``,
    ``add_events()``, ``adjust_timestamps()``, and ``sort_by_timestamp()``
    mutate the current Log or the Event objects it contains.
    """

    def __init__(self, events: Optional[List[Event]] = None):
        """
        Initialize a Log from an optional list of events.

        Args:
            events (list[Event] | None): Initial event collection.
        """
        self.events = events if events else []

    def filter(self, criterion: Callable[[Event], bool]) -> 'Log':
        """
        Return a new Log containing events accepted by a predicate.

        Args:
            criterion: Callable receiving one Event and returning True when the
                event must be retained.

        Returns:
            Log: Filtered event collection.
        """
        filtered_events = [event for event in self.events if criterion(event)]
        return Log(filtered_events)

    def filter_by_name(self, name: str) -> 'Log':
        """
        Return events emitted by one agent name.

        Args:
            name (str): Agent name or JID.

        Returns:
            Log: Matching events.
        """
        return self.filter(lambda event: event.name == name)

    def filter_by_class_type(self, class_type: str) -> 'Log':
        """
        Return events emitted by one stored agent class name.

        Args:
            class_type (str): Agent class name.

        Returns:
            Log: Matching events.
        """
        return self.filter(lambda event: event.class_type == class_type)

    def filter_by_event_type(self, event_type: str) -> 'Log':
        """
        Return events with one event identifier.

        Args:
            event_type (str): Event type to retain.

        Returns:
            Log: Matching events.
        """
        return self.filter(lambda event: event.event_type == event_type)

    def filter_by_time_window(self, start_time: datetime, end_time: datetime) -> 'Log':
        """
        Return events whose timestamps fall inside an inclusive time window.

        Args:
            start_time (datetime): Inclusive lower timestamp bound.
            end_time (datetime): Inclusive upper timestamp bound.

        Returns:
            Log: Events satisfying ``start_time <= timestamp <= end_time``.
        """
        return self.filter(lambda event: start_time <= event.timestamp <= end_time)

    def drop(self, fields: List[str]) -> 'Log':
        """
        Remove selected keys from every event ``details`` mapping in place.

        Args:
            fields (list[str]): Detail keys to remove.

        Returns:
            Log: This same Log instance after mutation.
        """
        for event in self.events:
            for field in fields:
                if field in event.details:
                    del event.details[field]
        return self

    def all_events(self) -> List[Dict]:
        """
        Serialize every event in this Log as a dictionary.

        Returns:
            list[dict]: Serialized events in current Log order.
        """
        return [event.to_dict() for event in self.events]

    def add_events(self, other_log: 'Log') -> None:
        """
        Append events from another Log to this Log.

        Event objects are reused rather than copied. Subsequent mutation of those
        Event objects is therefore visible through both logs.

        Args:
            other_log (Log): Source log whose events are appended.
        """
        self.events.extend(other_log.events)

    def adjust_timestamps(self, simulator_timestamp: str) -> None:
        """
        Normalize event timestamps relative to simulation start time.

        ``simulator_timestamp`` is interpreted as a Unix timestamp in seconds.

        Event timestamps are handled as follows:

        - ``datetime`` values are converted to elapsed seconds relative to the
          simulation start;
        - existing ``float`` values are preserved unchanged;
        - unsupported timestamp types raise TypeError.

        The Event objects are mutated in place.

        Args:
            simulator_timestamp (str): Simulation start Unix timestamp expressed
                as a string.

        Raises:
            TypeError: If an event contains an unsupported timestamp type.
        """
        # Convert the simulator timestamp
        simulator_time = datetime.fromtimestamp(float(simulator_timestamp))

        for event in self.events:
            if isinstance(event.timestamp, float):
                event.timestamp = event.timestamp
            elif isinstance(event.timestamp, datetime):
                delta = event.timestamp - simulator_time
                event.timestamp = delta.total_seconds()
            else:
                # If the timestamp is of another type, raise an error or handle it
                raise TypeError(f"Unsupported timestamp type: {type(event.timestamp)}")

    def sort_by_timestamp(self, reverse: bool = False) -> None:
        """
        Sort this Log in place by event timestamp.

        Args:
            reverse (bool): Sort descending when True; ascending when False.
        """
        self.events.sort(key=lambda event: event.timestamp, reverse=reverse)

    def to_dataframe(self, event_fields: List[str], details_fields: List[str]) -> pd.DataFrame:
        """
        Convert selected Event and ``details`` fields into a pandas DataFrame.

        Missing Event attributes or detail keys are represented as ``None``.
        Output column order follows ``event_fields`` followed by
        ``details_fields``.

        Args:
            event_fields (list[str]): Event attributes to include.
            details_fields (list[str]): Keys extracted from each Event ``details``
                mapping.

        Returns:
            pandas.DataFrame: Tabular representation of the selected fields.
        """
        # Initialize an empty list to store processed event data
        data = []

        for event in self.events:
            # Extract the specified fields from the event
            row = {}
            for field in event_fields:
                row[field] = getattr(event, field, None)
            # Extract the specified fields from the "details"
            details_data = {}

            for field in details_fields:
                details_data[field] = event.details.get(field, None)
            # Merge the event fields and details fields into a single row
            row.update(details_data)
            # Append the processed row to the data list
            data.append(row)

        # Create and return the DataFrame
        return pd.DataFrame(data, columns=event_fields + details_fields)
