import pandas as pd
from tabulate import tabulate
from simfleet.metrics.basestatistics import BaseStatisticsClass
from simfleet.utils.statistics import Log


class MobilityStatisticsClass(BaseStatisticsClass):

    def run(self, events_log: Log) -> None:
        """
        Run the statistics generation process.

        Args:
            events_log (Log):
                Log containing all simulation events.
        """

        self.transport_metrics(events_log,"simfleet_metrics_transport.json")

        self.taxi_metrics(events_log,"simfleet_metrics_taxi.json")

        self.electric_taxi_metrics(events_log,"simfleet_metrics_electrictaxi.json")

        self.customer_taxi_metrics(events_log,"simfleet_metrics_taxicustomer.json")

        self.public_transport_metrics(events_log,"simfleet_metrics_publictransport.json")

        self.print_stats()

    def mean_or_zero(self, series) -> float:
        if series is None or len(series) == 0:
            return 0.0

        value = series.mean()

        if pd.isna(value):
            return 0.0

        if hasattr(value, "total_seconds"):
            return float(value.total_seconds())

        return float(value)

    def empty_dataframe(self, columns):
        return pd.DataFrame(columns=columns)

    def transport_like_metrics(
        self,
        events_log: Log,
        class_type: str,
        dataframe_attr: str,
        file_path: str,
        json_key: str,
    ) -> None:
        """
        Generates common transport metrics for agents that emit the delivery/taxi
        mobility event vocabulary.
        """
        relevant_events = {
            "travel_to_pickup",
            "customer_pickup",
            "travel_to_destination",
            "trip_completion",
        }

        filtered_events = events_log.filter(
            lambda event: event.class_type == class_type
            and event.event_type in relevant_events
        )

        event_fields = ["name", "timestamp", "event_type", "class_type"]
        details_fields = ["distance", "duration"]
        dataframe = filtered_events.to_dataframe(
            event_fields=event_fields,
            details_fields=details_fields,
        )

        if dataframe.empty:
            result_df = self.empty_dataframe(
                [
                    "name",
                    "class_type",
                    "completed_trips",
                    "pickup_distance",
                    "customer_distance",
                    "total_distance",
                    "pickup_duration",
                ]
            )
        else:
            completed_trips = (
                dataframe[dataframe["event_type"] == "trip_completion"]
                .groupby("name")
                .size()
            )
            pickup_distance = (
                dataframe[dataframe["event_type"] == "travel_to_pickup"]
                .groupby("name")["distance"]
                .sum()
            )
            customer_distance = (
                dataframe[dataframe["event_type"] == "travel_to_destination"]
                .groupby("name")["distance"]
                .sum()
            )
            total_distance = dataframe[
                dataframe["event_type"].isin(["travel_to_pickup", "travel_to_destination"])
            ].groupby("name")["distance"].sum()
            pickup_duration = (
                dataframe[dataframe["event_type"] == "travel_to_pickup"]
                .groupby("name")["duration"]
                .sum()
            )

            result_df = pd.DataFrame(
                {
                    "name": dataframe.groupby("name")["name"].first(),
                    "class_type": dataframe.groupby("name")["class_type"].first(),
                    "completed_trips": completed_trips,
                    "pickup_distance": pickup_distance,
                    "customer_distance": customer_distance,
                    "total_distance": total_distance,
                    "pickup_duration": pickup_duration,
                }
            ).fillna(0)

            result_df = result_df.reset_index(drop=True)

        setattr(self, dataframe_attr, result_df)

        agent_metrics = result_df.to_dict(orient="records")
        numbered_agents = {str(i): agent_metrics[i] for i in range(len(agent_metrics))}

        json_structure = {
            "GeneralMetrics": {
                "Class type": class_type,
                "Agents": len(agent_metrics),
                "Total Completed Trips": (
                    int(result_df["completed_trips"].sum())
                    if "completed_trips" in result_df
                    else 0
                ),
                "Avg Total Distance": (
                    f"{self.mean_or_zero(result_df['total_distance']) if 'total_distance' in result_df else 0.0:.2f}"
                ),
            },
            json_key: numbered_agents,
        }

        self.export_to_json(json_structure, file_path)

    def transport_metrics(self, events_log: Log, file_path: str) -> None:
        self.transport_like_metrics(
            events_log=events_log,
            class_type="TransportAgent",
            dataframe_attr="transport_df",
            file_path=file_path,
            json_key="TransportAgent",
        )

    def taxi_metrics(self, events_log: Log, file_path: str) -> None:
        """
        Generates mobility metrics for TaxiAgent.

        Args:
            events_log (Log): A log containing all events from the simulation.
            file_path (str): The path where the final JSON file will be exported.
        """
        self.transport_like_metrics(
            events_log=events_log,
            class_type="TaxiAgent",
            dataframe_attr="taxi_df",
            file_path=file_path,
            json_key="TaxiAgent",
        )

    def electric_taxi_metrics(self, events_log: Log, file_path: str) -> None:
        """
        Generates mobility metrics for ElectricTaxiAgent.

        Args:
            events_log (Log): A log containing all events from the simulation.
            file_path (str): The path where the final JSON file will be exported.
        """
        self.transport_like_metrics(
            events_log=events_log,
            class_type="ElectricTaxiAgent",
            dataframe_attr="electrictaxi_df",
            file_path=file_path,
            json_key="ElectricTaxiAgent",
        )

    def customer_taxi_metrics(self, events_log: Log, file_path: str) -> None:
        """
        Combines the calculation, JSON generation, and export process for TaxiCustomerAgent.

        Args:
            events_log (Log): A log containing all events from the simulation.
            file_path (str): The path where the final JSON file will be exported.
        """
        # Filtering relevant events for TaxiCustomerAgent
        filtered_events = events_log.filter(
            lambda event: event.class_type == "TaxiCustomerAgent"
            and event.event_type
            in {"customer_request", "customer_pickup", "trip_completion"}
        )

        # Transform events into a DataFrame
        event_fields = ["name", "timestamp", "event_type", "class_type"]
        details_fields = []
        dataframe = filtered_events.to_dataframe(
            event_fields=event_fields,
            details_fields=details_fields,
        )

        # Using pivot table to calculate waiting time and total trip time
        pivot_df = dataframe.pivot_table(
            index="name",
            columns="event_type",
            values="timestamp",
            aggfunc="first",
        )

        if dataframe.empty or "customer_request" not in pivot_df.columns:
            waiting_time = 0
            total_time = 0
        else:
            waiting_time = (
                pivot_df["customer_pickup"] - pivot_df["customer_request"]
                if "customer_pickup" in pivot_df.columns
                else 0
            )
            total_time = (
                pivot_df["trip_completion"] - pivot_df["customer_request"]
                if "trip_completion" in pivot_df.columns
                else 0
            )

        # Combining all metrics into a final DataFrame
        result_df = pd.DataFrame(
            {
                "name": dataframe.groupby("name")["name"].first(),
                "class_type": dataframe.groupby("name")["class_type"].first(),
                "waiting_time": waiting_time,
                "total_time": total_time,
            }
        ).fillna(0)

        for column in ["waiting_time", "total_time"]:
            if column in result_df and pd.api.types.is_timedelta64_dtype(result_df[column]):
                result_df[column] = result_df[column].dt.total_seconds()

        # Clear duplicate column names
        self.taxicustomer_df = result_df.reset_index(drop=True)

        # Calculating general averages for the "GeneralMetrics" section
        avg_waiting_time = self.mean_or_zero(self.taxicustomer_df["waiting_time"])
        avg_total_time = self.mean_or_zero(self.taxicustomer_df["total_time"])

        # Convert the DataFrame into a dictionary structure indexed by an agent number
        agent_metrics = self.taxicustomer_df.to_dict(orient="records")

        # Convert the agent metrics into a dictionary with numeric keys (0, 1, 2, ...)
        numbered_agents = {str(i): agent_metrics[i] for i in range(len(agent_metrics))}

        # Converting the result DataFrame into a JSON-like structure
        json_structure = {
            "GeneralMetrics": {
                "Class type": "TaxiCustomerAgent",
                "Avg Waiting Time": f"{avg_waiting_time:.2f}",
                "Avg Total Time": f"{avg_total_time:.2f}"
            },
            "TaxiCustomerAgent": numbered_agents
        }

        # Exporting the result to a JSON file
        self.export_to_json(json_structure, file_path)

    def count_public_transport_pattern_runs(
        self,
        events
    ):
        """
        Count actual completed Public Transport Pattern runs.

        A Pattern is considered completed only when:

        1. a pt_stop_arrival event marks
           pattern_completed=True, and

        2. at least one pt_segment_completed event
           for that Pattern has occurred since its
           previous valid completion.

        This prevents an initial Vehicle position at
        the terminal Stop from being counted as a
        completed Pattern run.
        """

        events = sorted(
            events,
            key=lambda event:
            event.timestamp
        )

        pattern_has_segment = {}

        completed_runs = 0

        for event in events:

            details = (
                event.details
                or {}
            )

            pattern_id = (
                details.get(
                    "pattern_id"
                )
            )

            if pattern_id is None:
                continue

            if (
                event.event_type
                == "pt_segment_completed"
            ):
                pattern_has_segment[
                    pattern_id
                ] = True

                continue

            if (
                event.event_type
                != "pt_stop_arrival"
            ):
                continue

            if not bool(
                details.get(
                    "pattern_completed",
                    False
                )
            ):
                continue

            if not pattern_has_segment.get(
                pattern_id,
                False
            ):
                continue

            completed_runs += 1

            pattern_has_segment[
                pattern_id
            ] = False

        return completed_runs


    def public_transport_metrics(
        self,
        events_log: Log,
        file_path: str
    ) -> None:
        """
        Generate basic Public Transport statistics.

        Metrics are derived only from Public Transport
        events already stored in the global simulation Log.

        No Public Transport agent is queried and no
        operational state is modified.
        """

        #
        # Event vocabulary
        #

        vehicle_event_types = {
            "pt_stop_arrival",
            "pt_segment_completed",
            "pt_customer_boarded",
            "pt_customer_alighted",
        }

        customer_event_types = {
            "pt_journey_started",
            "pt_queue_entered",
            "pt_boarded",
            "pt_alighted",
            "pt_journey_completed",
            "pt_journey_failed",
        }

        #
        # We intentionally classify by event type rather
        # than by class name.
        #
        # This allows future specialized Public Transport
        # agents such as Bus/Metro/Tram agents to reuse
        # the same statistics vocabulary.
        #

        vehicle_events = [
            event
            for event in events_log.events
            if event.event_type
               in vehicle_event_types
        ]

        customer_events = [
            event
            for event in events_log.events
            if event.event_type
               in customer_event_types
        ]

        #
        # Local helpers
        #

        def timestamp_delta_seconds(
            start,
            end
        ):

            if (
                start is None
                or end is None
            ):
                return 0.0

            value = end - start

            if hasattr(
                value,
                "total_seconds"
            ):
                value = (
                    value.total_seconds()
                )

            return max(
                0.0,
                float(value)
            )

        def first_event(
            events,
            event_type
        ):

            matches = [
                event
                for event in events
                if event.event_type
                   == event_type
            ]

            if not matches:
                return None

            return min(
                matches,
                key=lambda event:
                event.timestamp
            )

        #
        # ==========================================================
        # VEHICLE METRICS
        # ==========================================================
        #

        vehicle_records = []

        vehicle_names = sorted(
            {
                event.name
                for event in vehicle_events
            }
        )

        for name in vehicle_names:

            events = sorted(
                [
                    event
                    for event in vehicle_events
                    if event.name == name
                ],
                key=lambda event:
                event.timestamp
            )

            stop_arrivals = [
                event
                for event in events
                if event.event_type
                   == "pt_stop_arrival"
            ]

            segment_events = [
                event
                for event in events
                if event.event_type
                   == "pt_segment_completed"
            ]

            boarding_events = [
                event
                for event in events
                if event.event_type
                   == "pt_customer_boarded"
            ]

            alighting_events = [
                event
                for event in events
                if event.event_type
                   == "pt_customer_alighted"
            ]

            capacity = 0
            max_onboard = 0
            capacity_violations = 0

            #
            # Validate all capacity snapshots emitted
            # by the Vehicle.
            #

            for event in events:

                details = (
                    event.details
                    or {}
                )

                event_capacity = (
                    details.get(
                        "capacity"
                    )
                )

                onboard = (
                    details.get(
                        "onboard"
                    )
                )

                free_capacity = (
                    details.get(
                        "free_capacity"
                    )
                )

                if event_capacity is not None:

                    try:

                        capacity = max(
                            capacity,
                            int(
                                event_capacity
                            )
                        )

                    except (
                            TypeError,
                            ValueError
                    ):

                        pass

                if onboard is not None:

                    try:

                        max_onboard = max(
                            max_onboard,
                            int(
                                onboard
                            )
                        )

                    except (
                            TypeError,
                            ValueError
                    ):

                        pass

                #
                # pt_segment_completed does not contain
                # capacity information, so it is ignored
                # by this validation.
                #

                if (
                    event_capacity is None
                    or onboard is None
                    or free_capacity is None
                ):
                    continue

                try:

                    event_capacity = int(
                        event_capacity
                    )

                    onboard = int(
                        onboard
                    )

                    free_capacity = int(
                        free_capacity
                    )

                except (
                        TypeError,
                        ValueError
                ):

                    capacity_violations += 1

                    continue

                if (
                    onboard < 0
                    or free_capacity < 0
                    or onboard
                    > event_capacity
                    or free_capacity
                    > event_capacity
                    or onboard
                    + free_capacity
                    != event_capacity
                ):
                    capacity_violations += 1

            #
            # Total travelled distance
            #

            total_distance = 0.0

            for event in segment_events:

                try:

                    distance = (
                        event.details
                        or {}
                    ).get(
                        "distance",
                        0
                    )

                    total_distance += float(
                        distance
                        or 0
                    )

                except (
                        TypeError,
                        ValueError
                ):

                    pass

            #
            # Completed Pattern executions
            #

            completed_pattern_runs = (
                self.count_public_transport_pattern_runs(
                    events
                )
            )

            vehicle_records.append(
                {
                    "name":
                        name,

                    "capacity":
                        int(
                            capacity
                        ),

                    "completed_pattern_runs":
                        int(
                            completed_pattern_runs
                        ),

                    "stops_served":
                        int(
                            len(
                                stop_arrivals
                            )
                        ),

                    "passengers_boarded":
                        int(
                            len(
                                boarding_events
                            )
                        ),

                    "passengers_alighted":
                        int(
                            len(
                                alighting_events
                            )
                        ),

                    "max_onboard":
                        int(
                            max_onboard
                        ),

                    "total_distance":
                        round(
                            float(
                                total_distance
                            ),
                            2
                        ),

                    "capacity_violations":
                        int(
                            capacity_violations
                        ),
                }
            )

        #
        # ==========================================================
        # CUSTOMER METRICS
        # ==========================================================
        #

        customer_records = []

        customer_names = sorted(
            {
                event.name
                for event in customer_events
            }
        )

        customer_leg_violations = 0
        journey_outcome_violations = 0

        for name in customer_names:

            events = sorted(
                [
                    event
                    for event in customer_events
                    if event.name == name
                ],
                key=lambda event:
                event.timestamp
            )

            started_event = first_event(
                events,
                "pt_journey_started"
            )

            completed_events = [
                event
                for event in events
                if event.event_type
                   == "pt_journey_completed"
            ]

            failed_events = [
                event
                for event in events
                if event.event_type
                   == "pt_journey_failed"
            ]

            completed_event = (
                completed_events[0]
                if completed_events
                else None
            )

            failed_event = (
                failed_events[0]
                if failed_events
                else None
            )

            #
            # Every Customer must finish in exactly
            # one terminal outcome.
            #
            # This also detects duplicated execution
            # of terminal FSM states.
            #

            terminal_events = (
                len(
                    completed_events
                )
                + len(
                failed_events
            )
            )

            if terminal_events != 1:
                journey_outcome_violations += 1

            queue_events = [
                event
                for event in events
                if event.event_type
                   == "pt_queue_entered"
            ]

            boarding_events = [
                event
                for event in events
                if event.event_type
                   == "pt_boarded"
            ]

            alighting_events = [
                event
                for event in events
                if event.event_type
                   == "pt_alighted"
            ]

            #
            # Group transactional Customer events
            # by PT leg.
            #

            queue_by_leg = {}
            boarded_by_leg = {}
            alighted_by_leg = {}

            for event in queue_events:
                leg_index = (
                    event.details
                    or {}
                ).get(
                    "leg_index"
                )

                queue_by_leg.setdefault(
                    leg_index,
                    []
                ).append(
                    event
                )

            for event in boarding_events:
                leg_index = (
                    event.details
                    or {}
                ).get(
                    "leg_index"
                )

                boarded_by_leg.setdefault(
                    leg_index,
                    []
                ).append(
                    event
                )

            for event in alighting_events:
                leg_index = (
                    event.details
                    or {}
                ).get(
                    "leg_index"
                )

                alighted_by_leg.setdefault(
                    leg_index,
                    []
                ).append(
                    event
                )

            #
            # For a completed Journey every PT leg
            # must contain exactly:
            #
            # 1 queue_entered
            # 1 boarded
            # 1 alighted
            #

            if completed_event is not None:

                pt_leg_indexes = set(
                    queue_by_leg.keys()
                )

                pt_leg_indexes.update(
                    boarded_by_leg.keys()
                )

                pt_leg_indexes.update(
                    alighted_by_leg.keys()
                )

                for leg_index in (
                    pt_leg_indexes
                ):

                    queue_count = len(
                        queue_by_leg.get(
                            leg_index,
                            []
                        )
                    )

                    boarded_count = len(
                        boarded_by_leg.get(
                            leg_index,
                            []
                        )
                    )

                    alighted_count = len(
                        alighted_by_leg.get(
                            leg_index,
                            []
                        )
                    )

                    if (
                        queue_count != 1
                        or boarded_count != 1
                        or alighted_count != 1
                    ):
                        customer_leg_violations += 1

            #
            # Waiting time
            #
            # Sum queue -> boarding for every PT leg.
            #

            waiting_time = 0.0

            for (
                    leg_index,
                    board_events
            ) in boarded_by_leg.items():

                board_events = sorted(
                    board_events,
                    key=lambda event:
                    event.timestamp
                )

                queued = sorted(
                    queue_by_leg.get(
                        leg_index,
                        []
                    ),
                    key=lambda event:
                    event.timestamp
                )

                for (
                        index,
                        board_event
                ) in enumerate(
                    board_events
                ):

                    if index >= len(
                        queued
                    ):
                        continue

                    waiting_time += (
                        timestamp_delta_seconds(
                            queued[
                                index
                            ].timestamp,
                            board_event.timestamp
                        )
                    )

            #
            # Time inside Vehicles
            #

            in_vehicle_time = 0.0

            for (
                    leg_index,
                    board_events
            ) in boarded_by_leg.items():

                board_events = sorted(
                    board_events,
                    key=lambda event:
                    event.timestamp
                )

                alighted = sorted(
                    alighted_by_leg.get(
                        leg_index,
                        []
                    ),
                    key=lambda event:
                    event.timestamp
                )

                for (
                        index,
                        board_event
                ) in enumerate(
                    board_events
                ):

                    if index >= len(
                        alighted
                    ):
                        continue

                    in_vehicle_time += (
                        timestamp_delta_seconds(
                            board_event.timestamp,
                            alighted[
                                index
                            ].timestamp
                        )
                    )

            #
            # Total Journey time
            #

            journey_end = (
                completed_event
                or failed_event
            )

            total_journey_time = 0.0

            if (
                started_event is not None
                and journey_end is not None
            ):
                total_journey_time = (
                    timestamp_delta_seconds(
                        started_event.timestamp,
                        journey_end.timestamp
                    )
                )

            #
            # Walking distance comes directly from
            # the selected Journey summary.
            #

            walking_distance = 0.0

            source_event = (
                started_event
                or completed_event
            )

            if source_event is not None:

                try:

                    walking_distance = float(
                        (
                            source_event.details
                            or {}
                        ).get(
                            "walking_distance",
                            0
                        )
                        or 0
                    )

                except (
                        TypeError,
                        ValueError
                ):

                    walking_distance = 0.0

            boardings = len(
                boarding_events
            )

            alightings = len(
                alighting_events
            )

            customer_records.append(
                {
                    "name":
                        name,

                    "journey_completed":
                        completed_event
                        is not None,

                    "journey_failed":
                        failed_event
                        is not None,

                    "boardings":
                        int(
                            boardings
                        ),

                    "alightings":
                        int(
                            alightings
                        ),

                    #
                    # Actual executed transfers.
                    #
                    # 1 PT leg  -> 0 transfers
                    # 2 PT legs -> 1 transfer
                    #

                    "transfers":
                        int(
                            max(
                                boardings - 1,
                                0
                            )
                        ),

                    "waiting_time":
                        round(
                            float(
                                waiting_time
                            ),
                            3
                        ),

                    "in_vehicle_time":
                        round(
                            float(
                                in_vehicle_time
                            ),
                            3
                        ),

                    "total_journey_time":
                        round(
                            float(
                                total_journey_time
                            ),
                            3
                        ),

                    "walking_distance":
                        round(
                            float(
                                walking_distance
                            ),
                            2
                        ),
                }
            )

        #
        # ==========================================================
        # STOP METRICS
        # ==========================================================
        #
        # Stops remain completely unmodified.
        #
        # Their statistics are inferred from Vehicle
        # and Customer events.
        #

        stop_ids = set()

        for event in vehicle_events:

            if (
                event.event_type
                != "pt_stop_arrival"
            ):
                continue

            stop_id = (
                event.details
                or {}
            ).get(
                "stop_id"
            )

            if stop_id is not None:
                stop_ids.add(
                    str(
                        stop_id
                    )
                )

        for event in customer_events:

            details = (
                event.details
                or {}
            )

            stop_id = None

            if (
                event.event_type
                == "pt_queue_entered"
            ):

                stop_id = details.get(
                    "stop_id"
                )

            elif (
                event.event_type
                == "pt_boarded"
            ):

                stop_id = details.get(
                    "origin_stop"
                )

            elif (
                event.event_type
                == "pt_alighted"
            ):

                stop_id = details.get(
                    "destination_stop"
                )

            if stop_id is not None:
                stop_ids.add(
                    str(
                        stop_id
                    )
                )

        stop_records = []

        for stop_id in sorted(
            stop_ids
        ):
            vehicle_arrivals = sum(
                1
                for event in vehicle_events
                if (
                    event.event_type
                    == "pt_stop_arrival"
                    and str(
                    (
                        event.details
                        or {}
                    ).get(
                        "stop_id"
                    )
                )
                    == stop_id
                )
            )

            customers_waiting = sum(
                1
                for event in customer_events
                if (
                    event.event_type
                    == "pt_queue_entered"
                    and str(
                    (
                        event.details
                        or {}
                    ).get(
                        "stop_id"
                    )
                )
                    == stop_id
                )
            )

            customers_boarded = sum(
                1
                for event in customer_events
                if (
                    event.event_type
                    == "pt_boarded"
                    and str(
                    (
                        event.details
                        or {}
                    ).get(
                        "origin_stop"
                    )
                )
                    == stop_id
                )
            )

            customers_alighted = sum(
                1
                for event in customer_events
                if (
                    event.event_type
                    == "pt_alighted"
                    and str(
                    (
                        event.details
                        or {}
                    ).get(
                        "destination_stop"
                    )
                )
                    == stop_id
                )
            )

            stop_records.append(
                {
                    "stop_id":
                        stop_id,

                    "vehicle_arrivals":
                        int(
                            vehicle_arrivals
                        ),

                    #
                    # Number of Customer waiting episodes
                    # started at this Stop.
                    #

                    "customers_waiting":
                        int(
                            customers_waiting
                        ),

                    "customers_boarded":
                        int(
                            customers_boarded
                        ),

                    "customers_alighted":
                        int(
                            customers_alighted
                        ),
                }
            )

        #
        # ==========================================================
        # DATAFRAMES
        # ==========================================================
        #
        # print_stats() already prints DataFrame attributes.
        #

        self.publictransport_vehicle_df = (
            pd.DataFrame(
                vehicle_records,
                columns=[
                    "name",
                    "capacity",
                    "completed_pattern_runs",
                    "stops_served",
                    "passengers_boarded",
                    "passengers_alighted",
                    "max_onboard",
                    "total_distance",
                    "capacity_violations",
                ]
            )
        )

        self.publictransport_customer_df = (
            pd.DataFrame(
                customer_records,
                columns=[
                    "name",
                    "journey_completed",
                    "journey_failed",
                    "boardings",
                    "alightings",
                    "transfers",
                    "waiting_time",
                    "in_vehicle_time",
                    "total_journey_time",
                    "walking_distance",
                ]
            )
        )

        self.publictransport_stop_df = (
            pd.DataFrame(
                stop_records,
                columns=[
                    "stop_id",
                    "vehicle_arrivals",
                    "customers_waiting",
                    "customers_boarded",
                    "customers_alighted",
                ]
            )
        )

        #
        # ==========================================================
        # GENERAL METRICS
        # ==========================================================
        #

        total_vehicle_boardings = sum(
            record[
                "passengers_boarded"
            ]
            for record in vehicle_records
        )

        total_vehicle_alightings = sum(
            record[
                "passengers_alighted"
            ]
            for record in vehicle_records
        )

        total_customer_boardings = sum(
            record[
                "boardings"
            ]
            for record in customer_records
        )

        total_customer_alightings = sum(
            record[
                "alightings"
            ]
            for record in customer_records
        )

        completed_journeys = sum(
            1
            for record in customer_records
            if record[
                "journey_completed"
            ]
        )

        failed_journeys = sum(
            1
            for record in customer_records
            if record[
                "journey_failed"
            ]
        )

        capacity_violations = sum(
            record[
                "capacity_violations"
            ]
            for record in vehicle_records
        )

        #
        # Customers still waiting at simulation end.
        #
        # No Stop instrumentation is required:
        #
        # queue_entered - boarded
        #

        customers_left_waiting = 0

        for name in customer_names:
            events = [
                event
                for event in customer_events
                if event.name == name
            ]

            queue_count = sum(
                1
                for event in events
                if event.event_type
                == "pt_queue_entered"
            )

            board_count = sum(
                1
                for event in events
                if event.event_type
                == "pt_boarded"
            )

            customers_left_waiting += max(
                queue_count
                - board_count,
                0
            )

        unfinished_customers = sum(
            1
            for record in customer_records
            if (
                not record[
                    "journey_completed"
                ]
                and not record[
                "journey_failed"
            ]
            )
        )

        if customer_records:

            avg_waiting_time = (
                sum(
                    record[
                        "waiting_time"
                    ]
                    for record
                    in customer_records
                )
                / len(
                customer_records
            )
            )

            avg_journey_time = (
                sum(
                    record[
                        "total_journey_time"
                    ]
                    for record
                    in customer_records
                )
                / len(
                customer_records
            )
            )

        else:

            avg_waiting_time = 0.0
            avg_journey_time = 0.0

        max_vehicle_occupancy = max(
            (
                record[
                    "max_onboard"
                ]
                for record
                in vehicle_records
            ),
            default=0
        )

        general_metrics = {
            "vehicles":
                int(
                    len(
                        vehicle_records
                    )
                ),

            "stops":
                int(
                    len(
                        stop_records
                    )
                ),

            "customers":
                int(
                    len(
                        customer_records
                    )
                ),

            "completed_journeys":
                int(
                    completed_journeys
                ),

            "failed_journeys":
                int(
                    failed_journeys
                ),

            "total_boardings":
                int(
                    total_customer_boardings
                ),

            "total_alightings":
                int(
                    total_customer_alightings
                ),

            "avg_waiting_time":
                round(
                    float(
                        avg_waiting_time
                    ),
                    3
                ),

            "avg_journey_time":
                round(
                    float(
                        avg_journey_time
                    ),
                    3
                ),

            "max_vehicle_occupancy":
                int(
                    max_vehicle_occupancy
                ),

            "customers_left_waiting":
                int(
                    customers_left_waiting
                ),

            "unfinished_customers":
                int(
                    unfinished_customers
                ),

            "capacity_violations":
                int(
                    capacity_violations
                ),
        }

        #
        # ==========================================================
        # VALIDATION METRICS
        # ==========================================================
        #

        validation_checks = {
            "boardings_equal_alightings":
                (
                    total_customer_boardings
                    == total_customer_alightings
                ),

            "vehicle_boardings_equal_customer_boardings":
                (
                    total_vehicle_boardings
                    == total_customer_boardings
                ),

            "vehicle_alightings_equal_customer_alightings":
                (
                    total_vehicle_alightings
                    == total_customer_alightings
                ),

            "capacity_ok":
                capacity_violations == 0,

            "queues_empty":
                customers_left_waiting == 0,

            "all_customers_finished":
                unfinished_customers == 0,

            "customer_legs_ok":
                customer_leg_violations == 0,

            "journey_outcomes_ok":
                journey_outcome_violations == 0,
        }

        validation_metrics = {
            **validation_checks,

            "customer_leg_violations":
                int(
                    customer_leg_violations
                ),

            "journey_outcome_violations":
                int(
                    journey_outcome_violations
                ),

            "all_ok":
                all(
                    validation_checks.values()
                ),
        }

        #
        # Keep summaries available to print_stats()
        # or future visual interfaces.
        #

        self.publictransport_general_metrics = (
            general_metrics
        )

        self.publictransport_validation_metrics = (
            validation_metrics
        )

        #
        # ==========================================================
        # JSON
        # ==========================================================
        #

        json_structure = {
            "GeneralMetrics":
                general_metrics,

            "ValidationMetrics":
                validation_metrics,

            "Vehicles": {
                str(index):
                    record

                for (
                    index,
                    record
                ) in enumerate(
                    vehicle_records
                )
            },

            "Customers": {
                str(index):
                    record

                for (
                    index,
                    record
                ) in enumerate(
                    customer_records
                )
            },

            "Stops": {
                str(index):
                    record

                for (
                    index,
                    record
                ) in enumerate(
                    stop_records
                )
            },
        }

        self.export_to_json(
            json_structure,
            file_path
        )


    def export_to_json(self, json_data: dict, file_path: str) -> None:
        """
        Export the final JSON structure to a JSON file.

        Args:
            json_data (dict): The data to be exported.
            file_path (str): Path where the JSON file will be saved.
        """
        with open(file_path, 'w') as f:
            import json
            json.dump(json_data, f, indent=4)

    def print_stats(self):
        """
        Prints all DataFrames stored as attributes in the class dynamically.
        """
        print("Simulation Results:")

        # Iterate over all attributes of the class that are DataFrames
        for attr_name in dir(self):
            attr_value = getattr(self, attr_name)
            if isinstance(attr_value, pd.DataFrame):
                print(f"{attr_name} stats")
                print(
                    tabulate(
                        attr_value, headers="keys", showindex=False, tablefmt="fancy_grid"
                    )
                )
                print("\n")  # Space between tables
