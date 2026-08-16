import pandas as pd
from tabulate import tabulate
from simfleet.metrics.basestatistics import BaseStatisticsClass
from simfleet.utils.statistics import Log


class MobilityStatisticsClass(BaseStatisticsClass):

    def run(self, events_log: Log) -> None:
        """
        Run the statistics generation process based on the agent type.

        Args:
            events_log (Log): A log containing all events from the simulation.
        """
        self.transport_metrics(events_log, "simfleet_metrics_transport.json")
        self.taxi_metrics(events_log, "simfleet_metrics_taxi.json")
        self.electric_taxi_metrics(events_log, "simfleet_metrics_electrictaxi.json")
        self.customer_taxi_metrics(events_log, "simfleet_metrics_taxicustomer.json")

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
