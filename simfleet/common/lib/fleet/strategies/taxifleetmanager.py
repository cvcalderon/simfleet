import json

from loguru import logger

from simfleet.common.agents.fleetmanager import FleetManagerStrategyBehaviour
from simfleet.communications.protocol import REQUEST_PERFORMATIVE, INFORM_PERFORMATIVE, REQUEST_PROTOCOL
from simfleet.utils.helpers import distance_in_meters

from spade.message import Message

class TaxiFleetManagerStrategy(FleetManagerStrategyBehaviour):

    async def on_start(self):
        await super().on_start()

        self.initial_taxi_positions = {}
        self.return_points = []

        self.return_point_tolerance = 100

    def update_initial_taxi_positions(self):
        vehicles = self.agent.get_available_vehicles()

        for item in vehicles:
            vehicle = item["vehicle"]
            data = item["data"]
            vehicle_jid = vehicle.get("jid")

            if not vehicle_jid:
                continue

            vehicle_jid = self.agent.bare_jid(vehicle_jid)

            if vehicle_jid in self.initial_taxi_positions:
                continue

            if data is None:
                continue

            position = data.get("p")

            if position is None:
                continue

            self.initial_taxi_positions[vehicle_jid] = list(position)

            self.register_taxi_return_point(
                vehicle_jid,
                position
            )

            logger.debug(
                "Agent[{}]: Initial position stored for taxi [{}]: {}".format(
                    self.agent.name,
                    vehicle_jid,
                    position
                )
            )

    def get_taxi_candidates(self, origin):
        candidates = []

        vehicles = self.agent.get_available_vehicles()

        for item in vehicles:
            vehicle = item["vehicle"]
            data = item["data"]

            position = data.get("p")
            assignments = data.get("a")

            if position is None:
                continue

            if assignments is None:
                continue

            distance = distance_in_meters(
                position,
                origin
            )

            candidates.append(
                {
                    "name": vehicle.get("name"),
                    "jid": str(vehicle["jid"]),
                    "position": position,
                    "assignments": assignments,
                    "distance": distance,
                }
            )

        return candidates

    def select_taxi(self, origin):
        candidates = self.get_taxi_candidates(origin)

        if not candidates:
            return None

        candidates.sort(
            key=lambda candidate: (
                candidate["distance"],
                candidate["assignments"]
            )
        )

        return candidates[0]

    def find_nearest_return_point(self, position):
        nearest_point = None
        nearest_distance = None

        for return_point in self.return_points:
            distance = distance_in_meters(
                position,
                return_point["position"]
            )

            if nearest_distance is None or distance < nearest_distance:
                nearest_point = return_point
                nearest_distance = distance

        return nearest_point, nearest_distance

    def register_taxi_return_point(self, taxi_jid, position):
        nearest_point, nearest_distance = self.find_nearest_return_point(
            position
        )

        if (
            nearest_point is not None
            and nearest_distance <= self.return_point_tolerance
        ):
            if taxi_jid not in nearest_point["taxis"]:
                nearest_point["taxis"].append(taxi_jid)

            logger.debug(
                "Agent[{}]: Taxi [{}] assigned to existing return point "
                "{} at {:.2f} meters.".format(
                    self.agent.name,
                    taxi_jid,
                    nearest_point["position"],
                    nearest_distance
                )
            )

            return nearest_point

        return_point = {
            "position": list(position),
            "taxis": [taxi_jid],
        }

        self.return_points.append(return_point)

        logger.debug(
            "Agent[{}]: New return point created at {} for taxi [{}].".format(
                self.agent.name,
                position,
                taxi_jid
            )
        )

        return return_point

    async def handle_return_request(self, msg, content):
        taxi_jid = str(msg.sender)

        if not self.agent.is_registered_vehicle(taxi_jid):
            logger.warning(
                "Agent[{}]: Return request received from unregistered taxi [{}].".format(
                    self.agent.name,
                    taxi_jid
                )
            )
            return

        position = content.get("position")

        if position is None:
            logger.warning(
                "Agent[{}]: Taxi [{}] return request has no position.".format(
                    self.agent.name,
                    taxi_jid
                )
            )
            return

        return_point, distance = self.find_nearest_return_point(
            position
        )

        if return_point is None:
            logger.warning(
                "Agent[{}]: No return point available for taxi [{}].".format(
                    self.agent.name,
                    taxi_jid
                )
            )
            return

        logger.info(
            "Agent[{}]: Return point {} selected for taxi [{}] "
            "at {:.2f} meters.".format(
                self.agent.name,
                return_point["position"],
                taxi_jid,
                distance
            )
        )

        await self.send_return_position(
            taxi_jid,
            return_point["position"]
        )

    async def send_return_position(self, taxi_jid, position):
        msg = Message()

        msg.to = taxi_jid
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
                "request_type": "taxi_return",
                "return_position": position,
            }
        )

        logger.debug(
            "Agent[{}]: Sending return position {} to taxi [{}].".format(
                self.agent.name,
                position,
                taxi_jid
            )
        )

        await self.send(msg)

    async def run(self):
        if not self.agent.registration:
            await self.send_registration()

        self.update_initial_taxi_positions()

        msg = await self.receive(timeout=5)

        if not msg:
            return

        logger.debug(
            "Agent[{}]: Taxi FleetManager received message: {}".format(
                self.agent.name,
                msg
            )
        )

        performative = msg.get_metadata("performative")

        if performative != REQUEST_PERFORMATIVE:
            return

        try:
            content = json.loads(msg.body)

        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Agent[{}]: Invalid taxi request.".format(
                    self.agent.name
                )
            )
            return

        request_type = content.get("request_type")

        if request_type == "taxi_return":
            await self.handle_return_request(
                msg,
                content
            )
            return

        origin = content.get("origin")

        if origin is None:
            logger.warning(
                "Agent[{}]: Taxi request has no origin.".format(
                    self.agent.name
                )
            )
            return

        taxi = self.select_taxi(origin)

        if taxi is None:
            logger.warning(
                "Agent[{}]: No available taxis found.".format(
                    self.agent.name
                )
            )
            return

        logger.info(
            "Agent[{}]: Taxi [{}] selected at {:.2f} meters "
            "with {} completed assignments.".format(
                self.agent.name,
                taxi["jid"],
                taxi["distance"],
                taxi["assignments"]
            )
        )

        msg.to = taxi["jid"]

        await self.send(msg)
