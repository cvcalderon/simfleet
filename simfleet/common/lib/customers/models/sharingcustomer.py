from loguru import logger

from simfleet.common.lib.customers.models.pedestrian import PedestrianAgent

from simfleet.utils.helpers import distance_in_meters


class SharingCustomerAgent(PedestrianAgent):
    """
    Represents a customer that uses free-floating sharing transports.
    """

    def __init__(self, agentjid, password):
        super().__init__(agentjid, password)
        self._init_sharing_state()

    def _init_sharing_state(self):
        """Initialize state owned by the sharing customer capability."""
        self.transport_candidates = []
        self.pending_transport = None
        self.sharing_transport = None

    def set_transport_candidates(self, candidates):
        if candidates is None:
            self.transport_candidates = []
            return

        self.transport_candidates = [
            dict(candidate)
            for candidate in candidates
        ]

    def get_transport_candidates(self):
        return self.transport_candidates

    def clear_transport_candidates(self):
        self.transport_candidates = []

    def remove_transport_candidate(self, transport_jid):
        self.transport_candidates = [
            candidate
            for candidate in self.transport_candidates
            if str(candidate.get("jid")) != str(transport_jid)
        ]

    def set_sharing_transport(self, transport):
        if transport is None:
            self.sharing_transport = None
            return

        self.sharing_transport = dict(transport)

    def get_sharing_transport(self):
        return self.sharing_transport

    def get_sharing_transport_id(self):
        if self.sharing_transport is None:
            return None

        return self.sharing_transport.get("jid")

    def get_sharing_transport_position(self):
        if self.sharing_transport is None:
            return None

        return self.sharing_transport.get("position")

    def clear_sharing_transport(self):
        self.sharing_transport = None

    def can_walk(self, coords):
        if self.max_walking_dist is None:
            return True

        distance = distance_in_meters(
            self.get_position(),
            coords
        )

        logger.debug(
            "Agent[{}]: Maximum walking distance is {}. "
            "Distance to transport is {}.".format(
                self.name,
                self.max_walking_dist,
                distance
            )
        )

        return distance <= self.max_walking_dist

    def set_pending_transport(self, transport):
        if transport is None:
            self.pending_transport = None
            return

        self.pending_transport = dict(transport)

    def get_pending_transport(self):
        return self.pending_transport

    def get_pending_transport_id(self):
        if self.pending_transport is None:
            return None

        return self.pending_transport.get("jid")

    def clear_pending_transport(self):
        self.pending_transport = None

    def reset_sharing_context(self):
        """Reset transient state from the current sharing service."""
        self.clear_transport_candidates()
        self.clear_pending_transport()
        self.clear_sharing_transport()
