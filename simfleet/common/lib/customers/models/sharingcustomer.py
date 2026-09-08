from loguru import logger

from simfleet.common.lib.customers.models.pedestrian import PedestrianAgent

from simfleet.utils.helpers import distance_in_meters


class SharingCustomerAgent(PedestrianAgent):
    """
    Customer model for free-floating sharing services.

    The model stores the transient selection and assignment context required
    while a customer searches for, books, and reaches a free-floating
    transport.

    Three transport states are kept deliberately separate:

    ``transport_candidates``
        Vehicles currently available for local evaluation.

    ``pending_transport``
        Selected vehicle whose booking request is awaiting confirmation.

    ``sharing_transport``
        Vehicle whose booking has been accepted and is assigned to the
        current service.

    Candidate discovery, filtering, booking negotiation, walking, and service
    progression are implemented by the configured Sharing customer strategy.
    """

    def __init__(self, agentjid, password):
        """
        Initialize pedestrian infrastructure and Sharing-specific state.

        Args:
            agentjid (str): XMPP JID used by the customer.
            password (str): XMPP authentication password.
        """
        super().__init__(agentjid, password)
        self._init_sharing_state()

    def _init_sharing_state(self):
        """
        Initialize state owned exclusively by the Sharing customer capability.

        The helper is intentionally independent from ``__init__`` so
        MultiModalCustomerAgent can initialize Sharing state without executing
        the complete SharingCustomerAgent constructor through multiple
        inheritance.
        """
        self.transport_candidates = []
        self.pending_transport = None
        self.sharing_transport = None

    def set_transport_candidates(self, candidates):
        """
        Replace the local set of candidate sharing transports.

        Candidate mappings are copied before storage so the customer owns its
        local selection context.

        Args:
            candidates (iterable[dict] | None): Candidate transport definitions.
                ``None`` clears the candidate set.
        """
        if candidates is None:
            self.transport_candidates = []
            return

        self.transport_candidates = [
            dict(candidate)
            for candidate in candidates
        ]

    def get_transport_candidates(self):
        """
        Return the current Sharing transport candidates.

        Returns:
            list[dict]: Locally stored candidate transports.
        """
        return self.transport_candidates

    def clear_transport_candidates(self):
        """Clear all locally stored Sharing transport candidates."""
        self.transport_candidates = []

    def remove_transport_candidate(self, transport_jid):
        """
        Remove one candidate transport by JID.

        Args:
            transport_jid: JID of the transport to remove.
        """
        self.transport_candidates = [
            candidate
            for candidate in self.transport_candidates
            if str(candidate.get("jid")) != str(transport_jid)
        ]

    def set_sharing_transport(self, transport):
        """
        Store the Sharing transport assigned to the active service.

        This value represents a validated booking, not merely a candidate whose
        approval is still pending.

        Args:
            transport (dict | None): Assigned transport definition. ``None``
                clears the assignment.
        """
        if transport is None:
            self.sharing_transport = None
            return

        self.sharing_transport = dict(transport)

    def get_sharing_transport(self):
        """
        Return the transport assigned to the active Sharing service.

        Returns:
            dict | None: Assigned transport definition.
        """
        return self.sharing_transport

    def get_sharing_transport_id(self):
        """
        Return the JID of the assigned Sharing transport.

        Returns:
            str | None: Assigned transport JID.
        """
        if self.sharing_transport is None:
            return None

        return self.sharing_transport.get("jid")

    def get_sharing_transport_position(self):
        """
        Return the last stored position of the assigned Sharing transport.

        Returns:
            Any: Stored transport position, or None when no transport is assigned.
        """
        if self.sharing_transport is None:
            return None

        return self.sharing_transport.get("position")

    def clear_sharing_transport(self):
        """Clear the Sharing transport assigned to the current service."""
        self.sharing_transport = None

    def can_walk(self, coords):
        """
        Return whether the customer may walk to the supplied coordinates.

        When no maximum walking distance is configured, every destination is
        considered walkable. Otherwise, the geographic distance from the current
        customer position is compared with ``max_walking_dist``.

        Args:
            coords: Candidate destination coordinates.

        Returns:
            bool: True when the destination satisfies the walking constraint.
        """
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
        """
        Store the selected transport whose booking response is pending.

        ``pending_transport`` is an intermediate negotiation state. It must not be
        interpreted as an assigned Sharing transport until the booking has been
        accepted by the transport.

        Args:
            transport (dict | None): Selected candidate transport. ``None`` clears
                the pending selection.
        """
        if transport is None:
            self.pending_transport = None
            return

        self.pending_transport = dict(transport)

    def get_pending_transport(self):
        """
        Return the transport currently awaiting booking confirmation.

        Returns:
            dict | None: Pending transport definition.
        """
        return self.pending_transport

    def get_pending_transport_id(self):
        """
        Return the JID of the transport awaiting booking confirmation.

        Returns:
            str | None: Pending transport JID.
        """
        if self.pending_transport is None:
            return None

        return self.pending_transport.get("jid")

    def clear_pending_transport(self):
        """Clear the transport selection awaiting booking confirmation."""
        self.pending_transport = None

    def reset_sharing_context(self):
        """
        Reset transient state owned by a completed Sharing service.

        Candidate transports, the pending booking, and the assigned Sharing
        transport are cleared.

        Generic customer state such as physical position, destination,
        FleetManagers, and accumulated metrics is intentionally preserved.
        """
        self.clear_transport_candidates()
        self.clear_pending_transport()
        self.clear_sharing_transport()
