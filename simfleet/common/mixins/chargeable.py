from loguru import logger

from simfleet.utils.helpers import (
    distance_in_meters,
)

MIN_AUTONOMY = 2

class ChargeableMixin:
    """
    Add autonomy and replenishment state to a transport agent.

    The mixin models autonomy as a remaining travel distance in kilometres.
    ``max_autonomy_km`` stores the fully replenished range while
    ``current_autonomy_km`` stores the range currently available.

    Autonomy checks reserve ``MIN_AUTONOMY`` kilometres instead of allowing a
    planned movement to consume the complete remaining range.

    The current ElectricTaxi strategy primarily uses the distance-based API:

    - ``calculate_distance_km()`` for one direct movement;
    - ``calculate_service_km()`` for travel to a customer plus the service
      journey;
    - ``has_enough_autonomy_km()`` for generic expense validation;
    - ``has_enough_autonomy_for_service()`` for a complete customer service;
    - ``has_enough_autonomy_to()`` for movement to one destination.

    ``calculate_km_expense()`` and ``has_enough_autonomy()`` are retained as
    compatibility APIs and are not the primary path used by the current
    ElectricTaxi strategy.

    The host agent is expected to provide ``name``, ``get_position()``, and
    the lifecycle in which autonomy is consumed or replenished.
    """

    def __init__(self):
        """
        Initialize autonomy and energy-service state.

        New instances start with 2000 km of maximum and current autonomy and no
        configured service type.
        """

        self.current_autonomy_km = 2000
        self.max_autonomy_km = 2000
        self.service_type = None


    def set_service_type(self, service_type):
        """
        Configure the station service requested when replenishment is required.

        Args:
            service_type (str): Service identifier advertised by compatible
                service stations.
        """
        self.service_type = service_type

    def set_autonomy(self, autonomy, current_autonomy=None):
        """
        Configure maximum and current vehicle autonomy in kilometres.

        When ``current_autonomy`` is omitted, the vehicle starts fully
        replenished.

        Args:
            autonomy: Maximum autonomy in kilometres.
            current_autonomy: Optional initial remaining autonomy in kilometres.
        """
        self.max_autonomy_km = autonomy
        self.current_autonomy_km = (
            current_autonomy if current_autonomy is not None else autonomy
        )

    def get_autonomy(self):
        """
        Return the currently available autonomy.

        Returns:
            float: Remaining travel range in kilometres.
        """
        return self.current_autonomy_km


    def decrease_autonomy_km(self, expense=0):
        """
        Consume autonomy after a completed or committed movement.

        Remaining autonomy is clamped to zero when the requested expense would
        exhaust or exceed the available range.

        Args:
            expense: Distance in kilometres to subtract.
        """
        if self.current_autonomy_km - expense > 0:
            self.current_autonomy_km -= expense
        else:
            self.current_autonomy_km = 0

        logger.debug(
            "Agent [{}]: The max autonomy is ({}), and the current autonomy is ({}).".format(self.name,
                                                                                                 self.max_autonomy_km,
                                                                                                 self.current_autonomy_km)
        )

    def increase_autonomy_km(self, expense=0):
        """
        Restore part of the vehicle autonomy.

        Current autonomy is capped at ``max_autonomy_km``.

        Args:
            expense: Distance-equivalent autonomy in kilometres to restore.
        """
        if expense + self.current_autonomy_km < self.max_autonomy_km:
            self.current_autonomy_km += expense
        else:
            self.current_autonomy_km = self.max_autonomy_km

        logger.debug(
            "Agent [{}]: The max autonomy is ({}), and the current autonomy is ({}).".format(self.name,
                                                                                             self.max_autonomy_km,
                                                                                             self.current_autonomy_km)
        )

    def increase_full_autonomy_km(self):
        """
        Restore the vehicle to its configured maximum autonomy.
        """
        self.current_autonomy_km = self.max_autonomy_km
        logger.debug(
            "Agent [{}]: The max autonomy is ({}), and the current autonomy is ({}).".format(self.name,
                                                                                                 self.max_autonomy_km,
                                                                                                 self.current_autonomy_km)
        )

    def calculate_km_expense(self, current_pos, origin, dest=None):
        """
        Calculate the legacy distance expense for reaching an origin and
        optionally continuing to a destination.

        The result uses straight-line geographic distances and truncates the
        accumulated metres to whole kilometres with floor division.

        This method is retained for compatibility. The current ElectricTaxi
        strategy uses ``calculate_distance_km()`` and ``calculate_service_km()``
        instead.

        Args:
            current_pos: Current transport coordinates.
            origin: First destination coordinates.
            dest: Optional second destination coordinates.

        Returns:
            float: Legacy distance expense expressed in whole kilometres.

        Note:
            The current implementation evaluates the origin-to-destination
            distance before checking whether ``dest`` is None. The optional
            destination contract therefore requires dedicated review before it
            should be relied upon.
        """
        fir_distance = distance_in_meters(current_pos, origin)
        sec_distance = distance_in_meters(origin, dest)
        if dest is None:
            sec_distance = 0
        return (fir_distance + sec_distance) // 1000

    def has_enough_autonomy(self, orig, dest):
        """
        Check autonomy for the legacy two-stage service-distance calculation.

        The vehicle must begin above ``MIN_AUTONOMY`` and the projected remaining
        autonomy after the calculated expense must not fall below that reserve.

        Args:
            orig: Service origin coordinates.
            dest: Service destination coordinates.

        Returns:
            bool: True when the legacy service expense satisfies the autonomy
            reserve.

        Note:
            The current ElectricTaxi strategy uses
            ``has_enough_autonomy_for_service()`` and
            ``has_enough_autonomy_km()`` instead.
        """
        autonomy = self.get_autonomy()
        if autonomy <= MIN_AUTONOMY:
            logger.warning(
                "Agent[{}]: Has not enough autonomy ({}).".format(self.name, autonomy)
            )
            return False
        travel_km = self.calculate_km_expense(
            self.get_position(), orig, dest
        )
        logger.debug(
            "Agent[{}]: Has autonomy ({}) when max autonomy is ({})"
            " and needs ({}) for the trip".format(
                self.name,
                self.get_autonomy(),
                self.max_autonomy_km,
                travel_km,
            )
        )

        if autonomy - travel_km < MIN_AUTONOMY:
            logger.warning(
                "Agent[{}]: Has not enough autonomy to do travel ({} for {} km).".format(
                    self.name, autonomy, travel_km
                )
            )
            return False
        return True

    def calculate_distance_km(self, origin, destination):
        """
        Calculate straight-line geographic distance between two positions.

        Distance is obtained with ``distance_in_meters()`` and converted to
        kilometres without truncation.

        Args:
            origin: Origin coordinates.
            destination: Destination coordinates.

        Returns:
            float: Geographic distance in kilometres.
        """
        distance = distance_in_meters(
            origin,
            destination
        )

        return distance / 1000

    def calculate_service_km(self, origin, destination):
        """
        Estimate autonomy required for a complete customer service.

        The expense is the sum of:

        1. distance from the vehicle's current position to the service origin;
        2. distance from the service origin to the final destination.

        Both segments use straight-line geographic distance.

        Args:
            origin: Customer or service origin coordinates.
            destination: Service destination coordinates.

        Returns:
            float: Total estimated service distance in kilometres.
        """
        distance_to_origin = self.calculate_distance_km(
            self.get_position(),
            origin
        )

        service_distance = self.calculate_distance_km(
            origin,
            destination
        )

        return distance_to_origin + service_distance

    def has_enough_autonomy_km(self, expense):
        """
        Check whether a distance expense respects the autonomy reserve.

        The vehicle must currently be above ``MIN_AUTONOMY`` and the projected
        autonomy after the expense must not fall below that threshold.

        This method performs validation only; it does not consume autonomy.

        Args:
            expense: Planned distance expense in kilometres.

        Returns:
            bool: True when the planned expense satisfies the autonomy reserve.
        """
        autonomy = self.get_autonomy()

        if autonomy <= MIN_AUTONOMY:
            logger.warning(
                "Agent[{}]: Has not enough autonomy ({}).".format(
                    self.name,
                    autonomy
                )
            )
            return False

        logger.debug(
            "Agent[{}]: Has autonomy ({}) when max autonomy is ({}) "
            "and needs ({}) for the trip.".format(
                self.name,
                autonomy,
                self.max_autonomy_km,
                expense
            )
        )

        if autonomy - expense < MIN_AUTONOMY:
            logger.warning(
                "Agent[{}]: Has not enough autonomy to do travel "
                "({} for {} km).".format(
                    self.name,
                    autonomy,
                    expense
                )
            )
            return False

        return True

    def has_enough_autonomy_for_service(
        self,
        origin,
        destination
    ):
        """
        Check autonomy for a complete customer-service movement.

        The required expense combines travel from the vehicle's current position
        to ``origin`` and from ``origin`` to ``destination``.

        Args:
            origin: Service origin coordinates.
            destination: Service destination coordinates.

        Returns:
            bool: True when the complete service satisfies the autonomy reserve.
        """
        travel_km = self.calculate_service_km(
            origin,
            destination
        )

        return self.has_enough_autonomy_km(
            travel_km
        )

    def has_enough_autonomy_to(self, destination):
        """
        Check autonomy for direct movement from the current position.

        Args:
            destination: Target coordinates.

        Returns:
            bool: True when direct movement to the destination satisfies the
            autonomy reserve.
        """
        travel_km = self.calculate_distance_km(
            self.get_position(),
            destination
        )

        return self.has_enough_autonomy_km(
            travel_km
        )
