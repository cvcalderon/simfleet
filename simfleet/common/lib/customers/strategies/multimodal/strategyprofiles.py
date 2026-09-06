from dataclasses import dataclass

from spade.template import Template

from simfleet.communications.protocol import (
    REQUEST_PROTOCOL,
    QUERY_PROTOCOL,
)

from simfleet.common.lib.customers.models.taxicustomer import (
    TaxiCustomerStrategyBehaviour,
)

from simfleet.common.lib.customers.strategies.sharingcustomer import (
    FSMSharingCustomerStrategyBehaviour,
)

from simfleet.common.lib.customers.strategies.stationsharingcustomer import (
    FSMStationSharingCustomerStrategyBehaviour,
)

from simfleet.common.lib.customers.strategies.publictransportcustomer import (
    FSMPublicTransportCustomerStrategyBehaviour,
)


STRATEGY_FAMILY_TAXI = "taxi"
STRATEGY_FAMILY_SHARING = "sharing"
STRATEGY_FAMILY_STATION_SHARING = "station_sharing"
STRATEGY_FAMILY_PUBLIC_TRANSPORT = "public_transport"


@dataclass(frozen=True)
class StrategyProfile:
    """
    Describes the runtime requirements of a customer strategy family.

    A profile associates a strategy base class with the SPADE protocols
    required to receive messages and the agent method used to reset the
    transient state of that modality.
    """

    family: str
    strategy_base: type
    protocols: tuple[str, ...]
    reset_method: str


    def build_template(self):
        """Build a fresh SPADE Template for one strategy execution."""
        if not self.protocols:
            raise RuntimeError(
                "Strategy profile '{}' has no protocols configured.".format(
                    self.family
                )
            )

        template = None

        for protocol in self.protocols:
            protocol_template = Template()
            protocol_template.set_metadata(
                "protocol",
                protocol,
            )

            if template is None:
                template = protocol_template
            else:
                template = template | protocol_template

        return template

    def prepare_completion(self, agent):
        """Prepare the completion signal for the modal strategy."""
        agent.prepare_modal_completion()

    async def wait_for_completion(self, agent):
        """Wait until the modal strategy reports its completion."""
        await agent.wait_modal_completion()


CUSTOMER_STRATEGY_PROFILES = (
    StrategyProfile(
        family=STRATEGY_FAMILY_TAXI,
        strategy_base=TaxiCustomerStrategyBehaviour,
        protocols=(
            REQUEST_PROTOCOL,
        ),
        reset_method="reset_taxi_context",
    ),

    StrategyProfile(
        family=STRATEGY_FAMILY_SHARING,
        strategy_base=FSMSharingCustomerStrategyBehaviour,
        protocols=(
            REQUEST_PROTOCOL,
            QUERY_PROTOCOL,
        ),
        reset_method="reset_sharing_context",
    ),

    StrategyProfile(
        family=STRATEGY_FAMILY_STATION_SHARING,
        strategy_base=FSMStationSharingCustomerStrategyBehaviour,
        protocols=(
            REQUEST_PROTOCOL,
            QUERY_PROTOCOL,
        ),
        reset_method="reset_station_sharing_context",
    ),

    StrategyProfile(
        family=STRATEGY_FAMILY_PUBLIC_TRANSPORT,
        strategy_base=FSMPublicTransportCustomerStrategyBehaviour,
        protocols=(
            REQUEST_PROTOCOL,
        ),
        reset_method="reset_public_transport_context",
    ),
)


def _strategy_class_name(strategy_class):
    """Return a readable fully-qualified strategy class name."""
    module = getattr(
        strategy_class,
        "__module__",
        None,
    )

    name = getattr(
        strategy_class,
        "__qualname__",
        repr(strategy_class),
    )

    if module:
        return "{}.{}".format(
            module,
            name,
        )

    return name


def resolve_strategy_profile(strategy_class):
    """
    Resolve exactly one StrategyProfile for a customer strategy class.

    Resolution uses inheritance rather than exact class equality so custom
    strategies derived from a supported strategy family remain compatible.
    """

    if not isinstance(
        strategy_class,
        type,
    ):
        raise TypeError(
            "Customer strategy must be a class, got {}.".format(
                type(strategy_class).__name__
            )
        )

    matches = [
        profile
        for profile in CUSTOMER_STRATEGY_PROFILES
        if issubclass(
            strategy_class,
            profile.strategy_base,
        )
    ]

    strategy_name = _strategy_class_name(
        strategy_class
    )

    if not matches:
        raise ValueError(
            "Unsupported customer strategy class: '{}'.".format(
                strategy_name
            )
        )

    if len(matches) > 1:
        families = ", ".join(
            profile.family
            for profile in matches
        )

        raise ValueError(
            "Ambiguous customer strategy class '{}'. "
            "It matches multiple strategy families: {}.".format(
                strategy_name,
                families,
            )
        )

    return matches[0]
