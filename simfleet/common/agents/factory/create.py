from loguru import logger
from abc import ABC, abstractmethod

from simfleet.common.agents.directory import DirectoryAgent
from simfleet.common.lib.vehicles.models.vehicle import VehicleAgent

from simfleet.utils.reflection import load_class

from simfleet.common.lib.customers.models.multimodalcustomer import (
    DestinationStep,
    MultiModalCustomerAgent,
)

from simfleet.common.lib.customers.strategies.multimodal.strategyprofiles import (
    resolve_strategy_profile,
)

class Factory(ABC):
    """
    Abstract base for configuration-driven SimFleet agent factories.

    Concrete factories translate simulator configuration into initialized
    agent instances. Depending on the agent family, this includes dynamic
    class loading, strategy resolution, registration configuration,
    geospatial state, service capabilities, and modality-specific options.

    Factories configure agents but do not start them. Agent lifecycle and
    behaviour execution remain responsibilities of SimulatorAgent and the
    created agents themselves.
    """

    @classmethod
    def configure_registration(cls, agent, registration, domain):
        """
        Apply optional fleet-registration configuration to an agent.

        A configured fleet name without an XMPP domain is expanded using the
        simulator domain before delegating to the agent's generic registration
        contract.

        Args:
            agent: Agent exposing ``configure_registration()``.
            registration (dict | None): Registration configuration containing
                optional ``fleet`` and ``presence`` fields.
            domain (str): XMPP domain used to complete relative fleet names.
        """
        if registration:
            fleet = registration.get("fleet")
            presence = registration.get("presence", False)

            if fleet:
                fleet_jid = str(fleet)

                if "@" not in fleet_jid:
                    fleet_jid = "{}@{}".format(fleet_jid, domain)

                agent.configure_registration(fleet_jid, presence)


    @classmethod
    @abstractmethod
    def create_agent(cls,
                     domain,
                     name,
                     password,
                     class_,
                     default_strategy,
                     simulatorjid,
                     optional,
                     strategy,
                     jid_directory,
                     bbox,
                     fleetmanager,
                     fleet_type,
                     route_host,
                     autonomy,
                     current_autonomy,
                     position,
                     speed,
                     target,
                     services,
                     capacity,
                     line,
                     lines,
                     max_walking_dist
                     ):
        """
        Create and configure one agent from simulator configuration.

        Concrete factories interpret only the parameters relevant to their agent
        family and must return an initialized, but not yet started, agent.

        Returns:
            SimfleetAgent: Configured agent instance.
        """

        raise NotImplementedError


class DirectoryFactory(Factory):
    """
    Factory for the central DirectoryAgent.

    Directory creation does not require dynamic model loading. The factory
    creates the standard DirectoryAgent, assigns its simulator identifier, and
    installs the configured default strategy.
    """
    @classmethod
    def create_agent(cls,
                     domain,
                     name,
                     password,
                     default_strategy,
                     simulatorjid=None,
                     class_=None,
                     optional=None,
                     strategy=None,
                     jid_directory=None,
                     bbox=None,
                     fleetmanager=None,
                     fleet_type=None,
                     route_host=None,
                     autonomy=None,
                     current_autonomy=None,
                     position=None,
                     speed=None,
                     target=None,
                     services=None,
                     capacity=None,
                     line=None,
                     lines=None,
                     max_walking_dist=None
                     ):
        """
        Create and configure the simulator DirectoryAgent.

        Args:
            domain (str): XMPP domain.
            name (str): Agent identifier.
            password (str): XMPP authentication password.
            default_strategy (type): Directory strategy class.

        Returns:
            DirectoryAgent: Configured Directory agent.
        """
        jid = f"{name}@{domain}"
        logger.debug("Creating Directory agent: {}".format(jid))
        agent = DirectoryAgent(jid, password)
        agent.set_id(name)
        agent.strategy = default_strategy
        return agent


class FleetManagerFactory(Factory):
    """
    Factory for generic and specialized FleetManager agents.

    The configured model class is resolved dynamically from its import path.
    Optional constructor arguments are forwarded to specialized managers, such
    as PublicTransportFleetManagerAgent.

    A configured strategy path overrides the default FleetManager strategy.
    """
    @classmethod
    def create_agent(cls,
                     domain,
                     name,
                     password,
                     default_strategy,
                     simulatorjid=None,
                     class_=None,
                     optional=None,
                     strategy=None,
                     jid_directory=None,
                     bbox=None,
                     fleetmanager=None,
                     fleet_type=None,
                     route_host=None,
                     autonomy=None,
                     current_autonomy=None,
                     position=None,
                     speed=None,
                     target=None,
                     services=None,
                     capacity=None,
                     line=None,
                     lines=None,
                     max_walking_dist=None
                    ):
        """
        Create and configure one FleetManager.

        The configured agent class is loaded dynamically with ``load_class()``.
        Optional constructor arguments are forwarded unchanged. After
        instantiation, the factory configures identity, Directory relationship,
        operational strategy, and fleet type.

        Args:
            domain (str): XMPP domain.
            name (str): FleetManager identifier.
            password (str): XMPP authentication password.
            class_ (str): Fully qualified FleetManager model class path.
            default_strategy (type): Strategy used when no custom strategy path is
                supplied.
            optional (dict | None): Extra constructor arguments.
            strategy (str | None): Optional fully qualified strategy class path.
            jid_directory: DirectoryAgent JID.
            fleet_type (str): Fleet identifier managed by this agent.

        Returns:
            FleetManagerAgent: Configured FleetManager instance.

        Raises:
            Exception: If ``class_`` is not supplied as an import path.
        """
        jid = f"{name}@{domain}"
        logger.debug("Creating FleetManager agent: {}".format(jid))


        if type(class_) is str:
            agent_class = load_class(class_)

            if optional:
                agent = agent_class(jid, password, **optional)
            else:
                agent = agent_class(jid, password)

        else:
            raise Exception("The agent needs a class in path format.")

        agent.set_id(name)
        agent.set_directory(jid_directory)

        # Load strategy if provided
        if type(strategy) is str:
            agent.strategy = load_class(strategy)
        else:
            agent.strategy = default_strategy

        logger.debug("Assigning type {} to fleetmanager {}".format(fleet_type, agent.agent_id))
        agent.set_fleet_type(fleet_type)
        return agent


class TransportFactory(Factory):
    """
    Factory for dynamically configured transport agents.

    The factory resolves the concrete transport model and optional strategy,
    configures fleet registration, and applies the common transport runtime
    configuration such as routing, position, capacity, speed, autonomy, and
    advertised services.
    """
    @classmethod
    def create_agent(cls,
                     domain,
                     name,
                     password,
                     class_,
                     default_strategy,
                     simulatorjid=None,
                     optional=None,
                     strategy=None,
                     jid_directory=None,
                     bbox=None,
                     fleetmanager=None,
                     fleet_type=None,
                     route_host=None,
                     autonomy=None,
                     current_autonomy=None,
                     position=None,
                     speed=None,
                     target=None,
                     services=None,
                     capacity=None,
                     max_walking_dist=None,
                     registration=None,
                     route_profile=None,
                    ):
        """
        Create and configure a transport agent.

        The transport model is dynamically loaded from ``class_`` and receives
        optional constructor arguments when configured. The factory then applies
        common transport configuration without starting the agent.

        Configuration may include:

        - Directory and FleetManager registration relationships;
        - strategy selection;
        - fleet type;
        - routing host and route profile;
        - simulation bounding box;
        - autonomy state;
        - provided services;
        - initial position;
        - passenger or service capacity;
        - movement speed.

        Args:
            domain (str): XMPP domain.
            name (str): Transport identifier.
            password (str): XMPP authentication password.
            class_ (str): Fully qualified transport model class path.
            default_strategy (type): Fallback transport strategy.
            optional (dict | None): Extra model-constructor arguments.
            strategy (str | None): Optional strategy class path.
            jid_directory: DirectoryAgent JID.
            fleet_type (str): Fleet identifier.
            route_host (str): Routing-service base address.
            position: Initial transport coordinates.
            services: Optional services exposed by the transport.
            capacity: Optional transport capacity.
            registration (dict | None): Fleet registration configuration.
            route_profile (str | None): Routing profile used by MovableMixin.

        Returns:
            TransportAgent: Configured concrete transport instance.
        """

        jid = f"{name}@{domain}"
        logger.debug("Creating Transport agent: {}".format(jid))

        # Load and instantiate agent class
        if type(class_) is str:
            agent_class = load_class(class_)
            if optional:
                agent = agent_class(jid, password, **optional)
            else:
                agent = agent_class(jid, password)
        else:
            raise Exception ("The agent needs a class in path format.")

        agent.set_id(name)
        agent.set_directory(jid_directory)

        cls.configure_registration(
            agent,
            registration,
            domain
        )

        # Load strategy if provided
        if type(strategy) is str:
            agent.strategy = load_class(strategy)
        else:
            agent.strategy = default_strategy

        # Set fleet type, route host, and additional attributes
        agent.set_fleet_type(fleet_type)
        agent.set_route_host(route_host)
        if route_profile is not None:
            agent.set_route_profile(route_profile)
        agent.set_boundingbox(bbox)

        if autonomy:
            agent.set_autonomy(autonomy, current_autonomy)

        if services:
            agent.set_service_type(services)

        # Additional attributes
        agent.set_initial_position(position)

        if capacity:
            agent.set_capacity(capacity)

        if speed:
            agent.set_speed(speed)

        return agent


class CustomerFactory(Factory):
    """
    Factory for legacy single-modality and multimodal customers.

    Traditional customers receive one operational strategy and one target
    destination.

    MultiModalCustomerAgent requires additional preprocessing. Its itinerary
    configuration is validated before the agent starts, and every modal
    strategy path is resolved exactly once into:

    - a strategy class;
    - one compatible StrategyProfile;
    - a DestinationStep stored in the ordered destination plan.

    This keeps dynamic class loading and strategy-family resolution outside the
    runtime multimodal orchestration FSM.
    """

    @classmethod
    def create_agent(
        cls,
        domain,
        name,
        password,
        class_,
        default_strategy,
        simulatorjid=None,
        optional=None,
        strategy=None,
        jid_directory=None,
        bbox=None,
        fleetmanager=None,
        fleet_type=None,
        route_host=None,
        autonomy=None,
        current_autonomy=None,
        position=None,
        speed=None,
        target=None,
        services=None,
        capacity=None,
        max_walking_dist=None,
        route_profile=None,
        destinations=None,
    ):
        """
        Create and configure a traditional or multimodal customer.

        The configured customer model is resolved first. When the model derives
        from MultiModalCustomerAgent, the factory validates the complete
        destination plan before agent runtime begins.

        For every multimodal destination, the factory:

        1. validates destination, fleet type, and strategy path;
        2. loads the modal strategy class with ``load_class()``;
        3. resolves exactly one StrategyProfile from the strategy inheritance
           hierarchy;
        4. creates a DestinationStep containing the already resolved runtime
           objects.

        The resulting plan is installed in the customer after construction. The
        first DestinationStep becomes the initial customer target.

        Traditional customers instead receive the configured fleet type and
        single target directly.

        Args:
            domain (str): XMPP domain.
            name (str): Customer identifier.
            password (str): XMPP authentication password.
            class_ (str): Fully qualified customer model class path.
            default_strategy (type): Fallback customer strategy.
            optional (dict | None): Extra constructor arguments.
            strategy (str | None): Customer strategy class path. For multimodal
                customers this is the orchestrator strategy.
            jid_directory: DirectoryAgent JID.
            fleet_type (str): Fleet used by a traditional customer.
            route_host (str): Routing-service base address.
            position: Initial customer coordinates.
            target: Destination of a traditional customer.
            speed: Optional pedestrian movement speed.
            max_walking_dist: Optional generic walking-distance constraint.
            route_profile (str | None): Routing profile for pedestrian movement.
            destinations (list | None): Multimodal itinerary configuration.

        Returns:
            CustomerAgent: Configured traditional or multimodal customer.

        Raises:
            ValueError: If multimodal configuration is incomplete or invalid.
            Exception: If ``class_`` is not supplied as an import path.
        """

        jid = f"{name}@{domain}"
        logger.debug("Creating Customer agent: {}".format(jid))

        if type(class_) is str:

            agent_class = load_class(
                class_
            )

            is_multimodal_customer = issubclass(
                agent_class,
                MultiModalCustomerAgent,
            )

            destination_plan = None

            if is_multimodal_customer:

                if not isinstance(
                    strategy,
                    str,
                ):
                    raise ValueError(
                        "MultiModalCustomerAgent requires a multimodal "
                        "orchestrator strategy."
                    )

                if not destinations:
                    raise ValueError(
                        "MultiModalCustomerAgent requires at least one destination."
                    )

                destination_plan = []

                for index, step in enumerate(
                    destinations
                ):
                    if not isinstance(
                        step,
                        dict,
                    ):
                        raise ValueError(
                            "Multimodal destination {} must be a dictionary.".format(
                                index + 1
                            )
                        )

                    destination = step.get(
                        "destination"
                    )

                    step_fleet_type = step.get(
                        "fleet_type"
                    )

                    strategy_path = step.get(
                        "strategy"
                    )

                    if destination is None:
                        raise ValueError(
                            "Multimodal destination {} has no destination.".format(
                                index + 1
                            )
                        )

                    if not step_fleet_type:
                        raise ValueError(
                            "Multimodal destination {} has no fleet_type.".format(
                                index + 1
                            )
                        )

                    if not strategy_path:
                        raise ValueError(
                            "Multimodal destination {} has no strategy.".format(
                                index + 1
                            )
                        )

                    strategy_class = load_class(
                        strategy_path
                    )

                    strategy_profile = resolve_strategy_profile(
                        strategy_class
                    )

                    destination_plan.append(
                        DestinationStep(
                            id=step.get(
                                "id",
                                "destination_{}".format(
                                    index + 1
                                ),
                            ),
                            destination=destination,
                            fleet_type=step_fleet_type,
                            strategy_path=strategy_path,
                            strategy_class=strategy_class,
                            strategy_profile=strategy_profile,
                            dwell_time=step.get(
                                "dwell_time",
                                0,
                            ),
                        )
                    )

            if optional:

                agent = agent_class(
                    jid,
                    password,
                    **optional
                )

            else:

                agent = agent_class(
                    jid,
                    password
                )

        else:

            raise Exception(
                "The agent needs a class in path format."
            )

        agent.set_id(name)
        agent.set_directory(jid_directory)

        # Load strategy if provided
        if type(strategy) is str:
            agent.strategy = load_class(strategy)
        else:
            agent.strategy = default_strategy

        # Set fleet type for traditional customers.
        # MultiModalCustomerAgent activates fleet_type per destination.
        if not is_multimodal_customer:
            agent.set_fleet_type(
                fleet_type
            )

        agent.set_route_host(route_host)
        if route_profile is not None:
            agent.set_route_profile(route_profile)
        agent.set_boundingbox(bbox)

        agent.set_initial_position(
            position
        )

        if is_multimodal_customer:

            agent.set_destination_plan(
                destination_plan
            )

            first_step = (
                agent.get_current_destination_step()
            )

            agent.set_target_position(
                first_step.destination
            )

        else:

            agent.set_target_position(
                target
            )

        # NEW set maximum walking distance if defined
        if max_walking_dist:
            agent.set_max_walking_dist(max_walking_dist)

        if speed:
            agent.set_speed(speed)

        return agent


class StationFactory(Factory):
    """
    Factory for service-station agents.

    Station configuration combines common geospatial state with one or more
    service definitions.

    Two service execution models are supported:

    ``mode="behaviour"``
        Requests consume service slots and execute a configured
        OneShotBehaviour.

    ``mode="agent"``
        The station owns a finite inventory of transport-agent JIDs, as used
        by station-based sharing.
    """
    @classmethod
    def create_agent(cls,
                    domain,
                    name,
                    password,
                    default_strategy,
                    simulatorjid=None,
                    class_=None,
                    optional=None,
                    strategy=None,
                    jid_directory=None,
                    bbox=None,
                    fleetmanager=None,
                    fleet_type=None,
                    route_host=None,
                    autonomy=None,
                    current_autonomy=None,
                    position=None,
                    speed=None,
                    target=None,
                    services=None,
                    capacity=None,
                    line=None,
                    lines=None,
                    max_walking_dist=None,
                    registration=None
                    ):

        """
        Create and configure a service station.

        The concrete station class is loaded dynamically, positioned in the
        simulation, and optionally configured for FleetManager registration.

        Service definitions are then translated into either slot-based Behaviour
        services or agent-backed inventories.

        Args:
            domain (str): XMPP domain.
            name (str): Station identifier.
            password (str): XMPP authentication password.
            class_ (str): Fully qualified station model class path.
            default_strategy (type): Fallback service Behaviour class.
            jid_directory: DirectoryAgent JID.
            route_host (str): Routing-service base address.
            bbox: Simulation bounding box.
            position: Station coordinates.
            services (iterable[dict]): Station service definitions.
            registration (dict | None): Fleet registration configuration.

        Returns:
            ServiceStationAgent: Configured station instance.
        """

        jid = f"{name}@{domain}"
        logger.debug("Creating Station agent: {}".format(jid))

        # Load and instantiate agent class
        if type(class_) is str:
            agent_class = load_class(class_)
            agent = agent_class(jid, password)
        else:
            raise Exception ("The agent needs a class in path format.")

        agent.set_id(name)
        agent.set_directory(jid_directory)

        cls.configure_registration(
            agent,
            registration,
            domain
        )

        # Set route host, and additional attributes
        agent.set_route_host(route_host)
        agent.set_boundingbox(bbox)
        agent.set_position(position)

        if simulatorjid:
            agent.set_simulatorjid(simulatorjid)

        # Station service management
        for service in services:
            mode = service["mode"]

            if mode == "behaviour":

                type_ = service["type"]
                behaviour = service["behaviour"]
                slots = service["slots"]
                args = service["args"]

                if type(behaviour) is str:
                    one_shot_behaviour = load_class(behaviour)
                else:
                    one_shot_behaviour = default_strategy
                agent.add_service(type_, mode, slots, one_shot_behaviour, **args)

            elif mode == "agent":

                type_ = service["type"]
                slots = service["slots"]
                args = service.get("args")
                args = args or {}

                agent.set_fleet_type(type_)
                agent.add_service_agent(type_, mode, slots, **args)

        return agent

class TransportStopFactory(Factory):
    """
    Factory for transport-stop agents.

    Specialized stop models may receive constructor-specific configuration,
    such as the directional Pattern list of PublicTransportStopAgent.

    The factory also configures identity, registration, operational strategy,
    geospatial state, Simulator access, fleet type, and any compatibility
    queue identifiers still supplied by the simulator configuration.
    """

    @classmethod
    def create_agent(
        cls,
        domain,
        name,
        password,
        default_strategy=None,
        simulatorjid=None,
        class_=None,
        optional=None,
        strategy=None,
        jid_directory=None,
        bbox=None,
        fleetmanager=None,
        fleet_type=None,
        route_host=None,
        autonomy=None,
        current_autonomy=None,
        position=None,
        speed=None,
        target=None,
        services=None,
        capacity=None,
        line=None,
        lines=None,
        max_walking_dist=None,
        registration=None,
    ):
        """
        Create and configure one transport stop.

        The stop model is dynamically loaded and receives optional constructor
        arguments before generic station configuration is applied.

        PublicTransportStopAgent uses its constructor options to initialize
        Pattern-specific queues. Any additional queue identifiers supplied through
        the generic factory interface are retained for configuration
        compatibility.

        Args:
            domain (str): XMPP domain.
            name (tuple): Stop identifier and display name.
            password (str): XMPP authentication password.
            class_ (str): Fully qualified stop model class path.
            default_strategy (type | None): Fallback stop strategy.
            optional (dict | None): Stop-model constructor arguments.
            strategy (str | None): Optional strategy class path.
            jid_directory: DirectoryAgent JID.
            position: Stop coordinates.
            fleet_type (str | None): Fleet associated with the stop.
            registration (dict | None): Fleet registration configuration.

        Returns:
            QueueStationAgent: Configured specialized stop instance.
        """

        jid = f"{name[0]}@{domain}"
        logger.debug("Creating Station agent: {}".format(jid))

        if type(class_) is str:
            agent_class = load_class(class_)

            if optional:
                agent = agent_class(
                    jid,
                    password,
                    **optional
                )
            else:
                agent = agent_class(
                    jid,
                    password
                )

        else:
            raise Exception(
                "The agent needs a class in path format."
            )

        agent.set_id(name[0])
        agent.set_name(name[1])
        agent.set_directory(jid_directory)

        cls.configure_registration(
            agent,
            registration,
            domain
        )

        # Load strategy if provided
        if type(strategy) is str:
            agent.strategy = load_class(strategy)
        else:
            agent.strategy = default_strategy

        # Set route host, and additional attributes
        agent.set_route_host(route_host)
        agent.set_boundingbox(bbox)
        agent.set_position(position)

        if simulatorjid:
            agent.set_simulatorjid(simulatorjid)

        for line in lines:
            agent.add_queue(line)

        if fleet_type is not None:
            agent.set_fleet_type(fleet_type)

        return agent

class VehicleFactory(Factory):
    """
    Factory for the generic VehicleAgent model.

    Unlike TransportFactory, this factory currently instantiates VehicleAgent
    directly rather than dynamically resolving ``class_``. It then applies
    registration, strategy, routing, target, position, and movement
    configuration.
    """
    @classmethod
    def create_agent(cls,
                    domain,
                    name,
                    password,
                    default_strategy,
                    simulatorjid=None,
                    class_=None,
                    optional=None,
                    strategy=None,
                    jid_directory=None,
                    bbox=None,
                    fleetmanager=None,
                    fleet_type=None,
                    route_host=None,
                    autonomy=None,
                    current_autonomy=None,
                    position=None,
                    speed=None,
                    target=None,
                    services = None,
                    capacity=None,
                    line=None,
                    lines=None,
                    max_walking_dist=None,
                    registration=None,
                    route_profile=None,
                    ):
        """
        Create and configure a generic VehicleAgent.

        Args:
            domain (str): XMPP domain.
            name (str): Vehicle identifier.
            password (str): XMPP authentication password.
            default_strategy (type): Fallback vehicle strategy.
            strategy (str | None): Optional strategy class path.
            jid_directory: DirectoryAgent JID.
            fleet_type (str): Vehicle fleet type.
            route_host (str): Routing-service base address.
            bbox: Simulation bounding box.
            position: Initial vehicle coordinates.
            target: Initial target coordinates.
            speed: Optional movement speed.
            registration (dict | None): Fleet registration configuration.
            route_profile (str | None): Routing profile.

        Returns:
            VehicleAgent: Configured generic vehicle.
        """
        jid = f"{name}@{domain}"
        logger.debug("Creating Vehicle agent: {}".format(jid))
        agent = VehicleAgent(jid, password)
        agent.set_id(name)
        agent.set_directory(jid_directory)

        cls.configure_registration(
            agent,
            registration,
            domain
        )

        # Load strategy if provided
        if type(strategy) is str:
            agent.strategy = load_class(strategy)
        else:
            agent.strategy = default_strategy

        # Set route host, and additional attributes
        agent.set_fleet_type(fleet_type)
        agent.set_route_host(route_host)
        if route_profile is not None:
            agent.set_route_profile(route_profile)
        agent.set_boundingbox(bbox)
        agent.set_target_position(target)

        agent.set_initial_position(position)

        if speed:
            agent.set_speed(speed)

        return agent
