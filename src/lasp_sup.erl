%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(lasp_sup).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(supervisor).

-include("lasp.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    DepDag = {lasp_dependence_dag,
              {lasp_dependence_dag, start_link, []},
               permanent, 5000, worker, [lasp_dependence_dag]},

    Process = {lasp_process_sup,
               {lasp_process_sup, start_link, []},
                permanent, infinity, supervisor, [lasp_process_sup]},

    Unique = {lasp_unique,
              {lasp_unique, start_link, []},
               permanent, 5000, worker,
               [lasp_unique]},

    %% Before initializing the partisan backend, be sure to configure it
    %% to use the proper ip and ports.
    case os:getenv("IP", "false") of
        "false" ->
            ok;
        IP ->
            {ok, IPAddress} = inet_parse:address(IP),
            partisan_config:set(peer_ip, IPAddress),
            ok
    end,
    case os:getenv("PEER_PORT", "false") of
        "false" ->
            partisan_config:set(peer_port, random_port()),
            ok;
        PeerPort ->
            partisan_config:set(peer_port, list_to_integer(PeerPort)),
            ok
    end,

    %% Configure the peer service.
    PeerServiceDefault = list_to_atom(os:getenv("PEER_SERVICE", "partisan_client_server_peer_service_manager")),
    PeerService = application:get_env(partisan,
                                      partisan_peer_service_manager,
                                      PeerServiceDefault),
    partisan_config:set(partisan_peer_service_manager, PeerService),

    Partisan = {partisan_sup,
                {partisan_sup, start_link, []},
                 permanent, infinity, supervisor, [partisan_sup]},

    DistributionBackend = {lasp_default_broadcast_distribution_backend,
                           {lasp_default_broadcast_distribution_backend, start_link, []},
                            permanent, 5000, worker,
                            [lasp_default_broadcast_distribution_backend]},

    Plumtree = {plumtree_sup,
                {plumtree_sup, start_link, []},
                 permanent, infinity, supervisor, [plumtree_sup]},

    Sprinter = {sprinter,
                {sprinter, start_link, []},
                 permanent, 5000, worker,
                 [sprinter]},

    BroadcastBuffer = {lasp_broadcast_buffer,
                       {lasp_broadcast_buffer, start_link, []},
                        permanent, 5000, worker,
                        [lasp_broadcast_buffer]},

    WebSpecs = web_specs(),

    BaseSpecs0 = [Unique,
                  BroadcastBuffer,
                  Partisan,
                  DistributionBackend,
                  Plumtree,
                  Sprinter,
                  Process] ++ WebSpecs,

    DagEnabled = application:get_env(?APP, dag_enabled, false),
    lasp_config:set(dag_enabled, DagEnabled),
    BaseSpecs = case DagEnabled of
        true -> [DepDag | BaseSpecs0];
        false -> BaseSpecs0
    end,

    %% Configure Plumtree logging.
    TransLogMFA = {lasp_instrumentation, transmission, [broadcast]},
    partisan_config:set(transmission_logging_mfa, TransLogMFA),

    InstrDefault = list_to_atom(os:getenv("INSTRUMENTATION", "false")),
    InstrEnabled = application:get_env(?APP, instrumentation, InstrDefault),
    lasp_config:set(instrumentation, InstrEnabled),

    Children0 = case InstrEnabled of
        true ->
            lager:info("Instrumentation is enabled!"),
            Instrumentation = {lasp_instrumentation,
                               {lasp_instrumentation, start_link, []},
                               permanent, 5000, worker,
                               [lasp_instrumentation]},

            BaseSpecs ++ [Instrumentation];
        false ->
            BaseSpecs
    end,

    %% Setup the advertisement counter example, if necessary.
    AdSpecs = advertisement_counter_child_specs(),

    %% Setup the game tournament example, if necessary.
    TournamentSpecs = game_tournament_child_specs(),

    Children = Children0 ++ AdSpecs ++ TournamentSpecs,

    %% Configure defaults.
    configure_defaults(),

    {ok, {{one_for_one, 5, 10}, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
web_specs() ->
    %% Before initializing the web backend, configure it using the
    %% proper ports.
    %%
    case os:getenv("WEB_PORT", "false") of
        "false" ->
            case lasp_config:get(web_port, undefined) of
                undefined ->
                    lasp_config:set(web_port, random_port());
                _ ->
                    ok
            end;
        WebPort ->
            lasp_config:set(web_port, list_to_integer(WebPort)),
            ok
    end,

    Web = {webmachine_mochiweb,
           {webmachine_mochiweb, start, [lasp_config:web_config()]},
            permanent, 5000, worker,
            [mochiweb_socket_server]},

    [Web].

%% @private
configure_defaults() ->
    TutorialDefault = list_to_atom(os:getenv("TUTORIAL", "false")),
    Tutorial = application:get_env(?APP, tutorial, TutorialDefault),
    lager:info("Setting tutorial: ~p", [Tutorial]),
    lasp_config:set(tutorial, Tutorial),

    ExtendedLoggingDefault = list_to_atom(os:getenv("EXTENDED_LOGGING", "false")),
    ExtendedLogging = application:get_env(?APP, extended_logging, ExtendedLoggingDefault),
    lager:info("Setting extended logging: ~p", [ExtendedLogging]),
    lasp_config:set(extended_logging, ExtendedLogging),

    MailboxLoggingDefault = list_to_atom(os:getenv("MAILBOX_LOGGING", "false")),
    MailboxLogging = application:get_env(?APP, mailbox_logging, MailboxLoggingDefault),
    lager:info("Setting mailbox logging: ~p", [MailboxLogging]),
    lasp_config:set(mailbox_logging, MailboxLogging),

    ModeDefault = list_to_atom(os:getenv("MODE", "state_based")),
    Mode = application:get_env(?APP, mode, ModeDefault),
    lager:info("Setting operation mode: ~p", [Mode]),
    lasp_config:set(mode, Mode),

    SetDefault = list_to_atom(os:getenv("SET", "orset")),
    Set = application:get_env(?APP, set, SetDefault),
    lager:info("Setting set type: ~p", [Set]),
    lasp_config:set(set, Set),

    ProfileDefault = list_to_atom(os:getenv("PROFILE", "false")),
    ProfileEnabled = application:get_env(?APP,
                                         profile,
                                         ProfileDefault),
    lasp_config:set(profile, ProfileEnabled),

    BroadcastDefault = list_to_atom(os:getenv("BROADCAST", "false")),
    BroadcastEnabled = application:get_env(?APP,
                                           broadcast,
                                           BroadcastDefault),
    lasp_config:set(broadcast, BroadcastEnabled),

    SimulationDefault = list_to_atom(os:getenv("SIMULATION", "undefined")),
    Simulation = application:get_env(?APP,
                                       simulation,
                                       SimulationDefault),
    lasp_config:set(simulation, Simulation),

    EvaluationIdDefault = list_to_atom(os:getenv("EVAL_ID", "undefined")),
    EvaluationId = application:get_env(?APP,
                                       evaluation_identifier,
                                       EvaluationIdDefault),
    lasp_config:set(evaluation_identifier, EvaluationId),

    EvaluationTimestampDefault = list_to_integer(os:getenv("EVAL_TIMESTAMP", "0")),
    EvaluationTimestamp = application:get_env(?APP,
                                              evaluation_timestamp,
                                              EvaluationTimestampDefault),
    lasp_config:set(evaluation_timestamp, EvaluationTimestamp),

    ClientNumberDefault = list_to_integer(os:getenv("CLIENT_NUMBER", "3")),
    ClientNumber = application:get_env(?APP,
                                       client_number,
                                       ClientNumberDefault),
    lasp_config:set(client_number, ClientNumber),

    HeavyClientsDefault = list_to_atom(os:getenv("HEAVY_CLIENTS", "false")),
    HeavyClients = application:get_env(?APP,
                                       heavy_clients,
                                       HeavyClientsDefault),
    lasp_config:set(heavy_clients, HeavyClients),

    ReactiveServerDefault = list_to_atom(os:getenv("REACTIVE_SERVER", "false")),
    ReactiveServer = application:get_env(?APP,
                                       reactive_server,
                                       ReactiveServerDefault),
    lasp_config:set(reactive_server, ReactiveServer),

    PartitionProbabilityDefault = list_to_integer(os:getenv("PARTITION_PROBABILITY", "0")),
    PartitionProbability = application:get_env(?APP,
                                               partition_probability,
                                               PartitionProbabilityDefault),
    lasp_config:set(partition_probability, PartitionProbability),

    %% Exchange mode.
    case Mode of
        delta_based ->
            application:set_env(plumtree, exchange_selection,
                                optimized);
        _ ->
            application:set_env(plumtree, exchange_selection,
                                normal)
    end,

    %% AAE interval.
    AAEIntervalDefault = list_to_integer(os:getenv("AAE_INTERVAL", "10000")),
    AAEInterval = application:get_env(?APP,
                                      aae_interval,
                                      AAEIntervalDefault),
    lasp_config:set(aae_interval, AAEInterval),
    application:set_env(plumtree, broadcast_exchange_timer, AAEInterval),

    %% Delta interval.
    DeltaIntervalDefault = list_to_integer(os:getenv("DELTA_INTERVAL", "10000")),
    DeltaInterval = application:get_env(?APP,
                                        delta_interval,
                                        DeltaIntervalDefault),
    lasp_config:set(delta_interval, DeltaInterval),

    %% Backend configurations.
    StorageBackend = application:get_env(
                       ?APP,
                       storage_backend,
                       lasp_ets_storage_backend),
    lasp_config:set(storage_backend, StorageBackend),

    DistributionBackend = application:get_env(
                            ?APP,
                            distribution_backend,
                            lasp_default_broadcast_distribution_backend),
    lasp_config:set(distribution_backend, DistributionBackend),

    %% Delta specific configuration values.
    MaxGCCounter = application:get_env(?APP, delta_mode_max_gc_counter, ?MAX_GC_COUNTER),
    lasp_config:set(delta_mode_max_gc_counter, MaxGCCounter),

    %% Incremental computation.
    IncrementalComputation = application:get_env(
                               ?APP,
                               incremental_computation_mode,
                               false),

    %% Automatic contraction configuration.
    %% Only makes sense if the dag is enabled.
    lasp_config:set(incremental_computation_mode, IncrementalComputation),
    case lasp_config:get(dag_enabled, false) of
        true ->
            AutomaticContraction = application:get_env(?APP, automatic_contraction, false),
            lasp_config:set(automatic_contraction, AutomaticContraction);
        _ -> ok
    end.

%% @private
advertisement_counter_child_specs() ->
    %% Figure out who is acting as the client.
    AdClientDefault = list_to_atom(os:getenv("AD_COUNTER_SIM_CLIENT", "false")),
    AdClientEnabled = application:get_env(?APP,
                                          ad_counter_simulation_client,
                                          AdClientDefault),
    lasp_config:set(ad_counter_simulation_client, AdClientEnabled),
    lager:info("AdClientEnabled: ~p", [AdClientEnabled]),

    ImpressionNumberDefault = 4800,
    ImpressionNumber = application:get_env(?APP,
                                           max_impressions,
                                           ImpressionNumberDefault),
    lasp_config:set(max_impressions, ImpressionNumber),

    ImpressionVelocityDefault = list_to_integer(os:getenv("IMPRESSION_VELOCITY", "1")),
    ImpressionVelocity = application:get_env(?APP,
                                             impression_velocity,
                                             ImpressionVelocityDefault),
    lasp_config:set(impression_velocity, ImpressionVelocity),

    ClientSpecs = case AdClientEnabled of
        true ->
            %% Start one advertisement counter client process per node.
            AdCounterClient = {lasp_advertisement_counter_client,
                               {lasp_advertisement_counter_client, start_link, []},
                                permanent, 5000, worker,
                                [lasp_advertisement_counter_client]},

            %% Configure proper partisan tag.
            partisan_config:set(tag, client),

            [AdCounterClient];
        false ->
            []
    end,

    %% Figure out who is acting as the server.
    AdServerDefault = list_to_atom(os:getenv("AD_COUNTER_SIM_SERVER", "false")),
    AdServerEnabled = application:get_env(?APP,
                                          ad_counter_simulation_server,
                                          AdServerDefault),
    lasp_config:set(ad_counter_simulation_server, AdServerEnabled),
    lager:info("AdServerEnabled: ~p", [AdServerEnabled]),

    ServerSpecs = case AdServerEnabled of
        true ->
            AdCounterServer = {lasp_advertisement_counter_server,
                               {lasp_advertisement_counter_server, start_link, []},
                                permanent, 5000, worker,
                                [lasp_advertisement_counter_server]},

            %% Configure proper partisan tag.
            partisan_config:set(tag, server),

            [AdCounterServer];
        false ->
            []
    end,

    ClientSpecs ++ ServerSpecs.

%% @private
random_port() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, {_, Port}} = inet:sockname(Socket),
    ok = gen_tcp:close(Socket),
    Port.

%% @private
game_tournament_child_specs() ->
    %% Figure out who is acting as the client.
    TournClientDefault = list_to_atom(os:getenv("TOURNAMENT_SIM_CLIENT", "false")),
    TournClientEnabled = application:get_env(?APP,
                                          tournament_simulation_client,
                                          TournClientDefault),
    lasp_config:set(tournament_simulation_client, TournClientEnabled),
    lager:info("TournClientEnabled: ~p", [TournClientEnabled]),

    ClientSpecs = case TournClientEnabled of
        true ->
            TournCounterClient = {lasp_game_tournament_client,
                                  {lasp_game_tournament_client, start_link, []},
                                   permanent, 5000, worker,
                                   [lasp_game_tournament_client]},

            %% Configure proper partisan tag.
            partisan_config:set(tag, client),

            [TournCounterClient];
        false ->
            []
    end,

    %% Figure out who is acting as the server.
    TournServerDefault = list_to_atom(os:getenv("TOURNAMENT_SIM_SERVER", "false")),
    TournServerEnabled = application:get_env(?APP,
                                             tournament_simulation_server,
                                             TournServerDefault),
    lasp_config:set(tournament_simulation_server, TournServerEnabled),
    lager:info("TournServerEnabled: ~p", [TournServerEnabled]),

    ServerSpecs = case TournServerEnabled of
        true ->
            TournServer = {lasp_game_tournament_server,
                           {lasp_game_tournament_server, start_link, []},
                            permanent, 5000, worker,
                            [lasp_game_tournament_server]},

            %% Configure proper partisan tag.
            partisan_config:set(tag, server),

            [TournServer];
        false ->
            []
    end,

    ClientSpecs ++ ServerSpecs.

