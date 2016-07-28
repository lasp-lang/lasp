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
    %% to use the proper ports.
    %%
    case os:getenv("PEER_PORT", "false") of
        "false" ->
            partisan_config:set(peer_port, random_port()),
            ok;
        PeerPort ->
            partisan_config:set(peer_port, list_to_integer(PeerPort)),
            ok
    end,

    %% Configure the peer service.
    partisan_config:set(partisan_peer_service_manager,
                        partisan_client_server_peer_service_manager),

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

    MarathonPeerRefresh = {lasp_marathon_peer_refresh_service,
                           {lasp_marathon_peer_refresh_service, start_link, []},
                            permanent, 5000, worker,
                            [lasp_marathon_peer_refresh_service]},

    WebSpecs = web_specs(),

    BaseSpecs0 = [Unique,
                  Partisan,
                  DistributionBackend,
                  Plumtree,
                  MarathonPeerRefresh,
                  Process] ++ WebSpecs,

    DagEnabled = application:get_env(?APP, dag_enabled, false),
    lasp_config:set(dag_enabled, DagEnabled),
    BaseSpecs = case DagEnabled of
        true -> [DepDag | BaseSpecs0];
        false -> BaseSpecs0
    end,

    InstrDefault = list_to_atom(os:getenv("INSTRUMENTATION", "false")),
    InstrEnabled = application:get_env(?APP, instrumentation, InstrDefault),
    lasp_config:set(instrumentation, InstrEnabled),

    Children0 = case InstrEnabled of
        true ->
            lager:info("Instrumentation is enabled!"),
            Transmission = {lasp_transmission_instrumentation,
                            {lasp_transmission_instrumentation, start_link, []},
                             permanent, 5000, worker,
                             [lasp_transmission_instrumentation]},

            BaseSpecs ++ [Transmission];
        false ->
            BaseSpecs
    end,

    ClientNumberDefault = list_to_integer(os:getenv("CLIENT_NUMBER", "3")),
    ClientNumber = application:get_env(?APP, client_number, ClientNumberDefault),
    lasp_config:set(client_number, ClientNumber),

    %% Setup the advertisement counter example, if necessary.
    AdSpecs = advertisement_counter_child_specs(),

    %% Setup the music festival example, if necessary.
    MusicSpecs = music_festival_child_specs(),

    Children = Children0 ++ AdSpecs ++ MusicSpecs,

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
            lasp_config:set(web_port, random_port()),
            ok;
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

    EvaluationIdDefault = list_to_atom(os:getenv("EVAL_ID", "undefined")),
    EvaluationIdEnabled = application:get_env(?APP,
                                              evaluation_identifier,
                                              EvaluationIdDefault),
    lasp_config:set(evaluation_identifier, EvaluationIdEnabled),

    EvaluationTimestampDefault = list_to_integer(os:getenv("EVAL_TIMESTAMP", "0")),
    EvaluationTimestampEnabled = application:get_env(?APP,
                                                  evaluation_timestamp,
                                                  EvaluationTimestampDefault),
    lasp_config:set(evaluation_timstamp, EvaluationTimestampEnabled),

    %% Peer service.
    PeerService = application:get_env(plumtree,
                                      peer_service,
                                      partisan_peer_service),
    PeerServiceManager = PeerService:manager(),
    lasp_config:set(peer_service_manager, PeerServiceManager),

    %% Exchange mode.
    case Mode of
        delta_based ->
            application:set_env(plumtree, exchange_selection,
                                optimized);
        _ ->
            application:set_env(plumtree, exchange_selection,
                                normal)
    end,

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
    lasp_config:set(incremental_computation_mode, IncrementalComputation).

%% @private
music_festival_child_specs() ->
    %% Figure out who is acting as the client.
    MusicClientDefault = list_to_atom(os:getenv("MUSIC_FESTIVAL_SIM_CLIENT", "false")),
    MusicClientEnabled = application:get_env(?APP,
                                             music_festival_simulation_client,
                                             MusicClientDefault),
    lasp_config:set(music_festival_simulation_client, MusicClientEnabled),
    lager:info("MusicClientEnabled: ~p", [MusicClientEnabled]),

    ClientSpecs = case MusicClientEnabled of
        true ->
            %% Start one advertisement counter client process per node.
            MusicFestivalClient = {lasp_music_festival_client,
                                   {lasp_music_festival_client, start_link, []},
                                    permanent, 5000, worker,
                                    [lasp_music_festival_client]},

            %% Configure proper partisan tag.
            partisan_config:set(tag, client),

            %% Configure reserved slots.
            partisan_config:set(reservations, [server]),

            [MusicFestivalClient];
        false ->
            []
    end,

    %% Figure out who is acting as the server.
    MusicServerDefault = list_to_atom(os:getenv("MUSIC_FESTIVAL_SIM_SERVER", "false")),
    MusicServerEnabled = application:get_env(?APP,
                                             music_festival_simulation_server,
                                             MusicServerDefault),
    lasp_config:set(music_festival_simulation_server, MusicServerEnabled),
    lager:info("MusicServerEnabled: ~p", [MusicServerEnabled]),

    ServerSpecs = case MusicServerEnabled of
        true ->
            MusicFestivalServer = {lasp_music_festival_server,
                                   {lasp_music_festival_server, start_link, []},
                                    permanent, 5000, worker,
                                    [lasp_music_festival_server]},

            %% Configure proper partisan tag.
            partisan_config:set(tag, server),

            [MusicFestivalServer];
        false ->
            []
    end,

    ClientSpecs ++ ServerSpecs.

%% @private
advertisement_counter_child_specs() ->
    %% Figure out who is acting as the client.
    AdClientDefault = list_to_atom(os:getenv("AD_COUNTER_SIM_CLIENT", "false")),
    AdClientEnabled = application:get_env(?APP,
                                          ad_counter_simulation_client,
                                          AdClientDefault),
    lasp_config:set(ad_counter_simulation_client, AdClientEnabled),
    lager:info("AdClientEnabled: ~p", [AdClientEnabled]),

    ClientSpecs = case AdClientEnabled of
        true ->
            %% Start one advertisement counter client process per node.
            AdCounterClient = {lasp_advertisement_counter_client,
                               {lasp_advertisement_counter_client, start_link, []},
                                permanent, 5000, worker,
                                [lasp_advertisement_counter_client]},

            %% Configure proper partisan tag.
            partisan_config:set(tag, client),

            %% Configure reserved slots.
            partisan_config:set(reservations, [server]),

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
