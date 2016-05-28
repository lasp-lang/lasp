%% -------------------------------------------------------------------
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
            %% No-op.
            ok;
        PeerPort ->
            partisan_config:set(peer_port, list_to_integer(PeerPort)),
            ok
    end,

    Partisan = {partisan_sup,
                {partisan_sup, start_link, []},
                 permanent, infinity, supervisor, [partisan_sup]},

    PlumtreeBackend = {lasp_plumtree_broadcast_distribution_backend,
                       {lasp_plumtree_broadcast_distribution_backend, start_link, []},
                        permanent, 5000, worker,
                        [lasp_plumtree_broadcast_distribution_backend]},

    Plumtree = {plumtree_sup,
                {plumtree_sup, start_link, []},
                 permanent, infinity, supervisor, [plumtree_sup]},

    %% Before initializing the web backend, configure it using the
    %% proper ports.
    case os:getenv("WEB_PORT", "false") of
        "false" ->
            %% Generate a random web port.
            random:seed(erlang:phash2([node()]),
                        erlang:monotonic_time(),
                        erlang:unique_integer()),
            WebPort = random:uniform(1000) + 10000,
            lasp_config:set(web_port, WebPort),
            ok;
        WebPort ->
            lasp_config:set(web_port, list_to_integer(WebPort)),
            ok
    end,

    Web = {webmachine_mochiweb,
           {webmachine_mochiweb, start, [lasp_config:web_config()]},
            permanent, 5000, worker,
            [mochiweb_socket_server]},

    MarathonPeerRefresh = {lasp_marathon_peer_refresh_service,
                           {lasp_marathon_peer_refresh_service, start_link, []},
                            permanent, 5000, worker,
                            [lasp_marathon_peer_refresh_service]},

    BaseSpecs = [Unique,
                 Partisan,
                 PlumtreeBackend,
                 Plumtree,
                 MarathonPeerRefresh,
                 Process,
                 Web],

    InstrDefault = list_to_atom(os:getenv("INSTRUMENTATION", "false")),
    InstrEnabled = application:get_env(?APP, instrumentation, InstrDefault),
    lasp_config:set(instrumentation, InstrEnabled),

    Children = case InstrEnabled of
        true ->
            ok = application:set_env(?APP, instrumentation, InstrEnabled),

            ClientTrans = {lasp_client_transmission_instrumentation,
                           {lasp_transmission_instrumentation, start_link, [client]},
                            permanent, 5000, worker,
                            [lasp_transmission_instrumentation]},

            ServerTrans = {lasp_server_transmission_instrumentation,
                           {lasp_transmission_instrumentation, start_link, [server]},
                            permanent, 5000, worker,
                            [lasp_transmission_instrumentation]},

            Divergence = {lasp_divergence_instrumentation,
                          {lasp_divergence_instrumentation, start_link, []},
                           permanent, 5000, worker,
                           [lasp_divergence_instrumentation]},

            BaseSpecs ++ [ClientTrans,
                          ServerTrans,
                          Divergence];
        false ->
            ok = application:set_env(?APP, instrumentation, InstrEnabled),
            BaseSpecs
    end,

    SimDefault = list_to_atom(os:getenv("AD_COUNTER_SIM", "false")),
    SimEnabled = application:get_env(?APP,
                                     ad_counter_simulation_on_boot,
                                     SimDefault),

    %% Run local simulations if instrumentation is enabled.
    case SimEnabled of
        true ->
            spawn(fun() ->
                        timer:sleep(10000),
                        lasp_simulate_resource:run()
                  end),
            ok;
        false ->
            ok
    end,

    ProfileDefault = list_to_atom(os:getenv("PROFILE", "false")),
    ProfileEnabled = application:get_env(?APP,
                                         profile,
                                         ProfileDefault),
    lasp_config:set(profile, ProfileEnabled),

    %% Cache values with lasp_config to help out on the performance.
    Mode = application:get_env(?APP, mode, state_based),
    lasp_config:set(mode, Mode),

    StorageBackend = application:get_env(
                       ?APP,
                       storage_backend,
                       lasp_ets_storage_backend),
    lasp_config:set(storage_backend, StorageBackend),

    DistributionBackend = application:get_env(
                            ?APP,
                            distribution_backend,
                            lasp_plumtree_broadcast_distribution_backend),
    lasp_config:set(distribution_backend, DistributionBackend),

    MaxDeltaSlots = application:get_env(?APP, delta_mode_max_slots, 10),
    lasp_config:set(delta_mode_max_slots, MaxDeltaSlots),

    MaxGCCounter = application:get_env(?APP, delta_mode_max_gc_counter, ?MAX_GC_COUNTER),
    lasp_config:set(delta_mode_max_gc_counter, MaxGCCounter),

    IncrementalComputation = application:get_env(
                               ?APP,
                               incremental_computation_mode,
                               false),
    lasp_config:set(incremental_computation_mode, IncrementalComputation),

    {ok, {{one_for_one, 5, 10}, Children}}.
