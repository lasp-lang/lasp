%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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

    Plumtree = {lasp_plumtree_broadcast_distribution_backend,
                {lasp_plumtree_broadcast_distribution_backend, start_link, []},
                 permanent, 5000, worker,
                 [lasp_plumtree_broadcast_distribution_backend]},

    Transmission = {lasp_transmission_instrumentation,
                    {lasp_transmission_instrumentation, start_link, []},
                     permanent, 5000, worker,
                     [lasp_transmission_instrumentation]},

    Web = {webmachine_mochiweb,
           {webmachine_mochiweb, start, [lasp_config:web_config()]},
            permanent, 5000, worker,
            [mochiweb_socket_server]},

    PeerRefresh = {lasp_peer_refresh_service,
                   {lasp_peer_refresh_service, start_link, []},
                    permanent, 5000, worker,
                    [lasp_peer_refresh_service]},

    InstrDefault = list_to_atom(os:getenv("INSTRUMENTATION", "false")),
    Instrumentation = application:get_env(?APP, instrumentation, InstrDefault),

    Children = case Instrumentation of
        true ->
            lager:info("Instrumentation: ~p", [Instrumentation]),
            [Web, Process, Unique, Plumtree, PeerRefresh, Transmission];
        false ->
            [Web, Process, Unique, Plumtree, PeerRefresh]
    end,

    {ok, {{one_for_one, 5, 10}, Children}}.
