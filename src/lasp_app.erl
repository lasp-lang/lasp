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

-module(lasp_app).

-behaviour(application).

-include("lasp.hrl").

%% Application callbacks
-export([start/2, stop/1]).

-define(PROGRAMS, [lasp_example_keylist_program,
                   lasp_example_program,
                   lasp_riak_keylist_program]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

%% @doc Start the lasp application.
start(_StartType, _StartArgs) ->
    case lasp_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register(lasp, [{vnode_module, lasp_vnode}]),

            ok = riak_core_node_watcher:service_up(lasp, self()),

            ok = riak_core_ring_events:add_guarded_handler(lasp_ring_event_handler, []),

            ok = riak_core_node_watcher_events:add_guarded_handler(lasp_node_event_handler, []),

            %% Register Lasp applications.
            [ok = lasp:register(Program,
                                code:lib_dir(?APP, src) ++ "/" ++ atom_to_list(Program) ++ ".erl",
                                global) || Program <- ?PROGRAMS],

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Stop the lasp application.
stop(_State) ->
    ok.
