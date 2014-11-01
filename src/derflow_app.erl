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

-module(derflow_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case derflow_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register(derflow,
                                    [{vnode_module, derflow_vnode}]),
            ok = riak_core_node_watcher:service_up(derflow, self()),

            ok = riak_core_ring_events:add_guarded_handler(
                    derflow_ring_event_handler, []),

            ok = riak_core_node_watcher_events:add_guarded_handler(
                    derflow_node_event_handler, []),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
