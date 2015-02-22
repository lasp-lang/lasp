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

%% @doc Test that a bind works on a distributed cluster of nodes.

-module(lasp_kvs_replica_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0,
         start/0]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = rt:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = lasp_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = lasp_test_helpers:wait_for_cluster(Nodes),

    lager:info("Remotely executing the test."),
    ?assertEqual(ok, rpc:call(Node, ?MODULE, test, [])),

    lager:info("Done!"),

    pass.

-endif.

-define(MAP, riak_dt_map).
-define(KEY, {'X', riak_dt_orset}).
-define(VALUE, <<"Chris">>).

test() ->
    %% Start the KVS replica.
    Replica = spawn_link(?MODULE, start, []),

    %% Write a value.
    Replica ! {put, ?KEY, ?VALUE, self()},

    %% Wait for response.
    receive
        ok ->
            ok
    end,

    %% Read a value.
    Replica ! {get, ?KEY, self()},

    %% Wait for response.
    receive
        {ok, _Value} ->
            ok
    end,

    %% Read a value.
    Replica ! {remove, ?KEY, self()},

    %% Wait for response.
    receive
        ok ->
            ok
    end,

    ok.

%% @doc Start one KVS replica.
start() ->
    lager:info("Replica started!"),

    %% Create a CRDT map.
    {ok, Map} = lasp:declare(?MAP),

    %% Wait for messages, and process them.
    receiver(Map, 1).

%% @doc Receive loop for processing incoming messages.
receiver(Map, ReplicaId) ->
    %% Wait for messages from clients.
    receive
        {get, Key, Client} ->
            {ok, {_, _, MapValue0, _}} = lasp:read(Map),
            MapValue = ?MAP:value(MapValue0),
            case orddict:find(Key, MapValue) of
                error ->
                    Client ! {ok, not_found};
                Value ->
                    Client ! {ok, Value}
            end,
            receiver(Map, ReplicaId);
        {put, Key, Value, Client} ->
            {ok, _} = lasp:update(Map,
                                  {update,
                                   [{update, Key, {add, Value}}]},
                                  ReplicaId),
            Client ! ok,
            receiver(Map, ReplicaId);
        {remove, Key, Client} ->
            {ok, _} = lasp:update(Map,
                                  {update,
                                   [{remove, Key}]},
                                  ReplicaId),
            Client ! ok,
            receiver(Map, ReplicaId)
    end.
