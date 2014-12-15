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

%% @doc Advertisement counter.

-module(derflow_adcounter_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-compile([export_all]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = rt:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = derflow_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = derflow_test_helpers:wait_for_cluster(Nodes),

    lager:info("Remotely executing the test."),
    Result = rpc:call(Node, ?MODULE, test, []),
    ?assertEqual(ok, Result),

    pass.

-endif.

test() ->
    lager:info("Initialize advertisement counters..."),
    Generator = fun(_) ->
            lager:info("Generating advertisement!"),
            {ok, Id} = derflow:declare(riak_dt_gcounter),
            Id
    end,
    Ads = lists:map(Generator, lists:seq(1,5)),

    lager:info("Launching clients..."),
    Launcher = fun(Id) ->
            ClientPid = spawn(?MODULE, client, [Id, Ads]),
            lager:info("Launched client; id: ~p pid: ~p~n", [Id, ClientPid]),
            ClientPid
    end,
    Clients = lists:map(Launcher, lists:seq(1,5)),

    lager:info("Launch a server for each advertisement..."),
    Server = fun(Ad) ->
            ServerPid = spawn(?MODULE, server, [Ad, Clients]),
            lager:info("Launched server; ad: ~p~n", [Ad]),
            ServerPid
    end,
    _Servers = lists:map(Server, Ads),

    lager:info("Running advertisements..."),
    Viewer = fun(_) ->
            Pid = lists:nth(random:uniform(5), Clients),
            io:format("Running advertisement for pid: ~p~n", [Pid]),
            Pid ! view_ad
    end,
    _ = lists:map(Viewer, lists:seq(1,50)),
    ok.

%% @doc Server functions for the advertisement counter.  After 5 views,
%       disable the advertisement.
server(Ad, Clients) ->
    lager:info("Server launched for ad: ~p", [Ad]),
    {ok, _, _} = derflow:read(Ad, 5),
    lager:info("Threshold reached; disable ad ~p for all clients!",
               [Ad]),
    lists:map(fun(Client) ->
                %% Tell clients to remove the advertisement.
                Client ! {remove_ad, Ad}
        end, Clients),
    io:format("Advertisement ~p reached display limit!", [Ad]).

%% @doc Client process; standard recurisve looping server.
client(Id, Ads) ->
    lager:info("Client ~p running; ads: ~p~n", [Id, Ads]),
    receive
        {active_ads, Pid} ->
            Pid ! Ads,
            client(Id, Ads);
        view_ad ->
            %% Choose an advertisement to display.
            Ad = hd(Ads),
            lager:info("Displaying ad: ~p from client: ~p~n", [Ad, Id]),

            %% Update ad by incrementing value.
            {ok, Value, _} = derflow:read(Ad),
            {ok, Updated} = riak_dt_gcounter:update(increment, Id, Value),
            {ok, _} = derflow:bind(Ad, Updated),

            client(Id, Ads);
        {remove_ad, Ad} ->
            %% Remove ad.
            lager:info("Removing ad: ~p from client: ~p~n", [Ad, Id]),
            client(Id, tl(Ads) -- [Ad])
    end.
