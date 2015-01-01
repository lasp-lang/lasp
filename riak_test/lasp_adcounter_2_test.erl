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

%% @doc Advertisement counter, second pass.

-module(lasp_adcounter_2_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0,
         client/2,
         server/2]).

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
    Result = rpc:call(Node, ?MODULE, test, []),
    ?assertEqual(ok, Result),

    pass.

-endif.

test() ->
    lager:info("Initialize advertisement counters..."),
    {ok, Ads} = lasp:declare(riak_dt_orset),

    Ads = lists:foldl(fun(_, _Ads) ->
                    lager:info("Generating advertisement!"),
                    {ok, Id} = lasp:declare(riak_dt_gcounter),

                    lager:info("Adding to counter set."),
                    {ok, _, _} = lasp:update(Ads, {add, Id}),

                    Ads
            end, Ads, lists:seq(1,5)),

    lager:info("Getting current list of advertisements..."),
    {ok, AType, AValue, _} = lasp:read(Ads, riak_dt_orset:new()),

    lager:info("Launching clients..."),
    {ok, Clients} = lasp:declare(riak_dt_orset),

    _ = lists:foldl(fun(Id, _Clients) ->
                    ClientPid = spawn(?MODULE, client, [Id, AType:value(AValue)]),
                    lager:info("Launched client; id: ~p pid: ~p",
                               [Id, ClientPid]),
                    {ok, _, _} = lasp:update(Clients, {add, ClientPid}),
                    Clients
            end, Clients, lists:seq(1,5)),

    lager:info("Getting current list of clients..."),
    {ok, CType, CValue, _} = lasp:read(Clients, riak_dt_orset:new()),

    lager:info("Launch a server for each advertisement..."),
    {ok, Servers} = lasp:declare(riak_dt_orset),

    _ = lists:foldl(fun(Ad, _Servers) ->
                    ServerPid = spawn(?MODULE, server, [Ad, CType:value(CValue)]),
                    lager:info("Launched server; ad: ~p pid: ~p",
                               [Ad, ServerPid]),
                    {ok, _, _} = lasp:update(Servers, {add, ServerPid}),
                    Servers
            end, Servers, AType:value(AValue)),

    lager:info("Running advertisements..."),
    Viewer = fun(_) ->
            Pid = lists:nth(random:uniform(5), CType:value(CValue)),
            io:format("Running advertisement for pid: ~p~n", [Pid]),
            Pid ! view_ad
    end,
    _ = lists:map(Viewer, lists:seq(1,50)),
    ok.

%% @doc Server functions for the advertisement counter.  After 5 views,
%%      disable the advertisement.
server(Ad, Clients) ->
    lager:info("Server launched for ad: ~p", [Ad]),
    {ok, _, _, _} = lasp:read(Ad, 5),
    lager:info("Threshold reached; disable ad ~p for all clients!",
               [Ad]),
    lists:map(fun(Client) ->
                %% Tell clients to remove the advertisement.
                Client ! {remove_ad, Ad}
        end, Clients),
    io:format("Advertisement ~p reached display limit!", [Ad]).

%% @doc Client process; standard recurisve looping server.
%%
client(Id, Ads) ->
    lager:info("Client ~p running; ads: ~p~n", [Id, Ads]),
    receive
        {active_ads, Pid} ->
            Pid ! Ads,
            client(Id, Ads);
        view_ad ->
            case length(Ads) of
                0 ->
                    client(Id, Ads);
                _ ->
                    %% Choose an advertisement to display.
                    Ad = hd(Ads),
                    lager:info("Displaying ad: ~p from client: ~p~n", [Ad, Id]),

                    %% Update ad by incrementing value.
                    {ok, _, _} = lasp:update(Ad, increment),

                    client(Id, tl(Ads) ++ [Ad])
            end;
        {remove_ad, Ad} ->
            %% Remove ad.
            lager:info("Removing ad: ~p from client: ~p~n", [Ad, Id]),
            client(Id, Ads -- [Ad])
    end.
