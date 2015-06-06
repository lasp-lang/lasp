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

%% @doc Advertisement counter, OR-Set example.

-module(lasp_adcounter_orset_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0,
         client/3,
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
    ?assertEqual([], Result),

    pass.

-endif.

test() ->
    %% Generate an OR-set for tracking advertisement counters.
    {ok, Ads} = lasp:declare(lasp_orset),

    %% Build an advertisement counter, and add it to the set.
    OriginalAdList = lists:foldl(fun(_, Acc) ->
                {ok, Id} = lasp:declare(riak_dt_gcounter),
                {ok, _} = lasp:update(Ads, {add, Id}, undefined),
                [Id] ++ Acc
                end, [], lists:seq(1,5)),

    %% Generate a OR-set for tracking clients.
    {ok, Clients} = lasp:declare(lasp_orset),

    %% Each client takes the full list of ads when it starts, and reads
    %% from the variable store.
    lists:foldl(fun(Id, _Clients) ->
                ClientPid = spawn_link(?MODULE, client,
                                       [Id, Ads, undefined]),
                {ok, _} = lasp:update(Clients, {add, ClientPid},
                                      undefined),
                Clients
                end, Clients, lists:seq(1,5)),

    %% Launch a server process for each advertisement, which will block
    %% until the advertisement should be disabled.

    %% Create a OR-set for the server list.
    {ok, Servers} = lasp:declare(lasp_orset),

    %% Get the current advertisement list.
    {ok, {_, _, AdList0}} = lasp:read(Ads, {strict, undefined}),
    AdList = lasp_orset:value(AdList0),

    %% For each advertisement, launch one server for tracking it's
    %% impressions and wait to disable.
    lists:foldl(fun(Ad, _Servers) ->
                ServerPid = spawn_link(?MODULE, server, [Ad, Ads]),
                {ok, _} = lasp:update(Servers, {add, ServerPid},
                                      undefined),
                Servers
                end, Servers, AdList),

    %% Start the client simulation.

    %% Get client list.
    {ok, {_, _, ClientList0}} = lasp:read(Clients, {strict, undefined}),
    ClientList = lasp_orset:value(ClientList0),

    Viewer = fun(_) ->
            Pid = lists:nth(random:uniform(5), ClientList),
            Pid ! view_ad
    end,
    lists:map(Viewer, lists:seq(1,100)),

    timer:sleep(1000),

    lager:info("Gathering totals..."),
    Totals = fun(Ad) ->
            {ok, {_, _, Value}} = lasp:read(Ad, 0),
            Impressions = riak_dt_gcounter:value(Value),
            lager:info("Advertisements: ~p impressions: ~p",
                       [Ad, Impressions])
    end,
    lists:map(Totals, OriginalAdList),

    lager:info("Verifying all impressions were used."),
    {ok, {_, _, FinalAdList0}} = lasp:read(Ads, {strict, undefined}),
    lasp_orset:value(FinalAdList0).

%% @doc Server functions for the advertisement counter.  After 5 views,
%%      disable the advertisement.
%%
server(Ad, Ads) ->
    %% Blocking threshold read for 5 advertisement impressions.
    {ok, _} = lasp:read(Ad, 5),

    %% Remove the advertisement.
    {ok, _} = lasp:update(Ads, {remove, Ad}, Ad),

    lager:info("Removing ad: ~p", [Ad]).

%% @doc Client process; standard recurisve looping server.
client(Id, Ads, PreviousValue) ->
    receive
        view_ad ->
            %% Get current ad list.
            {ok, {_, _, AdList0}} = lasp:read(Ads, PreviousValue),
            AdList = lasp_orset:value(AdList0),

            case length(AdList) of
                0 ->
                    %% No advertisements left to display; ignore
                    %% message.
                    client(Id, Ads, AdList0);
                _ ->
                    %% Select a random advertisement from the list of
                    %% active advertisements.
                    Ad = lists:nth(random:uniform(length(AdList)),
                                   AdList),

                    %% Increment it.
                    {ok, _} = lasp:update(Ad, increment, Id),
                    lager:info("Incremented ad counter: ~p", [Ad]),

                    client(Id, Ads, AdList0)
            end
    end.
