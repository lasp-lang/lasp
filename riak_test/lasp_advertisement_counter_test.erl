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

-module(lasp_advertisement_counter_test).
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
    ?assertEqual(ok, Result),

    pass.

-endif.

-define(SET, riak_dt_orset).

-define(COUNTER, riak_dt_gcounter).

-record(ad, {id, image, counter}).

-record(contract, {id}).

test() ->
    %% Setup lists of advertisements and lists of contracts for
    %% advertisements.

    %% Generate a series of unique identifiers.
    RovioAdIds = lists:map(fun(_) -> druuid:v4() end, lists:seq(1, 10)),
    lager:info("Rovio Ad Identifiers are: ~p", [RovioAdIds]),

    TriforkAdIds = lists:map(fun(_) -> druuid:v4() end, lists:seq(1, 10)),
    lager:info("Trifork Ad Identifiers are: ~p", [TriforkAdIds]),

    Ids = RovioAdIds ++ TriforkAdIds,
    lager:info("Ad Identifiers are: ~p", [Ids]),

    %% Generate Rovio's advertisements.
    {ok, RovioAds} = lasp:declare(?SET),
    lists:map(fun(Id) ->
                %% Generate a G-Counter.
                {ok, CounterId} = lasp:declare(?COUNTER),

                %% Add it to the advertisement set.
                {ok, _} = lasp:update(RovioAds,
                                      {add, #ad{id=Id, counter=CounterId}},
                                      undefined)

                end, RovioAdIds),

    %% Generate Trifork's advertisements.
    {ok, TriforkAds} = lasp:declare(?SET),
    lists:map(fun(Id) ->
                %% Generate a G-Counter.
                {ok, CounterId} = lasp:declare(?COUNTER),

                %% Add it to the advertisement set.
                {ok, _} = lasp:update(TriforkAds,
                                      {add, #ad{id=Id, counter=CounterId}},
                                      undefined)

                end, TriforkAdIds),

    %% Union ads.
    {ok, Ads} = lasp:declare(?SET),
    ok = lasp:union(RovioAds, TriforkAds, Ads),

    %% Debug; print the list of advertisements to the log.
    timer:sleep(500),
    {ok, {_, _, Ads0, _}} = lasp:read(Ads, undefined),
    lager:info("Current advertisements: ~p",
               [?SET:value(Ads0)]),

    %% For each identifier, generate a contract.
    {ok, Contracts} = lasp:declare(?SET),
    lists:map(fun(Id) ->
                {ok, _} = lasp:update(Contracts,
                                      {add, #contract{id=Id}},
                                      undefined)
                end, Ids),

    %% Debug; print the list of advertisements to the log.
    {ok, {_, _, Contracts0, _}} = lasp:read(Contracts, undefined),
    lager:info("Current contracts: ~p",
               [?SET:value(Contracts0)]),

    %% Compute the Cartesian product of both ads and contracts.
    {ok, AdsContracts} = lasp:declare(?SET),
    ok = lasp:product(Ads, Contracts, AdsContracts),

    %% Debug; print the product.
    timer:sleep(500),
    {ok, {_, _, AdsContracts0, _}} = lasp:read(AdsContracts, undefined),
    lager:info("Current ads-contracts: ~p",
               [?SET:value(AdsContracts0)]),

    %% Filter items by join on item it.
    {ok, AdsWithContracts} = lasp:declare(?SET),
    FilterFun = fun({#ad{id=Id1}, #contract{id=Id2}}) ->
        Id1 =:= Id2
    end,
    ok = lasp:filter(AdsContracts, FilterFun, AdsWithContracts),

    %% Debug; print the filtered.
    timer:sleep(500),
    {ok, {_, _, AdsWithContracts0, _}} = lasp:read(AdsWithContracts,
                                                   undefined),
    lager:info("Current ads with contracts: ~p",
               [?SET:value(AdsWithContracts0)]),

    %% Launch a series of client processes, each of which is responsible
    %% for displaying a particular advertisement.

    %% Generate a OR-set for tracking clients.
    {ok, Clients} = lasp:declare(?SET),

    %% Each client takes the full list of ads when it starts, and reads
    %% from the variable store.
    lists:map(fun(Id) ->
                ClientPid = spawn_link(?MODULE, client,
                                       [Id, AdsWithContracts, undefined]),
                {ok, _} = lasp:update(Clients,
                                      {add, ClientPid},
                                      undefined)
                end, lists:seq(1,5)),

    %% Launch a server process for each advertisement, which will block
    %% until the advertisement should be disabled.

    %% Create a OR-set for the server list.
    {ok, Servers} = lasp:declare(?SET),

    %% Get the current advertisement list.
    {ok, {_, _, AdList0, _}} = lasp:read(AdsWithContracts),
    AdList = riak_dt_orset:value(AdList0),

    %% For each advertisement, launch one server for tracking it's
    %% impressions and wait to disable.
    lists:map(fun(Ad) ->
                ServerPid = spawn_link(?MODULE, server, [Ad, Ads]),
                {ok, _} = lasp:update(Servers,
                                      {add, ServerPid},
                                      undefined)
                end, AdList),

    %% Get client list.
    {ok, {_, _, ClientList0, _}} = lasp:read(Clients),
    ClientList = ?SET:value(ClientList0),

    Viewer = fun(_) ->
            Pid = lists:nth(random:uniform(5), ClientList),
            Pid ! view_ad
    end,
    lists:map(Viewer, lists:seq(1,100)),

    ok.

%% @doc Server functions for the advertisement counter.  After 5 views,
%%      disable the advertisement.
%%
server({#ad{counter=Counter}=Ad, _}, Ads) ->
    %% Blocking threshold read for 5 advertisement impressions.
    {ok, _} = lasp:read(Counter, 5),

    %% Remove the advertisement.
    {ok, _} = lasp:update(Ads, {remove, Ad}, Ad),

    lager:info("Removing ad: ~p", [Ad]).

%% @doc Client process; standard recurisve looping server.
client(Id, AdsWithContracts, PreviousValue) ->
    receive
        view_ad ->
            %% Get current ad list.
            {ok, {_, _, AdList0, _}} = lasp:read(AdsWithContracts,
                                                 PreviousValue),
            AdList = riak_dt_orset:value(AdList0),

            case length(AdList) of
                0 ->
                    %% No advertisements left to display; ignore
                    %% message.
                    client(Id, AdsWithContracts, AdList0);
                _ ->
                    %% Select a random advertisement from the list of
                    %% active advertisements.
                    {#ad{counter=Ad}, _} = lists:nth(
                            random:uniform(length(AdList)), AdList),

                    %% Increment it.
                    {ok, _} = lasp:update(Ad, increment, Id),
                    lager:info("Incremented ad counter: ~p", [Ad]),

                    client(Id, AdsWithContracts, AdList0)
            end
    end.
