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

-module(lasp_adcounter_test).
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
    ?assertEqual([0,0,0,0,0], Result),

    pass.

-endif.

test() ->
    %% Generate advertisement counters.

    Generator = fun(_) ->
            {ok, Id} = lasp:declare(riak_dt_gcounter),
            Id
    end,
    Ads = lists:map(Generator, lists:seq(1,5)),

    %% Generate a bunch of clients, which will be responsible for
    %% displaying ads and updating the advertisement counters.

    Launcher = fun(Id) -> spawn_link(?MODULE, client, [Id, Ads]) end,
    Clients = lists:map(Launcher, lists:seq(1,5)),

    %% Start a server process for each advertisement counter, which will
    %% track the number of impressions and disable the advertisement
    %% when it hits the impression limit.

    Server = fun(Ad) -> spawn_link(?MODULE, server, [Ad, Clients]) end,
    lists:map(Server, Ads),

    %% Begin simulation.

    Viewer = fun(_) ->
            Pid = lists:nth(random:uniform(5), Clients),
            Pid ! view_ad
    end,
    lists:map(Viewer, lists:seq(1,100)),

    timer:sleep(1000),

    lager:info("Gathering totals..."),
    Totals = fun(Ad) ->
            {ok, {_, _, Value, _}} = lasp:read(Ad, 0),
            Impressions = riak_dt_gcounter:value(Value),
            lager:info("Advertisements: ~p impressions: ~p",
                       [Ad, Impressions])
    end,
    lists:map(Totals, Ads),

    %% Ensure we exhaust all available ads.

    lager:info("Verifying all impressions were used."),
    AdsRemaining = fun(Client) ->
            Client ! {active_ads, self()},
            receive
                X ->
                    length(X)
            end
    end,
    lists:map(AdsRemaining, Clients).

%% @doc Server functions for the advertisement counter.
server(Ad, Clients) ->

    %% Threshold read on the value of the impression counter being at
    %% least 5.
    {ok, _} = lasp:read(Ad, 5),

    %% Notify all clients to remove the advertisement.
    lists:map(fun(Client) -> Client ! {remove_ad, Ad} end, Clients),

    lager:info("Disabled advertisement: ~p", [Ad]).

%% @doc Client process; standard recurisve looping server.
client(Id, Ads) ->
    receive
        {active_ads, Pid} ->
            Pid ! Ads;
        view_ad ->
            case Ads of
                [] ->
                    lager:info("No advertisements to display."),
                    client(Id, Ads);
                _ ->
                    %% Choose an advertisement to display.
                    Ad = hd(Ads),

                    %% Update impression count.
                    {ok, _} = lasp:update(Ad, increment, Id),
                    lager:info("Incremented counter for ad: ~p", [Ad]),

                    client(Id, tl(Ads) ++ [Ad])
            end;
        {remove_ad, Ad} ->
            lager:info("Advertisement ~p removed!", [Ad]),
            client(Id, Ads -- [Ad])
    end.
