%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Christopher S. Meiklejohn.  All Rights Reserved.
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
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([test/0,
         client/5,
         server/2]).

-behaviour(lasp_test).

%% lasp_test callbacks
-export([init/0,
         clients/1,
         simulate/1,
         wait/1,
         terminate/1,
         summarize/1]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = lasp_test_helpers:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = lasp_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = lasp_test_helpers:wait_for_cluster(Nodes),

    lager:info("Remotely executing the test."),
    Result = rpc:call(Node, ?MODULE, test, []),
    ?assertMatch({ok, _}, Result),

    pass.

-endif.

test() ->
    lasp_test:test(?MODULE).

%% Macro definitions.

%% Set type to use.
-define(SET, lasp_orset).

%% Counter type to use.
-define(COUNTER, riak_dt_gcounter).

%% The maximum number of impressions for each advertisement to display.
-define(MAX_IMPRESSIONS, 5).

%% The number of events to sent to clients.
-define(NUM_EVENTS, 5000).

%% The number of clients.
-define(NUM_CLIENTS, 10).

%% Synchronization interval.
-define(SYNC_INTERVAL, 10).

%% Record definitions.

-record(ad, {id, image, counter}).

-record(contract, {id}).

-record(state, {runner,
                ads,
                ad_list,
                ads_with_contracts,
                client_list,
                count_events = 1,
                num_events = ?NUM_EVENTS}).

%% Callback functions.

%% @doc Setup lists of advertisements and lists of contracts for
%%      advertisements.
init() ->
    %% Get the process identifier of the runner.
    Runner = self(),

    %% For each identifier, generate a contract.
    {ok, {Contracts, _, _, _}} = lasp:declare(?SET),

    %% Generate Rovio's advertisements.
    {ok, {RovioAds, _, _, _}} = lasp:declare(?SET),
    RovioAdList = create_advertisements_and_contracts(RovioAds, Contracts),

    %% Generate Riot's advertisements.
    {ok, {RiotAds, _, _, _}} = lasp:declare(?SET),
    RiotAdList = create_advertisements_and_contracts(RiotAds, Contracts),

    %% Union ads.
    {ok, {Ads, _, _, _}} = lasp:declare(?SET),
    ok = lasp:union(RovioAds, RiotAds, Ads),

    %% Compute the Cartesian product of both ads and contracts.
    {ok, {AdsContracts, _, _, _}} = lasp:declare(?SET),
    ok = lasp:product(Ads, Contracts, AdsContracts),

    %% Filter items by join on item it.
    {ok, {AdsWithContracts, _, _, _}} = lasp:declare(?SET),
    FilterFun = fun({#ad{id=Id1}, #contract{id=Id2}}) ->
        Id1 =:= Id2
    end,
    ok = lasp:filter(AdsContracts, FilterFun, AdsWithContracts),

    %% Store the original list of ads.
    AdList = RiotAdList ++ RovioAdList,

    %% Launch server processes.
    servers(Ads, AdsWithContracts),

    {ok, #state{runner=Runner,
                ads=Ads,
                ad_list=AdList,
                ads_with_contracts=AdsWithContracts}}.

%% @doc Launch a series of client processes, each of which is responsible
%% for displaying a particular advertisement.
clients(#state{runner=Runner,
               ads_with_contracts=AdsWithContracts}=State) ->
    %% Each client takes the full list of ads when it starts, and reads
    %% from the variable store.
    Clients = lists:map(fun(Id) ->
                                spawn_link(?MODULE,
                                           client,
                                           [Runner,
                                            Id,
                                            AdsWithContracts,
                                            undefined,
                                            dict:new()])
                                end, lists:seq(1, ?NUM_CLIENTS)),
    {ok, State#state{client_list=Clients}}.


%% @doc Terminate any running clients gracefully issuing final
%%      synchronization.
terminate(#state{client_list=ClientList}=State) ->
    TerminateFun = fun(Pid) -> Pid ! terminate end,
    lists:map(TerminateFun, ClientList),
    {ok, State}.

%% @doc Simulate clients viewing advertisements.
simulate(#state{client_list=ClientList}=State) ->
    %% Start the simulation.
    Viewer = fun(_) ->
            Random = random:uniform(length(ClientList)),

            timer:sleep(10),

            Pid = lists:nth(Random, ClientList),
            Pid ! view_ad
    end,
    lists:foreach(Viewer, lists:seq(1, ?NUM_EVENTS)),
    {ok, State}.

%% @doc Summarize results.
summarize(#state{ad_list=AdList}=State) ->
    %% Wait until all advertisements have been exhausted before stopping
    %% execution of the test.
    lager:info("AdList is: ~p", [AdList]),
    Overcounts = lists:map(fun(#ad{counter=CounterId}) ->
                {ok, V} = lasp:query(CounterId),
                lager:info("Advertisement ~p reached max impressions: ~p with ~p....",
                           [CounterId, ?MAX_IMPRESSIONS, V]),
                V - ?MAX_IMPRESSIONS
        end, AdList),

    Sum = fun(X, Acc) ->
            X + Acc
    end,
    TotalOvercount = lists:foldl(Sum, 0, Overcounts),
    io:format("----------------------------------------"),
    io:format("Total overcount: ~p~n", [TotalOvercount]),
    io:format("Mean overcount per client: ~p~n", [TotalOvercount / ?NUM_CLIENTS]),
    io:format("----------------------------------------"),

    {ok, State}.

%% @doc Wait for all events to be delivered in the system.
wait(#state{count_events=Count, num_events=NumEvents}=State) ->
    receive
        view_ad ->
            case Count >= NumEvents of
                true ->
                    {ok, State};
                false ->
                    wait(State#state{count_events=Count + 1})
            end
    end.

%% Internal functions.

%% @doc Server functions for the advertisement counter.  After 5 views,
%%      disable the advertisement.
server({#ad{counter=Counter}=Ad, _}, Ads) ->
    %% Blocking threshold read for 5 advertisement impressions.
    {ok, _} = lasp:read(Counter, {value, ?MAX_IMPRESSIONS}),

    %% Remove the advertisement.
    {ok, _} = lasp:update(Ads, {remove, Ad}, Ad),

    lager:info("Removing ad: ~p", [Ad]).

%% @doc Client process; standard recurisve looping server.
client(Runner, Id, AdsWithContractsId, AdsWithContracts0, Counters0) ->
    receive
        terminate ->
            ok;
        view_ad ->
            Counters = case dict:size(Counters0) of
                0 ->
                    Counters0;
                _ ->
                    %% Select a random advertisement from the list of
                    %% active advertisements.
                    Random = random:uniform(dict:size(Counters0)),
                    {Ad, Counter0} = lists:nth(Random, dict:to_list(Counters0)),
                    {ok, Counter} = ?COUNTER:update(increment, Id, Counter0),
                    dict:store(Ad, Counter, Counters0)
            end,

            %% Notify the harness that an event has been processed.
            Runner ! view_ad,

            client(Runner, Id, AdsWithContractsId, AdsWithContracts0, Counters)
    after
        ?SYNC_INTERVAL ->
            {ok, AdsWithContracts, Counters} = synchronize(AdsWithContractsId, AdsWithContracts0, Counters0),
            client(Runner, Id, AdsWithContractsId, AdsWithContracts, Counters)
    end.

%% @doc Generate advertisements and advertisement contracts.
create_advertisements_and_contracts(Ads, Contracts) ->
    AdIds = lists:map(fun(_) -> druuid:v4() end, lists:seq(1, 10)),
    lists:map(fun(Id) ->
                {ok, _} = lasp:update(Contracts,
                                      {add, #contract{id=Id}},
                                      undefined)
                end, AdIds),
    lists:map(fun(Id) ->
                %% Generate a G-Counter.
                {ok, {CounterId, _, _, _}} = lasp:declare(?COUNTER),

                Ad = #ad{id=Id, counter=CounterId},

                %% Add it to the advertisement set.
                {ok, _} = lasp:update(Ads, {add, Ad}, undefined),

                Ad

                end, AdIds).

%% @doc Periodically synchronize state with the server.
synchronize(AdsWithContractsId, AdsWithContracts0, Counters0) ->
    %% Get latest list of advertisements from the server.
    {ok, {_, _, _, AdsWithContracts}} = lasp:read(AdsWithContractsId, AdsWithContracts0),
    AdList = ?SET:value(AdsWithContracts),
    Identifiers = [Id || {#ad{counter=Id},_} <- AdList],

    %% Refresh our dictionary with any new values from the server.
    %%
    %% 1.) Given the list of new values from the server...
    %% 2.) ...fill in any holes in our dictionary accordingly.
    %%
    RefreshFun = fun({#ad{counter=Ad}, _}, Acc) ->
                      case dict:is_key(Ad, Acc) of
                          false ->
                              {ok, {_, _, _, Counter}} = lasp:read(Ad, undefined),
                              dict:store(Ad, Counter, Acc);
                          true ->
                              Acc
                      end
              end,
    Counters = lists:foldl(RefreshFun, Counters0, AdList),

    %% Bind our latest values with the server process.
    %%
    %% 1.) Send our dictionary of values to server.
    %% 2.) Server computes a bind operation with the incoming value.
    %% 3.) Server returns the merged value to the client.
    %%
    SyncFun = fun(Ad, Counter0, Acc) ->
                    {ok, {_, _, _, Counter}} = lasp:bind(Ad, Counter0),
                    case lists:member(Ad, Identifiers) of
                        true ->
                            dict:store(Ad, Counter, Acc);
                        false ->
                            Acc
                    end
              end,
    Counters1 = dict:fold(SyncFun, dict:new(), Counters),

    {ok, AdsWithContracts, Counters1}.

%% @doc Launch a server process for each advertisement, which will block
%% until the advertisement should be disabled.
servers(Ads, AdsWithContracts) ->
    %% Create a OR-set for the server list.
    {ok, {Servers, _, _, _}} = lasp:declare(?SET),

    %% Get the current advertisement list.
    {ok, {_, _, _, AdList0}} = lasp:read(AdsWithContracts, {strict, undefined}),
    AdList = ?SET:value(AdList0),

    %% For each advertisement, launch one server for tracking it's
    %% impressions and wait to disable.
    lists:map(fun(Ad) ->
                ServerPid = spawn_link(?MODULE, server, [Ad, Ads]),
                {ok, _} = lasp:update(Servers, {add, ServerPid}, undefined),
                ServerPid
                end, AdList).
