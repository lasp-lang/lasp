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

-module(lasp_advertisement_counter).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([run/1,
         server/2]).

-behaviour(lasp_simulation).

-include("lasp.hrl").

-export([synchronize/5,
         log_transmission/1,
         log_divergence/2,
         view_ad/4]).

%% lasp_simulation callbacks
-export([init/1,
         clients/1,
         simulate/1,
         wait/1,
         terminate/1,
         summarize/1]).

run(Args) ->
    lasp_simulation:run(?MODULE, Args).

%% Macro definitions.

%% The maximum number of impressions for each advertisement to display.
-define(MAX_IMPRESSIONS, 50000).

%% Log frequency.
-define(FREQ, 1000).

%% Record definitions.

-record(ad, {id, image, counter}).

-record(contract, {id}).

-record(state, {runner,
                nodes,
                ads,
                ad_list,
                ads_with_contracts,
                client_list,
                count_events = 1,
                set_type,
                counter_type,
                num_events,
                num_clients,
                sync_interval,
                filenames}).

%% Callback functions.

%% @doc Setup lists of advertisements and lists of contracts for
%%      advertisements.
init([Nodes, Deltas, SetType, CounterType, NumEvents, NumClients, SyncInterval]) ->
    %% Enable or disable deltas.
    ok = application:set_env(?APP, delta_mode, Deltas),

    %% Get the process identifier of the runner.
    Runner = self(),

    %% For each identifier, generate a contract.
    {ok, {Contracts, _, _, _}} = lasp:declare(SetType),

    %% Generate Rovio's advertisements.
    {ok, {RovioAds, _, _, _}} = lasp:declare(SetType),
    RovioAdList = create_advertisements_and_contracts(CounterType, RovioAds, Contracts),

    %% Generate Riot's advertisements.
    {ok, {RiotAds, _, _, _}} = lasp:declare(SetType),
    RiotAdList = create_advertisements_and_contracts(CounterType, RiotAds, Contracts),

    %% Union ads.
    {ok, {Ads, _, _, _}} = lasp:declare(SetType),
    ok = lasp:union(RovioAds, RiotAds, Ads),

    %% Compute the Cartesian product of both ads and contracts.
    {ok, {AdsContracts, _, _, _}} = lasp:declare(SetType),
    ok = lasp:product(Ads, Contracts, AdsContracts),

    %% Filter items by join on item it.
    {ok, {AdsWithContracts, _, _, _}} = lasp:declare(SetType),
    FilterFun = fun({#ad{id=Id1}, #contract{id=Id2}}) ->
        Id1 =:= Id2
    end,
    ok = lasp:filter(AdsContracts, FilterFun, AdsWithContracts),

    %% Store the original list of ads.
    AdList = RiotAdList ++ RovioAdList,

    %% Launch server processes.
    servers(SetType, Ads, AdsWithContracts),

    %% Initialize divergence transmission instrumentation.
    DivergenceFilename = string:join(["divergence",
                                      atom_to_list(Deltas),
                                      atom_to_list(SetType),
                                      atom_to_list(CounterType),
                                      integer_to_list(NumEvents),
                                      integer_to_list(NumClients),
                                      integer_to_list(SyncInterval)], "-") ++ ".csv",
    ok = lasp_divergence_instrumentation:start(DivergenceFilename, NumClients),

    %% Initialize client transmission instrumentation.
    ClientFilename = string:join(["state-client",
                                  atom_to_list(Deltas),
                                  atom_to_list(SetType),
                                  atom_to_list(CounterType),
                                  integer_to_list(NumEvents),
                                  integer_to_list(NumClients),
                                  integer_to_list(SyncInterval)], "-") ++ ".csv",
    ok = lasp_transmission_instrumentation:start(client, ClientFilename, NumClients),

    %% Initialize server transmission instrumentation.
    ServerFilename = string:join(["state-server",
                                  atom_to_list(Deltas),
                                  atom_to_list(SetType),
                                  atom_to_list(CounterType),
                                  integer_to_list(NumEvents),
                                  integer_to_list(NumClients),
                                  integer_to_list(SyncInterval)], "-") ++ ".csv",
    ok = lasp_transmission_instrumentation:start(server, ServerFilename, NumClients),

    {ok, #state{runner=Runner,
                nodes=Nodes,
                ads=Ads,
                ad_list=AdList,
                ads_with_contracts=AdsWithContracts,
                set_type=SetType,
                counter_type=CounterType,
                num_events=NumEvents,
                num_clients=NumClients,
                sync_interval=SyncInterval,
                filenames=[DivergenceFilename, ClientFilename, ServerFilename]}}.

%% @doc Launch a series of client processes, each of which is responsible
%% for displaying a particular advertisement.
clients(#state{runner=Runner, nodes=Nodes, num_clients=NumClients, set_type=SetType,
               counter_type=CounterType, sync_interval=SyncInterval,
               ads_with_contracts=AdsWithContracts}=State) ->
    %% Each client takes the full list of ads when it starts, and reads
    %% from the variable store.
    Clients = launch_clients(NumClients, Nodes, SetType, CounterType,
                             SyncInterval, Runner, AdsWithContracts),
    {ok, State#state{client_list=Clients}}.

%% @doc Terminate any running clients gracefully issuing final
%%      synchronization.
terminate(#state{client_list=ClientList}=State) ->
    TerminateFun = fun(Pid) ->
            Pid ! terminate
    end,
    lists:foreach(TerminateFun, ClientList),
    lasp_transmission_instrumentation:stop(client),
    lasp_transmission_instrumentation:stop(server),
    {ok, State}.

%% @doc Simulate clients viewing advertisements.
simulate(#state{client_list=_ClientList, num_events=_NumEvents}=State) ->
    %% Do nothing, as clients will simulate their own events.
    {ok, State}.

%% @doc Summarize results.
summarize(#state{filenames=Filenames}) ->
    {ok, Filenames}.

%% @doc Wait for all events to be delivered in the system.
wait(#state{count_events=Count, num_events=NumEvents}=State) ->
    receive
        view_ad_complete ->
            case Count >= NumEvents of
                true ->
                    lager:info("Events all processed!"),
                    lasp_divergence_instrumentation:stop(),
                    {ok, State};
                false ->
                    case Count rem ?FREQ == 0 of
                        true ->
                            lager:info("Event ~p of ~p processed",
                                       [Count, NumEvents]);
                        false ->
                            ok
                    end,
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
    {ok, _} = lasp:update(Ads, {remove, Ad}, Ad).

%% @doc Generate advertisements and advertisement contracts.
create_advertisements_and_contracts(Counter, Ads, Contracts) ->
    AdIds = lists:map(fun(_) ->
                              {ok, Unique} = lasp_unique:unique(),
                              Unique
                      end, lists:seq(1, 10)),
    lists:map(fun(Id) ->
                {ok, _} = lasp:update(Contracts,
                                      {add, #contract{id=Id}},
                                      undefined)
                end, AdIds),
    lists:map(fun(Id) ->
                %% Generate a G-Counter.
                {ok, {CounterId, _, _, _}} = lasp:declare(Counter),

                Ad = #ad{id=Id, counter=CounterId},

                %% Add it to the advertisement set.
                {ok, _} = lasp:update(Ads, {add, Ad}, undefined),

                Ad

                end, AdIds).

%% @doc Periodically synchronize state with the server.
%%
%%      Periodic synchronization serves as the anti-entropy process with
%%      clients; we can't assume delta delivery for clients when they go
%%      offline, unless we keep a global counter of the
%%      greatest-lower-bound across all clients for delta delivery,
%%      which assumes we understand the client topology.
%%
synchronize(SetType, AdsWithContractsId, AdsWithContracts0, Counters0, CountersDelta0) ->
    %% Get latest list of advertisements from the server.
    {ok, {_, _, _, AdsWithContracts}} = lasp:read(AdsWithContractsId, AdsWithContracts0),
    %% Log state received from the server.
    log_transmission(AdsWithContracts),
    AdList = SetType:value(AdsWithContracts),
    Identifiers = [Id || {#ad{counter=Id}, _} <- AdList],

    %% Refresh our dictionary with any new values from the server.
    %%
    %% 1.) Given the list of new values from the server...
    %% 2.) ...fill in any holes in our dictionary accordingly.
    %%
    RefreshFun = fun({#ad{counter=Ad}, _}, Acc) ->
                      case dict:is_key(Ad, Acc) of
                          false ->
                              {ok, {_, _, _, Counter}} = lasp:read(Ad, undefined),
                              %% Log state received from the server.
                              log_transmission(Counter),
                              dict:store(Ad, Counter, Acc);
                          true ->
                              Acc
                      end
              end,
    Counters = lists:foldl(RefreshFun, Counters0, AdList),

    %% Bind our latest values with the server process.
    %%
    %% 1.) Iterate our dictionary, and upload any state/deltas pending.
    %%     Store returned value from the server.
    %% 2.) If the item in our dictionary is no longer part of the server
    %%     state, prune it by identifier.
    %%
    SyncFun = fun(Ad, Counter0, Acc) ->
                      Counter = case application:get_env(?APP, delta_mode, false) of
                          true ->
                              case dict:find(Ad, CountersDelta0) of
                                  {ok, Delta} ->
                                      %% Log transmission of the local delta.
                                      log_transmission(Delta),

                                      {ok, {_, _, _, Counter1}} = lasp:bind(Ad, Delta),

                                      %% Log receipt of information from the server.
                                      log_transmission(Counter1),

                                      %% Return server value.
                                      Counter1;
                                  _ ->
                                      %% Transmit nothing.
                                      Counter0
                              end;
                          false ->
                              %% Log transmission of the local delta (or state).
                              log_transmission(Counter0),

                              {ok, {_, _, _, Counter1}} = lasp:bind(Ad, Counter0),

                              %% Log receipt of information from the server.
                              log_transmission(Counter1),

                              Counter1
                      end,

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
servers(SetType, Ads, AdsWithContracts) ->
    %% Create a OR-set for the server list.
    {ok, {Servers, _, _, _}} = lasp:declare(SetType),

    %% Get the current advertisement list.
    {ok, {_, _, _, AdList0}} = lasp:read(AdsWithContracts, {strict, undefined}),
    AdList = SetType:value(AdList0),

    %% For each advertisement, launch one server for tracking it's
    %% impressions and wait to disable.
    lists:map(fun(Ad) ->
                ServerPid = spawn_link(?MODULE, server, [Ad, Ads]),
                {ok, _} = lasp:update(Servers, {add, ServerPid}, undefined),
                ServerPid
                end, AdList).

%% @private
log_transmission(Term) ->
    case application:get_env(?APP, instrumentation, false) of
        true ->
            lasp_transmission_instrumentation:log(client, Term, node());
        false ->
            ok
    end.

%% @private
log_divergence(buffer, Number) ->
    case application:get_env(?APP, instrumentation, false) of
        true ->
            lasp_divergence_instrumentation:buffer(Number, node());
        false ->
            ok
    end;
log_divergence(flush, Number) ->
    case application:get_env(?APP, instrumentation, false) of
        true ->
            lasp_divergence_instrumentation:flush(Number, node());
        false ->
            ok
    end.

%% @private
view_ad(CounterType, Id, Counters0, CountersDelta0) ->
    case dict:size(Counters0) of
        0 ->
            {ok, {Counters0, CountersDelta0}};
        _ ->
            %% Select a random advertisement from the list of
            %% active advertisements.
            Random = random:uniform(dict:size(Counters0)),
            {Ad, Counter0} = lists:nth(Random, dict:to_list(Counters0)),
            view_ad(CounterType, Id, Counters0, CountersDelta0, Ad, Counter0)
    end.

%% @private
view_ad(CounterType, Id, Counters0, CountersDelta0, Ad, Counter0) ->
    case application:get_env(?APP, delta_mode, false) of
        true ->
            %% If deltas are enabled, then we maintain two pieces of
            %% state: a.) local dictionary of state, and b.) local
            %% dictionary of delta intervals waiting to be transmitted
            %% to the central datacenter.

            %% First, always ensure that our counter is merged up with
            %% the delta interval before doing any work; if we don't
            %% have a delta for the object, assume bottom.
            CounterDelta0 = case dict:find(Ad, CountersDelta0) of
                           {ok, PreviousDelta} ->
                               PreviousDelta;
                           error ->
                               CounterType:new()
                       end,
            MergedCounter = CounterType:merge(CounterDelta0, Counter0),

            %% Generate delta for current operation from new state.
            {ok, {delta, Delta}} = CounterType:update_delta(increment, Id, MergedCounter),
            %% Merge new delta with old delta and store in interval
            %% dictionary for next synchronization interval.
            CounterDelta = CounterType:merge(Delta, CounterDelta0),

            %% Merge new delta into old state and store in the state
            %% dictionary.
            Counter = CounterType:merge(MergedCounter, CounterDelta),

            %% At this point we should have a new delta interval
            %% computed and a new state, so update dictionaries
            %% accordingly.
            {ok, {dict:store(Ad, Counter, Counters0),
                  dict:store(Ad, CounterDelta, CountersDelta0)}};
        false ->
            %% If deltas are disabled, then just create a new copy of
            %% the object and store it in the local nodes dictionary.
            {ok, Counter} = CounterType:update(increment, Id, Counter0),
            {ok, {dict:store(Ad, Counter, Counters0), CountersDelta0}}
    end.

%% @private
% memory_report() ->
%     MemoryData = {_, _, {BadPid, _}} = memsup:get_memory_data(),
%     lager:info(""),
%     lager:info("-----------------------------------------------------------", []),
%     lager:info("Allocated areas: ~p", [erlang:system_info(allocated_areas)]),
%     lager:info("Worst: ~p", [process_info(BadPid)]),
%     lager:info("Worst trace: ~s", [element(2, erlang:process_info(BadPid, backtrace))]),
%     lager:info("Memory Data: ~p", [MemoryData]),
%     lager:info("System memory data: ~p", [memsup:get_system_memory_data()]),
%     lager:info("Local process count: ~p", [length(processes())]),
%     lager:info("-----------------------------------------------------------", []),
%     lager:info("").

%% @private
launch_clients(NumClients, Nodes, SetType, CounterType, SyncInterval,
               Runner, AdsWithContracts) ->
    lists:flatmap(fun(Node) ->
                        launch_clients(Node, NumClients, Nodes, SetType, CounterType, SyncInterval,
                                       Runner, AdsWithContracts)
                  end, Nodes).

%% @private
launch_clients(Node, NumClients, _Nodes, SetType, CounterType, SyncInterval,
               Runner, AdsWithContracts) ->
    lists:map(fun(Id) ->
                      {ok, Pid} = rpc:call(Node,
                                           lasp_advertisement_counter_client,
                                           start_link,
                                           [
                                            SetType,
                                            CounterType,
                                            SyncInterval,
                                            Runner,
                                            Id,
                                            AdsWithContracts
                                           ]),
                      Pid
              end, lists:seq(1, NumClients)).
