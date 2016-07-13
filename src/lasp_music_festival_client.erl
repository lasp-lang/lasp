%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(lasp_music_festival_client).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

%% State record.
-record(state, {actor, votes, identifiers}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([]) ->
    lager:info("Music festival client initialized."),

    %% Generate actor identifier.
    Actor = self(),

    %% Schedule voting.
    schedule_voting(),

    %% Schedule logging.
    schedule_logging(),

    %% Schedule check convergence
    schedule_check_convergence(),

    %% Declare a bunch of counters.
    Identifiers = lists:map(fun(N) ->
                    Id = {N, ?COUNTER_TYPE},
                    {ok, _} = lasp:declare(Id, ?COUNTER_TYPE),
                    Id
            end, identifiers()),

    {ok, #state{actor=Actor, votes=0, identifiers=Identifiers}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(log, #state{votes=Votes}=State) ->
    lager:info("Votes: ~p", [Votes]),

    %% Schedule logging.
    schedule_logging(),

    {noreply, State};

handle_info(vote, #state{actor=Actor, votes=Votes0, identifiers=Identifiers0}=State) ->
    %% Get number of counters.
    Size = length(Identifiers0),

    case Size of
        0 ->
            {noreply, State};
        _ ->
            %% Select random.
            Random = lasp_support:puniform(Size),

            %% Select random.
            Identifier = lists:nth(Random, Identifiers0),

            %% Increment counter.
            {ok, _} = lasp:update(Identifier, increment, Actor),

            %% Remove identifier.
            Identifiers = Identifiers0 -- [Identifier],

            %% Update CT instance
            {ok, _} = lasp:update(?CONVERGENCE_ID, {fst, increment}, Actor),

            %% Bump votes.
            Votes = Votes0 + 1,

            %% Schedule advertisement counter impression.
            case Votes < max_votes() of
                true ->
                    schedule_voting();
                false ->
                    lager:info("Max number of votes reached. Node: ~p", [node()])
            end,

            {noreply, State#state{votes=Votes, identifiers=Identifiers}}
    end;

handle_info(check_convergence, #state{actor=Actor}=State) ->
    MaxEvents = max_votes() * client_number(),
    {ok, {TotalEvents, _}} = lasp:query(?CONVERGENCE_ID),
    % lager:info("Total number of events observed so far ~p of ~p", [TotalEvents, MaxEvents]),

    case TotalEvents == MaxEvents of
        true ->
            lager:info("Convergence reached on node ~p", [node()]),
            %% Update CT instance
            lasp:update(?CONVERGENCE_ID, {snd, {Actor, true}}, Actor),
            lasp_transmission_instrumentation:convergence();
        false ->
            schedule_check_convergence()
    end,

    {noreply, State};

handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
schedule_voting() ->
    %% Add random jitter.
    Jitter = rand_compat:uniform(?VOTING_INTERVAL),
    erlang:send_after(?VOTING_INTERVAL + Jitter, self(), vote).

%% @private
schedule_logging() ->
    erlang:send_after(?LOG_INTERVAL, self(), log).

%% @private
schedule_check_convergence() ->
    erlang:send_after(?CONVERGENCE_INTERVAL, self(), check_convergence).

%% @private
max_votes() ->
    lasp_config:get(simulation_event_number, 10).

%% @private
client_number() ->
    ?NUM_NODES.

%% @private
identifiers() ->
    lists:map(fun(N) -> term_to_binary({counter, N}) end, lists:seq(1, 100)).
