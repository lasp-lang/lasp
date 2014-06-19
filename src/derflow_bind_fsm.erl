-module(derflow_bind_fsm).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-behaviour(gen_fsm).

-include("derflow.hrl").

-export([start_link/1]).

-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-export([execute/2,
         await_responses/2]).

-record(state, {from :: pid(),
                key :: term(),
                id :: term(),
                value :: term(),
                results = []}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link([From, Id, Value]) ->
    gen_fsm:start_link(?MODULE, [From, Id, Value], []).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([From, Id, Value]) ->
    {ok, execute, #state{id=Id, value=Value, from=From}, 0}.

%% @private
execute(timeout, State=#state{id=Id, value=Value}) ->
    derflow_vnode:bind(Id, Value),
    {next_state, await_responses, State#state{key=Id}}.

await_responses(Result={ok, Next},
                State=#state{from=Pid, results=Results0}) ->
    Results = [Result | Results0],
    case length(Results) of
        ?W ->
            Pid ! {ok, Next},
            {stop, normal, State};
        _ ->
            {next_state, await_responses, State#state{results=Results}}
    end.

%% @private
handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

%% @private
handle_sync_event(_Event, _From, _StateName, State) ->
    {stop, badmsg, State}.

%% @private
handle_info(request_timeout, StateName, State) ->
    ?MODULE:StateName(request_timeout, State);
handle_info(_Info, _StateName, State) ->
    {stop, badmsg, State}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
