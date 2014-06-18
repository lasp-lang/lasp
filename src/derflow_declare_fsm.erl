%% @doc coordination of Derflow declare requests

-module(derflow_declare_fsm).

-behaviour(gen_fsm).

-include("derflow.hrl").

-export([start_link/1]).
%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
%% State-event callbacks
-export([execute/2, await_responses/2]).

-record(state, {from :: pid(),
                key,
                results=[] %% success responses
               }).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(From) ->
    gen_fsm:start_link(?MODULE, From, []).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init(From) ->
    {ok, execute, #state{from=From}, 0}.

%% @private
execute(timeout, StateData) ->
    Id = druuid:v4(),
    derflow_vnode:declare(Id),
    {next_state, await_responses, StateData#state{key=Id}}.

await_responses({ok, Id}, StateData=#state{from=Pid, results=Results0}) ->
    Results = [Id | Results0],
    case length(Results) of
        ?W ->
            Pid ! {ok, Id},
            {stop, normal, StateData};
        _ ->
            {next_state, await_responses, StateData#state{results=Results}}
    end.

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

%% @private
handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
