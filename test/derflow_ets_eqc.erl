-module(derflow_ets_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-behavior(eqc_statem).

-compile(export_all).

-export([command/1,
         initial_state/0,
         next_state/3,
         precondition/2,
         postcondition/3]).

-record(state, {ets, store}).

-define(NUM_TESTS, 200).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

derflow_ets_test_() ->
    {timeout, 60,
     ?_assert(eqc:quickcheck(
                eqc:numtests(?NUM_TESTS, ?QC_OUT(?MODULE:prop_statem()))))}.

%% Generators
declare(Store) ->
    {ok, Id} = derflow_ets:declare(Store),
    Id.

bind(Id, Value, Store) ->
    derflow_ets:bind(Id, Value, Store).

read(Id, Store) ->
    derflow_ets:read(Id, Store).

value() ->
    int().

%% Initialize state
initial_state() ->
    Ets = ets:new(derflow_ets_eqc, [set]),
    #state{ets=Ets, store=dict:new()}.

%% Generate commands
command(#state{ets=Ets, store=Store}) ->
    Variables = dict:fetch_keys(Store),
    oneof([{call, ?MODULE, declare, [Ets]}] ++
          [{call, ?MODULE, bind, [elements(Variables), value(), Ets]}
           || length(Variables) > 0] ++
          [{call, ?MODULE, read, [elements(Variables), Ets]}
           || length(Variables) > 0]).

%% Next state transformation
next_state(#state{store=Store0}=S, V, {call, ?MODULE, declare, _}) ->
    Store = dict:store(V, undefined, Store0),
    S#state{store=Store};

next_state(S,_V,{call,_,_,_}) ->
    S.

precondition(#state{store=Store}, {call, ?MODULE, read, [Id, _Store]}) ->
    case dict:find(Id, Store) of
        error ->
            %% Not declared.
            false;
        {ok, undefined} ->
            %% Not bound.
            false;
        {ok, _} ->
            true
    end;

%% Precondition, checked before command is added to the command sequence
precondition(_S,{call,_,_,_}) ->
    true.

%% If a bind failed, that's only allowed if the variable is already
%% bound or undefined.
%%
postcondition(#state{store=Store},
              {call, ?MODULE, bind, [Id, Value, _]}, error) ->
    case dict:find(Id, Store) of
        error ->
            %% Not declared.
            true;
        {ok, Value} ->
            %% Already bound to same value.
            false;
        {ok, _} ->
            %% Bound, to different value.
            true;
        _ ->
            false
    end;

%% Postcondition, checked after command has been evaluated
%% OBS: S is the state before next_state(S,_,<command>)
postcondition(_S,{call,_,_,_},_Res) ->
    true.

%% Property for the state machine.
prop_statem() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H, S, Res} = run_commands(?MODULE, Cmds),
                ?WHENFAIL(
                    io:format("History: ~p~nState: ~p~nRes: ~p~n", [H, S, Res]),
                    Res == ok)
            end).

-endif.
