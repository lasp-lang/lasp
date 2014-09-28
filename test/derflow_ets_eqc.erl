-module(derflow_ets_eqc).

-include("derflow.hrl").

-ifdef(TEST).
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

-record(state, {ets, types, store}).
-record(variable, {type, value}).

-define(NUM_TESTS, 200).

-define(ETS, derflow_ets_eqc).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

derflow_ets_test_() ->
    {timeout, 60,
     ?_assert(eqc:quickcheck(
                eqc:numtests(?NUM_TESTS,
                             ?QC_OUT(?MODULE:prop_sequential())))),
    ?_assert(eqc:quickcheck(
               eqc:numtests(?NUM_TESTS,
                            ?QC_OUT(?MODULE:prop_parallel()))))
    }.

%% Generators
declare(Type, Ets) ->
    {ok, Id} = derflow_ets:declare(Type, Ets),
    Id.

bind(Id, Value, Ets) ->
    derflow_ets:bind(Id, Value, Ets).

read(Id, Threshold, Ets) ->
    derflow_ets:read(Id, Threshold, Ets).

%% Initialize state
initial_state() ->
    #state{ets=?ETS, types=?LATTICES, store=dict:new()}.

%% Generate commands
command(#state{ets=Ets, types=Types, store=Store}) ->
    Variables = dict:fetch_keys(Store),
    oneof(
        [{call, ?MODULE, declare,
          [oneof([elements(Types), undefined]), Ets]}] ++
        [{call, ?MODULE, read,
          [elements(Variables), undefined, Ets]} || length(Variables) > 0] ++
        [?LET({Variable, GeneratedValue}, {elements(Variables), nat()},
             begin
                    Value = case dict:find(Variable, Store) of
                        {ok, #variable{type=undefined}} ->
                            GeneratedValue;
                        {ok, #variable{type=Type}} ->
                            ?LET({Object, Update},
                                 {Type:new(), Type:gen_op()},
                                  begin
                                    {ok, X} = Type:update(Update,
                                                          undefined,
                                                          Object),
                                    X
                                  end)
                    end,
                    {call, ?MODULE, bind, [Variable, Value, Ets]}
                end) || length(Variables) > 0]).

next_state(#state{store=Store0}=S, V, {call, ?MODULE, declare, [Type, _]}) ->
    Store = dict:store(V, #variable{type=Type}, Store0),
    S#state{store=Store};
next_state(#state{store=Store0}=S, V, {call, ?MODULE, declare, [_]}) ->
    Store = dict:store(V, #variable{}, Store0),
    S#state{store=Store};
next_state(S, _V, {call, ?MODULE, bind, _}) ->
    S;

%% Next state transformation
next_state(S,_V,{call,_,_,_}) ->
    S.

precondition(#state{store=Store},
             {call, ?MODULE, read, [Id, _Threshold, _Store]}) ->
    case dict:find(Id, Store) of
        error ->
            %% Not declared.
            false;
        {ok, #variable{value=undefined}} ->
            %% Not bound.
            false;
        {ok, _} ->
            true
    end;

%% Precondition, checked before command is added to the command sequence
precondition(_S,{call,_,_,_}) ->
    true.

%% If the bind succeeded, the value must be an inflation.
postcondition(#state{store=Store},
              {call, ?MODULE, bind, [Id, Value, _]}, {ok, _}) ->
    case dict:find(Id, Store) of
        {ok, #variable{type=Type, value=V}} ->
            case Type of
                undefined ->
                    V == undefined;
                _ ->
                    derflow_ets:is_inflation(Type, V, Value)
            end;
        _ ->
            false
    end;

%% If a bind failed, that's only allowed if the variable is already
%% bound or undefined.
%%
postcondition(#state{store=Store},
              {call, ?MODULE, bind, [Id, Value, _]}, error) ->
    case dict:find(Id, Store) of
        error ->
            %% Not declared.
            true;
        {ok, #variable{value=Value}} ->
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

%% Sequential property for the state machine.
prop_sequential() ->
    ?SETUP(fun() ->
                setup(),
                fun teardown/0
           end,
        ?FORALL(Cmds, commands(?MODULE),
                begin
                    {H, S, Res} = run_commands(?MODULE, Cmds),
                    ?WHENFAIL(
                        io:format("History: ~p~nState: ~p~nRes: ~p~n", [H, S, Res]),
                        Res == ok)
                end)).

prop_parallel() ->
    ?SETUP(fun() ->
                setup(),
                fun teardown/0
           end,
        ?FORALL(Cmds, parallel_commands(?MODULE),
                begin
                    {H, S, Res} = run_parallel_commands(?MODULE, Cmds),
                    ?WHENFAIL(
                        io:format("History: ~p~nState: ~p~nRes: ~p~n", [H, S, Res]),
                        Res == ok)
                end)).

setup() ->
    ?ETS = ets:new(?ETS, [public, set, named_table]),
    ok.

teardown() ->
    ok.

-endif.
-endif.
