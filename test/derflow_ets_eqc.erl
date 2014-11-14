-module(derflow_ets_eqc).

-include("derflow.hrl").

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state, {store}).
-record(variable, {type, value}).

-define(SECS, 60).
-define(NUM_TESTS, 200).

-define(ETS, derflow_ets_eqc).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

derflow_ets_sequential_test_() ->
    {timeout, 60,
     ?_assert(eqc:quickcheck(
                eqc:numtests(?NUM_TESTS,
                             ?QC_OUT(?MODULE:prop_sequential()))))}.

%% Generators.

value(Variable, Store, DefaultValue) ->
    case dict:find(Variable, Store) of
        {ok, #variable{type=undefined}} ->
            DefaultValue;
        {ok, #variable{type=Type}} ->
            ?LET({Object, Update},
                 {Type:new(), Type:gen_op()},
                  begin
                    {ok, X} = Type:update(Update, undefined, Object),
                    X
                  end)
    end.

threshold(Variable, Store) ->
    case dict:find(Variable, Store) of
        {ok, #variable{type=undefined}} ->
            undefined;
        {ok, #variable{type=Type}} ->
            ?LET({Object, Update},
                 {Type:new(), Type:gen_op()},
                  begin
                    {ok, X} = Type:update(Update, undefined, Object),
                    X
                  end)
    end.

%% Initialize state

initial_state() ->
    #state{store=dict:new()}.

%% Declaring new variables.

declare(Type) ->
    {ok, Id} = derflow_ets:declare(Type, ?ETS),
    Id.

declare_args(_S) ->
    [oneof([elements(?LATTICES), undefined])].

declare_next(#state{store=Store0}=S, V, [Type]) ->
    Store = dict:store(V, #variable{type=Type}, Store0),
    S#state{store=Store}.

%% Binding variables.
%%
%% If a bind failed, that's only allowed if the variable is already
%% bound or undefined.

bind(Id, Value) ->
    derflow_ets:bind(Id, Value, ?ETS).

bind_pre(S) ->
    has_variables(S).

bind_args(#state{store=Store}) ->
    Variables = dict:fetch_keys(Store),
    ?LET({Variable, GeneratedValue}, {elements(Variables), nat()},
                    begin
                        Value = value(Variable, Store, GeneratedValue),
                        [Variable, Value]
                    end).

bind_next(#state{store=Store0}=S, _V, [Id, NewValue]) ->
    %% Only update the record, if it's in inflation or has never been
    %% updated before.
    Store = case dict:find(Id, Store0) of
        {ok, #variable{type=undefined, value=undefined}=Variable} ->
            dict:store(Id, Variable#variable{value=NewValue}, Store0);
        {ok, #variable{type=undefined}} ->
            Store0;
        {ok, #variable{type=Type, value=Value}=Variable} ->
            Merged = case Value of
                undefined ->
                    NewValue;
                _ ->
                    Type:merge(Value, NewValue)
            end,
            case derflow_ets:is_inflation(Type, Value, Merged) of
                true ->
                    dict:store(Id, Variable#variable{value=Merged}, Store0);
                false ->
                    Store0
            end
    end,
    S#state{store=Store}.

bind_post(#state{store=Store}, [Id, V], error) ->
    case dict:find(Id, Store) of
        {ok, #variable{type=_Type, value=undefined}} ->
            false;
        {ok, #variable{type=Type, value=Value}} ->
            case derflow_ets:is_lattice(Type) of
                true ->
                    Merged = Type:merge(V, Value),
                    case derflow_ets:is_inflation(Type, Value, Merged) of
                        true ->
                            false;
                        false ->
                            true
                    end;
                false ->
                    V =/= Value
            end
    end;
bind_post(_, _, _) ->
    true.

%% Read variables.

read(Id, Threshold) ->
    derflow_ets:read(Id, Threshold, ?ETS).

read_pre(S) ->
    has_variables(S).

read_pre(S, [Id, Threshold]) ->
    is_read_valid(S, Id, Threshold).

read_args(#state{store=Store}) ->
    Variables = dict:fetch_keys(Store),
    ?LET(Variable, elements(Variables),
                    begin
                        Threshold = threshold(Variable, Store),
                        [Variable, Threshold]
                    end).

read_post(#state{store=Store}, [Id, _Threshold], {ok, V, _}) ->
    case dict:find(Id, Store) of
        {ok, #variable{value=Value}} ->
            Value == V;
        _ ->
            false
    end.

%% Predicates.

has_variables(#state{store=Store}) ->
    Variables = dict:fetch_keys(Store),
    length(Variables) > 0.

is_read_valid(#state{store=Store}, Id, Threshold) ->
    case dict:find(Id, Store) of
        error ->
            %% Not declared.
            false;
        {ok, #variable{value=undefined}} ->
            %% Not bound.
            false;
        {ok, #variable{type=undefined}} ->
            true;
        {ok, #variable{value=Value, type=Type}} ->
            case derflow_ets:threshold_met(Type, Value, Threshold) of
                true ->
                    true;
                false ->
                    false
            end
    end.

%% Properties for the state machine.

prop_sequential() ->
    eqc:testing_time(?SECS,
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
                     end))).

setup() ->
    ?ETS = ets:new(?ETS, [public, set, named_table,
                          {write_concurrency, true},
                          {read_concurrency, true}]),
    ok.

teardown() ->
    ok.

-endif.
-endif.
