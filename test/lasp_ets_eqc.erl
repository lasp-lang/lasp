-module(lasp_ets_eqc).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-include("lasp.hrl").

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state, {store}).
-record(variable, {type, value}).

-define(LATTICES, [lasp_ivar,
                   riak_dt_gset,
                   riak_dt_gcounter,
                   riak_dt_orset,
                   riak_dt_map,
                   riak_dt_orswot]).

-define(ETS, lasp_ets_eqc).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                io:format(user, Str, Args)
        end, P)).

lasp_ets_sequential_test_() ->
    {timeout, 60,
     ?_assert(eqc:quickcheck(?QC_OUT(?MODULE:prop_sequential())))}.

%% Generators.

value(Variable, Store) ->
    case dict:find(Variable, Store) of
        {ok, #variable{type=Type}} ->
            ?LET({Object, Update},
                 {Type:new(), Type:gen_op()},
                  begin
                    case Type:update(Update, undefined, Object) of
                        {ok, X} ->
                            X;
                        _ ->
                            Object
                    end
                  end)
    end.

threshold(Variable, Store) ->
    case dict:find(Variable, Store) of
        {ok, #variable{type=Type}} ->
            ?LET({Object, Update},
                 {Type:new(), Type:gen_op()},
                  begin
                    case Type:update(Update, undefined, Object) of
                        {ok, X} ->
                            X;
                        _ ->
                            Object
                    end
                  end)
    end.

%% Initialize state

initial_state() ->
    #state{store=dict:new()}.

%% Declaring new variables.

declare(Type) ->
    {ok, Id} = lasp_ets:declare(Type, ?ETS),
    Id.

declare_args(_S) ->
    [elements(?LATTICES)].

declare_next(#state{store=Store0}=S, V, [Type]) ->
    Store = dict:store(V, #variable{type=Type}, Store0),
    S#state{store=Store}.

%% Binding variables.
%%
%% If a bind failed, that's only allowed if the variable is already
%% bound or undefined.

bind(Id, Value) ->
    lasp_ets:bind(Id, Value, ?ETS).

bind_pre(S) ->
    has_variables(S).

bind_args(#state{store=Store}) ->
    Variables = dict:fetch_keys(Store),
    ?LET(Variable, elements(Variables),
        begin
            Value = value(Variable, Store),
            [Variable, Value]
        end).

bind_next(#state{store=Store0}=S, _V, [Id, NewValue]) ->
    %% Only update the record, if it's in inflation or has never been
    %% updated before.
    Store = case dict:find(Id, Store0) of
        {ok, #variable{type=Type, value=Value}=Variable} ->
            try
                Merged = case Value of
                    undefined ->
                        NewValue;
                    _ ->
                        Type:merge(Value, NewValue)
                end,
                case lasp_lattice:is_inflation(Type, Value, Merged) of
                    true ->
                        dict:store(Id, Variable#variable{value=Merged}, Store0);
                    false ->
                        Store0
                end
            catch
                _:_ ->
                    Store0
            end
    end,
    S#state{store=Store}.

bind_post(#state{store=Store}, [Id, V], error) ->
    case dict:find(Id, Store) of
        {ok, #variable{type=_Type, value=undefined}} ->
            false;
        {ok, #variable{type=Type, value=Value}} ->
            try
                Merged = Type:merge(V, Value),
                case lasp_lattice:is_inflation(Type, Value, Merged) of
                    true ->
                        false;
                    false ->
                        true
                end
            catch
                _:_ ->
                    true
            end
    end;
bind_post(_, _, _) ->
    true.

%% Read variables.

read(Id, Threshold) ->
    lasp_ets:read(Id, Threshold, ?ETS).

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

read_post(#state{store=Store}, [Id, _Threshold], {ok, {_, V, _}}) ->
    case dict:find(Id, Store) of
        {ok, #variable{value=Value}} ->
            Value == V;
        _ ->
            false
    end.

%% Weights.

weight(_, declare) -> 1;
weight(_, bind) -> 1;
weight(_, read) -> 2;
weight(_, _) -> 3.

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
        {ok, #variable{value=Value, type=Type}} ->
            case lasp_lattice:threshold_met(Type, Value, Threshold) of
                true ->
                    true;
                false ->
                    false
            end
    end.

%% Properties for the state machine.

prop_sequential() ->
    eqc:quickcheck(?SETUP(fun() ->
                                 setup(),
                                 fun teardown/0
                          end,
                         ?FORALL(Cmds, commands(?MODULE),
                                 begin
                                     {H, S, Res} = run_commands(?MODULE, Cmds),
                                     pretty_commands(?MODULE, Cmds, {H, S, Res},
                                        aggregate(command_names(Cmds), Res == ok))
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
