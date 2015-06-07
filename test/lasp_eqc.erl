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

%% @doc EQC test for the Lasp backend.
%%

-module(lasp_eqc).
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

-define(ETS, lasp_ets_eqc).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                io:format(user, Str, Args)
        end, P)).

lasp_ets_sequential_test_() ->
    {timeout, 60,
     ?_assert(eqc:quickcheck(?QC_OUT(?MODULE:prop_sequential())))}.

%% Initialize state.

initial_state() ->
    #state{store=dict:new()}.

%% Declaring new variables.

declare(Type) ->
    {ok, Id} = ?CORE:declare(Type, ?ETS),
    Id.

declare_args(_S) ->
    [elements([lasp_ivar,
               lasp_orset,
               lasp_orswot,
               lasp_orset_gbtree])].

declare_next(#state{store=Store0}=S, Res, [Type]) ->
    Store = dict:store(Res,
                       #variable{type=Type, value=Type:new()}, Store0),
    S#state{store=Store}.

%% Bind variables.

bind(Id, Value) ->
    ?CORE:bind(Id, Value, ?ETS).

bind_pre(S) ->
    has_variables(S).

bind_args(#state{store=Store}) ->
    Variables = dict:fetch_keys(Store),
    ?LET(Variable, elements(Variables),
        begin
            Value = value(Variable, Store),
            [Variable, Value]
        end).

bind_post(S, [Id, NewValue], Res) ->
    case bind_ok(S, [Id, NewValue]) of
        true ->
            ok(Res);
        false ->
            is_error(Res)
    end.

bind_next(#state{store=Store0}=S, _Res, [Id, NewValue]) ->
    Store = case bind_ok(S, [Id, NewValue]) of
        true ->
            case dict:find(Id, Store0) of
                {ok, #variable{type=Type, value=Value}=Variable} ->
                    try
                        Merged = Type:merge(Value, NewValue),
                        case lasp_lattice:is_inflation(Type, Value, Merged) of
                            true ->
                                dict:store(Id,
                                           Variable#variable{value=NewValue},
                                           Store0);
                            false ->
                                Store0
                        end
                    catch
                        _:_ ->
                            Store0
                    end
            end;
        false ->
            Store0
    end,
    S#state{store=Store}.

bind_ok(#state{store=Store0}, [Id, NewValue]) ->
    case dict:find(Id, Store0) of
        {ok, #variable{type=Type, value=Value}} ->
            try
                Merged = Type:merge(Value, NewValue),
                lasp_lattice:is_inflation(Type, Value, Merged)
            catch
                _:_ ->
                    %% In event of a merge which fails, we still return
                    %% ok, but do not change the value.  This might need
                    %% to change.
                    %%
                    true
            end;
        _ ->
            false
    end.

%% Read variables.

read(Id, Threshold) ->
    ?CORE:read(Id, Threshold, ?ETS).

read_pre(S) ->
    has_variables(S).

read_pre(#state{store=Store}, [Id, _Threshold]) ->
    case dict:find(Id, Store) of
        error ->
            %% Not declared.
            false;
        {ok, #variable{value=undefined}} ->
            %% Not bound.
            false;
        {ok, #variable{value=_Value, type=_Type}} ->
            true
    end.

read_args(#state{store=Store}) ->
    Variables = dict:fetch_keys(Store),
    ?LET(Variable, elements(Variables),
                    begin
                        Threshold = threshold(Variable, Store),
                        [Variable, Threshold]
                    end).

read_post(S, [Id, Threshold], Res) ->
    case read_ok(S, [Id, Threshold]) of
        true ->
            ok(Res);
        false ->
            is_error(Res)
    end.

read_ok(_S, [_Id, _Threshold]) ->
    true.

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
        {ok, #variable{type=Type, value=Value0}} ->
            case Type of
                lasp_ivar ->
                    case Value0 of
                        undefined ->
                            undefined;
                        _ ->
                            {strict, undefined}
                    end;
                lasp_orset ->
                    %% @TODO: should toggle the boolean flag on some
                    %% elements, if possible.
                    %%
                    Length = length(Value0),
                    case Length of
                        0 ->
                            [];
                        _ ->
                            Random = random:uniform(Length),
                            lists:sublist(Value0, Random, Length)
                    end;
                lasp_orswot ->
                    %% @TODO: make this way better.
                    {VClock, Entries, Deferred} = Value0,
                    Value = dict:to_list(Entries),
                    Length = length(Value),
                    Threshold = case Length of
                        0 ->
                            dict:new();
                        _ ->
                            Random = random:uniform(Length),
                            T = lists:sublist(Value, Random, Length),
                            dict:from_list(T)
                    end,
                    {VClock, Threshold, Deferred};
                lasp_orset_gbtree ->
                    Value = gb_trees:to_list(Value0),
                    Length = length(Value),
                    case Length of
                        0 ->
                            [];
                        _ ->
                            Random = random:uniform(Length),
                            Threshold = lists:sublist(Value, Random, Length),
                            gb_trees:from_orddict(Threshold)
                    end
            end
    end.

%% Predicates.

num_variables(#state{store=Store}) ->
    Variables = dict:fetch_keys(Store),
    length(Variables).

has_variables(S) ->
    num_variables(S) > 0.

is_set(#variable{type=Type}) ->
    case Type of
        lasp_orset ->
            true;
        _ ->
            false
    end.

set_variables(#state{store=Store}) ->
    dict:fold(fun(Key, Variable, Acc) ->
                case is_set(Variable) of
                    true ->
                        Acc ++ [Key];
                    _ ->
                        Acc
                end
        end, [], Store).

has_two_set_variables(S) ->
    Sets = set_variables(S),
    length(Sets) > 1.

ok({ok, _}) ->
    true;
ok(_) ->
    false.

is_error(error) ->
    true;
is_error(Other) ->
    {expected_error, Other}.

%% Weights.

weight(_, declare) ->
    1;

weight(_, bind) ->
    1;

weight(_, read) ->
    2;

weight(_, _) ->
    3.

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
    {ok, ?ETS} = lasp_ets_storage_backend:start(?ETS),
    ok.

teardown() ->
    ok.

-endif.
-endif.
