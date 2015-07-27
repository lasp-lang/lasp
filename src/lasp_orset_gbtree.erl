%% -------------------------------------------------------------------
%%
%% lasp_orset: A convergent, replicated, state based observe remove set
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc This module is a fork of the riak_dt_orset, which is modified to
%%      support some additional update functions needed for correctly
%%      composing objects in Riak.

-module(lasp_orset_gbtree).

-behaviour(riak_dt).

%% API
-export([new/0, value/1, update/3, merge/2, equal/2,
         to_binary/1, from_binary/1, value/2, precondition_context/1, stats/1, stat/2]).
-export([update/4, parent_clock/2]).
-export([to_binary/2]).
-export([to_version/2]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% EQC API
-ifdef(EQC).
-export([init_state/0, gen_op/0, update_expected/3, eqc_state_value/1]).
-endif.

-export_type([orset/0, binary_orset/0, orset_op/0]).
-ifdef(namespaced_types).
-opaque orset() :: gb_trees:tree().
-type value() :: gb_trees:tree().
-else.
-opaque orset() :: gb_tree().
-type value() :: gb_tree().
-endif.

-type binary_orset() :: binary(). %% A binary that from_binary/1 will operate on.

-type orset_op() :: {add, member()} | {remove, member()} |
                    {add_all, [member()]} | {remove_all, [member()]} |
                    {update, [orset_op()]} | {add_by_token, term(), member()}.

-type actor() :: riak_dt:actor().
-type member() :: term().

-spec new() -> orset().
new() ->
    gb_trees:empty().

-spec value(orset()) -> [member()].
value(ORSet) ->
    gb_trees_ext:foldl(fun(Elem, Tokens, Acc0) ->
                case length(valid_tokens(Tokens)) > 0 of
                    true ->
                        Acc0 ++ [Elem];
                    false ->
                        Acc0
                end
        end, [], ORSet).

-spec value(any(), orset()) -> [member()] | value().
value({fragment, Elem}, ORSet) ->
    case value({tokens, Elem}, ORSet) of
        [] ->
            gb_trees:empty();
        Tokens ->
            gb_trees:enter(Elem, Tokens, gb_trees:empty())
    end;
value({tokens, Elem}, ORSet) ->
    case gb_trees:is_defined(Elem, ORSet) of
        true ->
            gb_trees:get(Elem, ORSet);
        false ->
            gb_trees:empty()
    end;
value(removed, ORSet) ->
    gb_trees_ext:foldl(fun(Elem, Tokens, Acc0) ->
                case length(removed_tokens(Tokens)) > 0 of
                    true ->
                        Acc0 ++ [Elem];
                    false ->
                        Acc0
                end
        end, [], ORSet);
value(_,ORSet) ->
    value(ORSet).

-spec update(orset_op(), actor(), orset()) ->
    {ok, orset()} | {error, {precondition, {not_present, member()}}}.
update({add_by_token,Token,Elem}, _Actor, ORDict) ->
    add_elem(Elem,Token,ORDict);
update({add,Elem}, Actor, ORDict) ->
    Token = unique(Actor),
    add_elem(Elem,Token,ORDict);
update({add_all,Elems}, Actor, ORDict0) ->
    OD = lists:foldl(fun(Elem,ORDict) ->
                {ok, ORDict1} = update({add,Elem},Actor,ORDict),
                ORDict1
            end, ORDict0, Elems),
    {ok, OD};
update({remove,Elem}, _Actor, ORSet) ->
    remove_elem(Elem, ORSet);
update({remove_all,Elems}, _Actor, ORSet0) ->
    remove_elems(Elems, ORSet0);
update({update, Ops}, Actor, ORSet) ->
    apply_ops(Ops, Actor, ORSet).

-spec update(orset_op(), actor(), orset(), riak_dt:context()) ->
    {ok, orset()} | {error, {precondition ,{not_present, member()}}}.
update(Op, Actor, ORSet, _Ctx) ->
    update(Op, Actor, ORSet).

-spec parent_clock(riak_dt_vclock:vclock(), orset()) -> orset().
parent_clock(_Clock, ORSet) ->
    ORSet.

-spec merge(orset(), orset()) -> orset().
merge(ORSet1, ORSet2) ->
    MergeFun = fun(TokensA, TokensB) ->
            TokenMergeFun = fun(A, B) ->
                    A orelse B
            end,
            gb_trees_ext:merge(TokensA, TokensB, TokenMergeFun)
    end,
    gb_trees_ext:merge(ORSet1, ORSet2, MergeFun).

-spec equal(orset(), orset()) -> boolean().
equal(ORDictA, ORDictB) ->
    gb_trees_ext:equal(ORDictA, ORDictB).

%% @doc the precondition context is a fragment of the CRDT that
%% operations with pre-conditions can be applied too.  In the case of
%% OR-Sets this is the set of adds observed.  The system can then
%% apply a remove to this context and merge it with a replica.
%% Especially useful for hybrid op/state systems where the context of
%% an operation is needed at a replica without sending the entire
%% state to the client.
-spec precondition_context(orset()) -> orset().
precondition_context(ORSet) ->
    gb_trees_ext:foldl(fun(Elem, Tokens, ORSet1) ->
            case minimum_tokens(Tokens) of
                [] ->
                        ORSet1;
                Tokens1 ->
                        gb_trees:enter(Elem, Tokens1, ORSet1)
            end
        end, gb_trees:empty(), ORSet).

-spec stats(orset()) -> [{atom(), number()}].
stats(ORSet) ->
    [ {S, stat(S, ORSet)} || S <- [element_count,
                                   adds_count,
                                   removes_count,
                                   waste_pct] ].

-spec stat(atom(), orset()) -> number() | undefined.
stat(element_count, ORSet) ->
    gb_trees:size(ORSet);
stat(adds_count, ORSet) ->
    gb_trees_ext:foldl(fun(_, Tags, Acc0) ->
                         gb_trees_ext:foldl(fun(_Tag, false, Acc) -> Acc + 1;
                                              (_, _, Acc) -> Acc end,
                                     Acc0, Tags)
                 end, 0, ORSet);
stat(removes_count, ORSet) ->
    gb_trees_ext:foldl(fun(_, Tags, Acc0) ->
                         gb_trees_ext:foldl(fun(_Tag, true, Acc) -> Acc + 1;
                                              (_, _, Acc) -> Acc end,
                                     Acc0, Tags)
                 end, 0, ORSet);
stat(waste_pct, ORSet) ->
    {Tags, Tombs} = gb_trees_ext:foldl(
                      fun(_K, Tags, Acc0) ->
                              gb_trees_ext:foldl(fun(_Tag, false, {As, Rs}) ->
                                                        {As + 1, Rs};
                                                   (_Tag, true, {As, Rs}) ->
                                                        {As, Rs + 1}
                                          end, Acc0, Tags)
                      end, {0,0}, ORSet),
    AllTags = Tags + Tombs,
    case Tags of
        0 -> 0;
        _ ->  round(Tombs / AllTags * 100)
    end;
stat(_, _) -> undefined.

-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(TAG, ?DT_ORSET_TAG).
-define(V1_VERS, 1).

-spec to_binary(orset()) -> binary_orset().
to_binary(ORSet) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(ORSet))/binary>>.

-spec to_binary(Vers :: pos_integer(), orset()) ->
    {ok, binary_orset()} | ?UNSUPPORTED_VERSION.
to_binary(1, Set) ->
    {ok, to_binary(Set)};
to_binary(Vers, _Set) ->
    ?UNSUPPORTED_VERSION(Vers).

-spec from_binary(binary_orset()) ->
    {ok, orset()} | ?UNSUPPORTED_VERSION | ?INVALID_BINARY.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    riak_dt:from_binary(Bin);
from_binary(<<?TAG:8/integer, Vers:8/integer, _Bin/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

-spec to_version(pos_integer(), orset()) -> orset().
to_version(_Version, Set) ->
    Set.


%% Private
add_elem(Elem, Token, ORSet) ->
    case gb_trees:lookup(Elem, ORSet) of
        {value, Tokens} ->
            Tokens1 = gb_trees:insert(Token, false, Tokens),
            {ok, gb_trees:enter(Elem, Tokens1, ORSet)};
        none ->
            Tokens = gb_trees:insert(Token, false, gb_trees:empty()),
            {ok, gb_trees:enter(Elem, Tokens, ORSet)}
    end.

remove_elem(Elem, ORSet) ->
    case gb_trees:lookup(Elem, ORSet) of
        {value, Tokens} ->
            Tokens = gb_trees:get(Elem, ORSet),
            Tokens1 = gb_trees_ext:foldl(fun(Key, _Value, Acc0) ->
                        gb_trees:enter(Key, true, Acc0)
                end, gb_trees:empty(), Tokens),
            {ok, gb_trees:enter(Elem, Tokens1, ORSet)};
        none ->
            {error, {precondition, {not_present, Elem}}}
    end.


remove_elems([], ORSet) ->
    {ok, ORSet};
remove_elems([Elem|Rest], ORSet) ->
    case remove_elem(Elem, ORSet) of
        {ok, ORSet1} ->
            remove_elems(Rest, ORSet1);
        Error ->
            Error
    end.


apply_ops([], _Actor, ORSet) ->
    {ok, ORSet};
apply_ops([Op | Rest], Actor, ORSet) ->
    case update(Op, Actor, ORSet) of
        {ok, ORSet1} ->
            apply_ops(Rest, Actor, ORSet1);
        Error ->
            Error
    end.

unique(_Actor) ->
    crypto:strong_rand_bytes(20).

minimum_tokens(Tokens) ->
    gb_trees_ext:foldl(fun(Key, Removed, Acc) ->
                case Removed of
                    true ->
                        Acc;
                    false ->
                        Acc ++ [{Key, Removed}]
                end
        end, [], Tokens).

valid_tokens(Tokens) ->
    [Token || {Token, false} <- gb_trees:to_list(Tokens)].

removed_tokens(Tokens) ->
    [Token || {Token, true} <- gb_trees:to_list(Tokens)].

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
stat_test() ->
    Set = new(),
    {ok, Set1} = update({add, <<"foo">>}, 1, Set),
    {ok, Set2} = update({add, <<"foo">>}, 2, Set1),
    {ok, Set3} = update({add, <<"bar">>}, 3, Set2),
    {ok, Set4} = update({remove, <<"foo">>}, 1, Set3),
    ?assertEqual([{element_count, 0},
                  {adds_count, 0},
                  {removes_count, 0},
                  {waste_pct, 0}], stats(Set)),
    %% Note this doesn't exclude things that are not all removed!
    ?assertEqual(2, stat(element_count, Set4)),
    ?assertEqual(1, stat(adds_count, Set4)),
    ?assertEqual(2, stat(removes_count, Set4)),
    ?assertEqual(67, stat(waste_pct, Set4)).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

%% EQC generator
gen_op() ->
    oneof([gen_updates(), gen_update()]).

gen_updates() ->
     {update, non_empty(list(gen_update()))}.

gen_update() ->
    oneof([{add, int()}, {remove, int()},
           {add_all, list(int())},
           {remove_all, list(int())}]).

init_state() ->
    {0, dict:new()}.

do_updates(_ID, [], _OldState, NewState) ->
    NewState;
do_updates(ID, [{_Action, []} | Rest], OldState, NewState) ->
    do_updates(ID, Rest, OldState, NewState);
do_updates(ID, [Update | Rest], OldState, NewState) ->
    case {Update, update_expected(ID, Update, NewState)} of
        {{Op, Arg}, NewState} when Op == remove;
                                   Op == remove_all ->
            %% precondition fail, or idempotent remove?
            {_Cnt, Dict} = NewState,
            {_A, R} = dict:fetch(ID, Dict),
            Removed = [ E || {E, _X} <- sets:to_list(R)],
            case member(Arg, Removed) of
                true ->
                    do_updates(ID, Rest, OldState, NewState);
                false ->
                    OldState
            end;
        {_, NewNewState} ->
            do_updates(ID, Rest, OldState, NewNewState)
    end.

member(_Arg, []) ->
    false;
member(Arg, L) when is_list(Arg) ->
    sets:is_subset(sets:from_list(Arg), sets:from_list(L));
member(Arg, L) ->
    lists:member(Arg, L).

update_expected(ID, {update, Updates}, State) ->
    do_updates(ID, Updates, State, State);
update_expected(ID, {add, Elem}, {Cnt0, Dict}) ->
    Cnt = Cnt0+1,
    ToAdd = {Elem, Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)};
update_expected(ID, {remove, Elem}, {Cnt, Dict}) ->
    {A, R} = dict:fetch(ID, Dict),
    ToRem = [ {E, X} || {E, X} <- sets:to_list(A), E == Elem],
    {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, Dict)};
update_expected(ID, {merge, SourceID}, {Cnt, Dict}) ->
    {FA, FR} = dict:fetch(ID, Dict),
    {TA, TR} = dict:fetch(SourceID, Dict),
    MA = sets:union(FA, TA),
    MR = sets:union(FR, TR),
    {Cnt, dict:store(ID, {MA, MR}, Dict)};
update_expected(ID, create, {Cnt, Dict}) ->
    {Cnt, dict:store(ID, {sets:new(), sets:new()}, Dict)};
update_expected(ID, {add_all, Elems}, State) ->
    lists:foldl(fun(Elem, S) ->
                       update_expected(ID, {add, Elem}, S) end,
               State,
               Elems);
update_expected(ID, {remove_all, Elems}, {_Cnt, Dict}=State) ->
    %% Only if _all_ elements are in the set do we remove any elems
    {A, R} = dict:fetch(ID, Dict),
    Members = [E ||  {E, _X} <- sets:to_list(sets:union(A,R))],
    case sets:is_subset(sets:from_list(Elems), sets:from_list(Members)) of
        true ->
            lists:foldl(fun(Elem, S) ->
                                update_expected(ID, {remove, Elem}, S) end,
                        State,
                        Elems);
        false ->
            State
    end.

eqc_state_value({_Cnt, Dict}) ->
    {A, R} = dict:fold(fun(_K, {Add, Rem}, {AAcc, RAcc}) ->
                               {sets:union(Add, AAcc), sets:union(Rem, RAcc)} end,
                       {sets:new(), sets:new()},
                       Dict),
    Remaining = sets:subtract(A, R),
    Values = [ Elem || {Elem, _X} <- sets:to_list(Remaining)],
    lists:usort(Values).

-endif.

-endif.
