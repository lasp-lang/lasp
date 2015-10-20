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

-module(lasp_lattice).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-include("lasp.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Exported utility functions.
-export([threshold_met/3,
         is_inflation/3,
         is_lattice_inflation/3,
         is_strict_inflation/3,
         is_lattice_strict_inflation/3,
         causal_product/3,
         causal_remove/2,
         causal_union/3]).

%% @doc Determine if a threshold is met.
%%
%%      Given a particular type, `Threshold' and `Value', determine if
%%      a given threshold is met by a particular lattice.
%%
%%      When the threshold is specified as `{strict, value()', ensure
%%      that the lattice is a strict inflation over the supplied
%%      threshold.
%%
%%      `Value' and `Threshold' both should be an instance of one of the
%%      `type()'s and not a pure value.
%%
threshold_met({Type, _}, Value, Current) ->
    threshold_met(Type, Value, Current);
threshold_met(lasp_ivar, undefined, {strict, undefined}) ->
    false;
threshold_met(lasp_ivar, undefined, undefined) ->
    true;
threshold_met(lasp_ivar, Value, undefined) when Value =/= undefined ->
    true;
threshold_met(lasp_ivar, _Value, {strict, undefined}) ->
    true;
threshold_met(lasp_ivar, Value, Threshold) when Value =:= Threshold ->
    true;
threshold_met(lasp_ivar, Value, Threshold) when Value =/= Threshold ->
    false;

threshold_met(lasp_top_k_var, Value, Threshold) ->
    is_inflation(lasp_top_k_var, Threshold, Value);

threshold_met(lasp_orset_gbtree, Value, {strict, Threshold}) ->
    is_strict_inflation(lasp_orset_gbtree, Threshold, Value);
threshold_met(lasp_orset_gbtree, Value, Threshold) ->
    is_inflation(lasp_orset_gbtree, Threshold, Value);

threshold_met(lasp_orset, Value, {strict, Threshold}) ->
    is_strict_inflation(lasp_orset, Threshold, Value);
threshold_met(lasp_orset, Value, Threshold) ->
    is_inflation(lasp_orset, Threshold, Value);

threshold_met(lasp_orswot, Value, {strict, Threshold}) ->
    is_strict_inflation(lasp_orswot, Threshold, Value);
threshold_met(lasp_orswot, Value, Threshold) ->
    is_inflation(lasp_orswot, Threshold, Value);

threshold_met(riak_dt_orswot, Value, {strict, Threshold}) ->
    is_strict_inflation(riak_dt_orswot, Threshold, Value);
threshold_met(riak_dt_orswot, Value, Threshold) ->
    is_inflation(riak_dt_orswot, Threshold, Value);

threshold_met(riak_dt_map, Value, {strict, Threshold}) ->
    is_strict_inflation(riak_dt_map, Threshold, Value);
threshold_met(riak_dt_map, Value, Threshold) ->
    is_inflation(riak_dt_map, Threshold, Value);

threshold_met(riak_dt_gcounter, Value, {strict, Threshold}) ->
    Threshold < riak_dt_gcounter:value(Value);
threshold_met(riak_dt_gcounter, Value, Threshold) ->
    Threshold =< riak_dt_gcounter:value(Value).

%% @doc Determine if a change is an inflation or not.
%%
%%      Given a particular type and two instances of that type,
%%      determine if `Current' is an inflation of `Previous'.
%%
is_inflation({Type, _}, Previous, Current) ->
    is_inflation(Type, Previous, Current);
is_inflation(Type, Previous, Current) ->
    is_lattice_inflation(Type, Previous, Current).

%% @doc Determine if a change is a strict inflation or not.
%%
%%      Given a particular type and two instances of that type,
%%      determine if `Current' is a strict inflation of `Previous'.
%%
is_strict_inflation({Type, _}, Previous, Current) ->
    is_strict_inflation(Type, Previous, Current);
is_strict_inflation(Type, Previous, Current) ->
    is_lattice_strict_inflation(Type, Previous, Current).

%% @doc Determine if a change for a given type is an inflation or not.
%%
%%      Given a particular type and two instances of that type,
%%      determine if `Current' is an inflation of `Previous'.
%%
%%      Inflation of an I-Var is moving from the undefined state to a
%%      defined state.
%%
%%      Inflation of a G-Set ensures that the `Previous' sets elements
%%      are contained by the `Current' set.
%%
%%      Inflation of an OR-Set ensures that the `Previous' sets elements
%%      all exist with the same unique idenfiers as `Current'.
%%
%%      Inflation of a G-Counter ensure that all actors in the
%%      `Previous' state are covered in the `Current' state with at
%%      least the same counts.
%%
is_lattice_inflation(lasp_ivar, undefined, undefined) ->
    true;
is_lattice_inflation(lasp_ivar, undefined, _Current) ->
    true;
is_lattice_inflation(lasp_ivar, Previous, Current)
        when Previous =/= Current ->
    false;
is_lattice_inflation(lasp_ivar, Previous, Current)
        when Previous =:= Current ->
    true;

is_lattice_inflation(lasp_orswot, {Previous, _, _}, {Current, _, _}) ->
    riak_dt_vclock:descends(Current, Previous);

is_lattice_inflation(lasp_top_k_var, {K, Previous}, {K, Current}) ->
    orddict:fold(fun(Key, Value, Acc) ->
                        case orddict:find(Key, Current) of
                            error ->
                                Acc andalso false;
                            {ok, Value1} ->
                                Acc andalso (Value1 >= Value)
                        end
                end, true, Previous);

is_lattice_inflation(lasp_orset_gbtree, Previous, Current) ->
    gb_trees_ext:foldl(fun(Element, Ids, Acc) ->
                        case gb_trees:lookup(Element, Current) of
                            none ->
                                Acc andalso false;
                            {value, Ids1} ->
                                Acc andalso ids_inflated(lasp_orset_gbtree, Ids, Ids1)
                        end
                end, true, Previous);

is_lattice_inflation(lasp_orset, Previous, Current) ->
    lists:foldl(fun({Element, Ids}, Acc) ->
                        case lists:keyfind(Element, 1, Current) of
                            false ->
                                Acc andalso false;
                            {_, Ids1} ->
                                Acc andalso ids_inflated(lasp_orset, Ids, Ids1)
                        end
                end, true, Previous);

is_lattice_inflation(riak_dt_orswot, {Previous, _, _}, {Current, _, _}) ->
    riak_dt_vclock:descends(Current, Previous);

is_lattice_inflation(riak_dt_map, {Previous, _, _}, {Current, _, _}) ->
    riak_dt_vclock:descends(Current, Previous);

is_lattice_inflation(riak_dt_gcounter, Previous, Current) ->
    PreviousList = lists:sort(orddict:to_list(Previous)),
    CurrentList = lists:sort(orddict:to_list(Current)),
    lists:foldl(fun({Actor, Count}, Acc) ->
            case lists:keyfind(Actor, 1, CurrentList) of
                false ->
                    Acc andalso false;
                {_Actor1, Count1} ->
                    Acc andalso (Count =< Count1)
            end
            end, true, PreviousList).

%% @doc Determine if a change for a given type is a strict inflation or
%%      not.
%%
%%      Given a particular type and two instances of that type,
%%      determine if `Current' is a strict inflation of `Previous'.
%%
%%      Strict inflation of an I-Var is verifying it's an inflation,
%%      along with ensuring that it's moving to a value from
%%      `undefined'.
%%
%%      Strict inflation for a G-Set is verifying the change is an
%%      inflation, along with ensuring the change introduces an
%%      additional element.
%%
%%      Strict inflation for an OR-Set is verifying the change is an
%%      inflation, along with ensuring the change does at least one of
%%      two things: adds an additional identifer, or changes one
%%      identifer's deleted marker from false -> true.
%%
%%      Strict inflation for a G-Counter is verifying the change is an
%%      inflation, along with ensuring that at least one of the counts
%%      has increased, or a new actor has been introduced.
%%
is_lattice_strict_inflation(lasp_ivar, undefined, undefined) ->
    false;
is_lattice_strict_inflation(lasp_ivar, undefined, Current)
        when Current =/= undefined ->
    true;
is_lattice_strict_inflation(lasp_ivar, _Previous, _Current) ->
    false;

is_lattice_strict_inflation(lasp_orswot, {Previous, _, _}, {Current, _, _}) ->
    riak_dt_vclock:dominates(Current, Previous);

is_lattice_strict_inflation(lasp_orset_gbtree, Previous, Current) ->
    %% We already know that `Previous' is fully covered in `Current', so
    %% we just need to look for a change -- removal (false -> true), or
    %% addition of a new element.
    IsLatticeInflation = is_lattice_inflation(lasp_orset_gbtree,
                                              Previous,
                                              Current),
    DeletedElements = gb_trees_ext:foldl(fun(Element, Ids, Acc) ->
                    case gb_trees:lookup(Element, Current) of
                        none ->
                            Acc;
                        {value, Ids1} ->
                            Acc orelse Ids =/= Ids1
                    end
                    end, false, Previous),
    NewElements = gb_trees:size(Previous) < gb_trees:size(Current),
    IsLatticeInflation andalso (DeletedElements orelse NewElements);

is_lattice_strict_inflation(lasp_orset, [], Current) when Current =/= [] ->
    true;
is_lattice_strict_inflation(lasp_orset, Previous, Current) ->
    %% We already know that `Previous' is fully covered in `Current', so
    %% we just need to look for a change -- removal (false -> true), or
    %% addition of a new element.
    IsLatticeInflation = is_lattice_inflation(lasp_orset,
                                              Previous,
                                              Current),
    DeletedElements = lists:foldl(fun({Element, Ids}, Acc) ->
                    case lists:keyfind(Element, 1, Current) of
                        false ->
                            Acc;
                        {_, Ids1} ->
                            Acc orelse Ids =/= Ids1
                    end
                    end, false, Previous),
    NewElements = length(Previous) < length(Current),
    IsLatticeInflation andalso (DeletedElements orelse NewElements);

is_lattice_strict_inflation(riak_dt_orswot, {PV, PE, _}=Previous, {CV, CE, _}=Current) ->
    IsLatticeInflation = is_lattice_inflation(riak_dt_orswot, Previous, Current),
    DeletedElements = dict:size(PE) > dict:size(CE),
    DominatedClock = riak_dt_vclock:dominates(CV, PV),
    EqualClocks = riak_dt_vclock:equal(CV, PV),
    IsLatticeInflation andalso (
        (EqualClocks andalso DeletedElements) orelse
        DominatedClock);

is_lattice_strict_inflation(riak_dt_map, {PV, PE, _}=Previous, {CV, CE, _}=Current) ->
    IsLatticeInflation = is_lattice_inflation(riak_dt_orswot, Previous, Current),
    DeletedElements = dict:size(PE) > dict:size(CE),
    DominatedClock = riak_dt_vclock:dominates(CV, PV),
    EqualClocks = riak_dt_vclock:equal(CV, PV),
    IsLatticeInflation andalso (
        (EqualClocks andalso DeletedElements) orelse
        DominatedClock);

is_lattice_strict_inflation(riak_dt_gcounter, Previous, Current) ->
    %% Massive shortcut here -- get the value and see if it's different.
    riak_dt_gcounter:value(Previous) < riak_dt_gcounter:value(Current).

ids_inflated(lasp_orset, Previous, Current) ->
    lists:foldl(fun({Id, _}, Acc) ->
                        case lists:keyfind(Id, 1, Current) of
                            false ->
                                Acc andalso false;
                            _ ->
                                Acc andalso true
                        end
                end, true, Previous);

ids_inflated(lasp_orset_gbtree, Previous, Current) ->
    gb_trees_ext:foldl(fun(Id, _, Acc) ->
                        case gb_trees:lookup(Id, Current) of
                            none ->
                                Acc andalso false;
                            {value, _} ->
                                Acc andalso true
                        end
                end, true, Previous).

%% @doc Compute a cartesian product from causal metadata stored in the
%%      orset.
%%
%%      Computes product of `Xs' and `Ys' and map deleted through using
%%      an or operation.
%%
causal_product(lasp_orset, Xs, Ys) ->
    lists:foldl(fun({X, XDeleted}, XAcc) ->
                lists:foldl(fun({Y, YDeleted}, YAcc) ->
                            [{[X, Y], XDeleted orelse YDeleted}] ++ YAcc
                    end, [], Ys) ++ XAcc
        end, [], Xs);
causal_product(lasp_orset_gbtree, Xs, Ys) ->
    gb_trees_ext:foldl(fun(X, XDeleted, XAcc) ->
                gb_trees_ext:foldl(fun(Y, YDeleted, YAcc) ->
                            gb_trees:enter([X, Y], XDeleted orelse YDeleted, YAcc)
                    end, XAcc, Ys)
        end, gb_trees:empty(), Xs).

%% @doc Compute the union of causal metadata.
causal_union(lasp_orset, Xs, Ys) ->
    Xs ++ Ys;
causal_union(lasp_orset_gbtree, Xs, Ys) ->
    MergeFun = fun(X, Y) ->
            X orelse Y
    end,
    gb_trees_ext:merge(Xs, Ys, MergeFun).

%% @doc Given the metadata for a given value, force that the object
%%      appears removed by marking all of the metadata as removed.
causal_remove(lasp_orset, Metadata) ->
    orddict:fold(fun(Key, _Value, Acc) ->
                orddict:store(Key, true, Acc)
        end, orddict:new(), Metadata);
causal_remove(lasp_orset_gbtree, Metadata) ->
    gb_trees_ext:foldl(fun(Key, _Value, Acc) ->
                gb_trees:enter(Key, true, Acc)
        end, gb_trees:empty(), Metadata).

-ifdef(TEST).

%% lasp_ivar test.

lasp_ivar_inflation_test() ->
    A1 = lasp_ivar:new(),
    B1 = lasp_ivar:new(),

    {ok, A2} = lasp_ivar:update({set, 1}, a, A1),
    {ok, B2} = lasp_ivar:update({set, 2}, b, B1),

    %% A1 and B1 are equivalent.
    ?assertEqual(true, is_lattice_inflation(lasp_ivar, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_inflation(lasp_ivar, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_inflation(lasp_ivar, A2, B2)).

lasp_ivar_strict_inflation_test() ->
    A1 = lasp_ivar:new(),
    B1 = lasp_ivar:new(),

    {ok, A2} = lasp_ivar:update({set, 1}, a, A1),
    {ok, B2} = lasp_ivar:update({set, 2}, b, B1),

    %% A1 and B1 are equivalent.
    ?assertEqual(false, is_lattice_strict_inflation(lasp_ivar, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_strict_inflation(lasp_ivar, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_strict_inflation(lasp_ivar, A2, B2)).

%% riak_dt_gcounter tests.

riak_dt_gcounter_inflation_test() ->
    A1 = riak_dt_gcounter:new(),
    B1 = riak_dt_gcounter:new(),

    {ok, A2} = riak_dt_gcounter:update(increment, a, A1),
    {ok, B2} = riak_dt_gcounter:update(increment, b, B1),

    %% A1 and B1 are equivalent.
    ?assertEqual(true,
                 is_lattice_inflation(riak_dt_gcounter, A1, B1)),

    %% A2 is after B1.
    ?assertEqual(false,
                 is_lattice_inflation(riak_dt_gcounter, A2, B1)),

    %% A2 comes after both A1 and B1 in the order.
    ?assertEqual(true,
                 is_lattice_inflation(riak_dt_gcounter, A1, A2)),
    ?assertEqual(true,
                 is_lattice_inflation(riak_dt_gcounter, B1, A2)),

    %% Concurrent requests.
    ?assertEqual(false,
                 is_lattice_inflation(riak_dt_gcounter, A2, B2)).

riak_dt_gcounter_strict_inflation_test() ->
    A1 = riak_dt_gcounter:new(),
    B1 = riak_dt_gcounter:new(),

    {ok, A2} = riak_dt_gcounter:update(increment, a, A1),
    {ok, A3} = riak_dt_gcounter:update(increment, a, A2),
    {ok, B2} = riak_dt_gcounter:update(increment, b, B1),

    %% A1 and B1 are equivalent.
    ?assertEqual(false,
                 is_lattice_strict_inflation(riak_dt_gcounter, A1, B1)),

    %% A2 is after B1.
    ?assertEqual(false,
                 is_lattice_strict_inflation(riak_dt_gcounter, A2, B1)),

    %% A2 comes after both A1 and B1 in the order.
    ?assertEqual(true,
                 is_lattice_strict_inflation(riak_dt_gcounter, A1, A2)),
    ?assertEqual(true,
                 is_lattice_strict_inflation(riak_dt_gcounter, B1, A2)),

    %% Concurrent requests.
    ?assertEqual(false,
                 is_lattice_strict_inflation(riak_dt_gcounter, A2, B2)),

    %% A2 is equivalent to A2.
    ?assertEqual(false,
                 is_lattice_strict_inflation(riak_dt_gcounter, A2, A2)),

    %% A3 is after A2.
    ?assertEqual(true,
                 is_lattice_strict_inflation(riak_dt_gcounter, A2, A3)).

%% riak_dt_orswot tests.

riak_dt_orswot_inflation_test() ->
    A1 = riak_dt_orswot:new(),
    B1 = riak_dt_orswot:new(),

    {ok, A2} = riak_dt_orswot:update({add, 1}, a, A1),
    {ok, B2} = riak_dt_orswot:update({add, 2}, b, B1),
    {ok, A3} = riak_dt_orswot:update({remove, 1}, a, A2),

    %% A1 and B1 are equivalent.
    ?assertEqual(true, is_lattice_inflation(riak_dt_orswot, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_inflation(riak_dt_orswot, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_inflation(riak_dt_orswot, A2, B2)),

    %% A3 after A2.
    ?assertEqual(true, is_lattice_inflation(riak_dt_orswot, A2, A3)).

riak_dt_orswot_strict_inflation_test() ->
    A1 = riak_dt_orswot:new(),
    B1 = riak_dt_orswot:new(),

    {ok, A2} = riak_dt_orswot:update({add, 1}, a, A1),
    {ok, B2} = riak_dt_orswot:update({add, 2}, b, B1),
    {ok, A3} = riak_dt_orswot:update({remove, 1}, a, A2),

    %% A1 and B1 are equivalent.
    ?assertEqual(false, is_lattice_strict_inflation(riak_dt_orswot, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_strict_inflation(riak_dt_orswot, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_strict_inflation(riak_dt_orswot, A2, B2)),

    %% A3 after A2.
    ?assertEqual(true, is_lattice_strict_inflation(riak_dt_orswot, A2, A3)).

%% riak_dt_map tests.

riak_dt_map_inflation_test() ->
    A1 = riak_dt_map:new(),
    B1 = riak_dt_map:new(),

    {ok, A2} = riak_dt_map:update({update, [{update, {'X', riak_dt_orset}, {add, 1}}]}, a, A1),
    {ok, B2} = riak_dt_map:update({update, [{update, {'X', riak_dt_orset}, {add, 2}}]}, b, B1),
    {ok, A3} = riak_dt_map:update({update, [{update, {'X', riak_dt_orset}, {remove, 1}}]}, a, A2),

    %% A1 and B1 are equivalent.
    ?assertEqual(true, is_lattice_inflation(riak_dt_map, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_inflation(riak_dt_map, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_inflation(riak_dt_map, A2, B2)),

    %% A3 after A2.
    ?assertEqual(true, is_lattice_inflation(riak_dt_map, A2, A3)).

riak_dt_map_strict_inflation_test() ->
    A1 = riak_dt_map:new(),
    B1 = riak_dt_map:new(),

    {ok, A2} = riak_dt_map:update({update, [{update, {'X', riak_dt_orset}, {add, 1}}]}, a, A1),
    {ok, B2} = riak_dt_map:update({update, [{update, {'X', riak_dt_orset}, {add, 2}}]}, b, B1),
    {ok, A3} = riak_dt_map:update({update, [{update, {'X', riak_dt_orset}, {remove, 1}}]}, a, A2),

    %% A1 and B1 are equivalent.
    ?assertEqual(false, is_lattice_strict_inflation(riak_dt_map, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_strict_inflation(riak_dt_map, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_strict_inflation(riak_dt_map, A2, B2)),

    %% A3 after A2.
    ?assertEqual(true, is_lattice_strict_inflation(riak_dt_map, A2, A3)).

%% lasp_orset tests.

lasp_orset_inflation_test() ->
    A1 = lasp_orset:new(),
    B1 = lasp_orset:new(),

    {ok, A2} = lasp_orset:update({add, 1}, a, A1),
    {ok, B2} = lasp_orset:update({add, 2}, b, B1),
    {ok, A3} = lasp_orset:update({remove, 1}, a, A2),

    %% A1 and B1 are equivalent.
    ?assertEqual(true, is_lattice_inflation(lasp_orset, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_inflation(lasp_orset, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_inflation(lasp_orset, A2, B2)),

    %% A3 after A2.
    ?assertEqual(true, is_lattice_inflation(lasp_orset, A2, A3)).

lasp_orset_strict_inflation_test() ->
    A1 = lasp_orset:new(),
    B1 = lasp_orset:new(),

    {ok, A2} = lasp_orset:update({add, 1}, a, A1),
    {ok, B2} = lasp_orset:update({add, 2}, b, B1),
    {ok, A3} = lasp_orset:update({remove, 1}, a, A2),

    %% A1 and B1 are equivalent.
    ?assertEqual(false, is_lattice_strict_inflation(lasp_orset, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_strict_inflation(lasp_orset, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_strict_inflation(lasp_orset, A2, B2)),

    %% A3 after A2.
    ?assertEqual(true, is_lattice_strict_inflation(lasp_orset, A2, A3)).

%% lasp_orset_gbtree tests.

lasp_orset_gbtree_inflation_test() ->
    A1 = lasp_orset_gbtree:new(),
    B1 = lasp_orset_gbtree:new(),

    {ok, A2} = lasp_orset_gbtree:update({add, 1}, a, A1),
    {ok, B2} = lasp_orset_gbtree:update({add, 2}, b, B1),
    {ok, A3} = lasp_orset_gbtree:update({remove, 1}, a, A2),

    %% A1 and B1 are equivalent.
    ?assertEqual(true, is_lattice_inflation(lasp_orset_gbtree, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_inflation(lasp_orset_gbtree, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_inflation(lasp_orset_gbtree, A2, B2)),

    %% A3 after A2.
    ?assertEqual(true, is_lattice_inflation(lasp_orset_gbtree, A2, A3)).

lasp_orset_gbtree_strict_inflation_test() ->
    A1 = lasp_orset_gbtree:new(),
    B1 = lasp_orset_gbtree:new(),

    {ok, A2} = lasp_orset_gbtree:update({add, 1}, a, A1),
    {ok, B2} = lasp_orset_gbtree:update({add, 2}, b, B1),
    {ok, A3} = lasp_orset_gbtree:update({remove, 1}, a, A2),

    %% A1 and B1 are equivalent.
    ?assertEqual(false, is_lattice_strict_inflation(lasp_orset_gbtree, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_strict_inflation(lasp_orset_gbtree, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_strict_inflation(lasp_orset_gbtree, A2, B2)),

    %% A3 after A2.
    ?assertEqual(true, is_lattice_strict_inflation(lasp_orset_gbtree, A2, A3)).

-endif.
