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
         orset_causal_product/2,
         orset_causal_union/2]).

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
threshold_met(lasp_ivar, undefined, {strict, undefined}) ->
    false;
threshold_met(lasp_ivar, undefined, undefined) ->
    true;
threshold_met(lasp_ivar, _Value, {strict, undefined}) ->
    true;
threshold_met(lasp_ivar, Value, Threshold) when Value =:= Threshold ->
    true;
threshold_met(lasp_ivar, Value, Threshold) when Value =/= Threshold ->
    false;

threshold_met(riak_dt_gset, Value, {strict, Threshold}) ->
    is_strict_inflation(riak_dt_gset, Threshold, Value);
threshold_met(riak_dt_gset, Value, Threshold) ->
    is_inflation(riak_dt_gset, Threshold, Value);

threshold_met(riak_dt_orset, Value, {strict, Threshold}) ->
    is_strict_inflation(riak_dt_orset, Threshold, Value);
threshold_met(riak_dt_orset, Value, Threshold) ->
    is_inflation(riak_dt_orset, Threshold, Value);

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
is_inflation(Type, Previous, Current) ->
    is_lattice_inflation(Type, Previous, Current).

%% @doc Determine if a change is a strict inflation or not.
%%
%%      Given a particular type and two instances of that type,
%%      determine if `Current' is a strict inflation of `Previous'.
%%
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

is_lattice_inflation(riak_dt_gset, Previous, Current) ->
    sets:is_subset(
        sets:from_list(riak_dt_gset:value(Previous)),
        sets:from_list(riak_dt_gset:value(Current)));

is_lattice_inflation(riak_dt_orset, Previous, Current) ->
    lists:foldl(fun({Element, Ids}, Acc) ->
                        case lists:keyfind(Element, 1, Current) of
                            false ->
                                Acc andalso false;
                            {_, Ids1} ->
                                Acc andalso ids_inflated(Ids, Ids1)
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

is_lattice_strict_inflation(riak_dt_gset, Previous, Current) ->
    is_lattice_inflation(riak_dt_gset, Previous, Current) andalso
        lists:usort(riak_dt_gset:value(Previous)) =/=
        lists:usort(riak_dt_gset:value(Current));

is_lattice_strict_inflation(riak_dt_orset, [], Current)
  when Current =/= []->
    true;
is_lattice_strict_inflation(riak_dt_orset, Previous, Current) ->
    %% We already know that `Previous' is fully covered in `Current', so
    %% we just need to look for a change -- removal (false -> true), or
    %% addition of a new element.
    IsLatticeInflation = is_lattice_inflation(riak_dt_orset,
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

ids_inflated(Previous, Current) ->
    lists:foldl(fun({Id, _}, Acc) ->
                        case lists:keyfind(Id, 1, Current) of
                            false ->
                                Acc andalso false;
                            _ ->
                                Acc andalso true
                        end
                end, true, Previous).

%% @doc Compute a cartesian product from causal metadata stored in the
%%      orset.
%%
%%      Computes product of `Xs' and `Ys' and map deleted through using
%%      an or operation.
%%
orset_causal_product(Xs, Ys) ->
    lists:foldl(fun({X, XDeleted}, XAcc) ->
                lists:foldl(fun({Y, YDeleted}, YAcc) ->
                            [{[X, Y], XDeleted orelse YDeleted}] ++ YAcc
                    end, [], Ys) ++ XAcc
        end, [], Xs).

%% @doc Compute the union of causal metadata.
orset_causal_union(Xs, Ys) ->
    Xs ++ Ys.

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

%% riak_dt_gset tests.

riak_dt_gset_inflation_test() ->
    A1 = riak_dt_gset:new(),
    B1 = riak_dt_gset:new(),

    {ok, A2} = riak_dt_gset:update({add, 1}, a, A1),
    {ok, B2} = riak_dt_gset:update({add, 2}, b, B1),

    %% A1 and B1 are equivalent.
    ?assertEqual(true, is_lattice_inflation(riak_dt_gset, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_inflation(riak_dt_gset, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_inflation(riak_dt_gset, A2, B2)).

riak_dt_gset_strict_inflation_test() ->
    A1 = riak_dt_gset:new(),
    B1 = riak_dt_gset:new(),

    {ok, A2} = riak_dt_gset:update({add, 1}, a, A1),
    {ok, B2} = riak_dt_gset:update({add, 2}, b, B1),

    %% A1 and B1 are equivalent.
    ?assertEqual(false, is_lattice_strict_inflation(riak_dt_gset, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_strict_inflation(riak_dt_gset, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_strict_inflation(riak_dt_gset, A2, B2)).

%% riak_dt_orset tests.

riak_dt_orset_inflation_test() ->
    A1 = riak_dt_orset:new(),
    B1 = riak_dt_orset:new(),

    {ok, A2} = riak_dt_orset:update({add, 1}, a, A1),
    {ok, B2} = riak_dt_orset:update({add, 2}, b, B1),
    {ok, A3} = riak_dt_orset:update({remove, 1}, a, A2),

    %% A1 and B1 are equivalent.
    ?assertEqual(true, is_lattice_inflation(riak_dt_orset, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_inflation(riak_dt_orset, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_inflation(riak_dt_orset, A2, B2)),

    %% A3 after A2.
    ?assertEqual(true, is_lattice_inflation(riak_dt_orset, A2, A3)).

riak_dt_orset_strict_inflation_test() ->
    A1 = riak_dt_orset:new(),
    B1 = riak_dt_orset:new(),

    {ok, A2} = riak_dt_orset:update({add, 1}, a, A1),
    {ok, B2} = riak_dt_orset:update({add, 2}, b, B1),
    {ok, A3} = riak_dt_orset:update({remove, 1}, a, A2),

    %% A1 and B1 are equivalent.
    ?assertEqual(false, is_lattice_strict_inflation(riak_dt_orset, A1, B1)),

    %% A2 after A1.
    ?assertEqual(true, is_lattice_strict_inflation(riak_dt_orset, A1, A2)),

    %% Concurrent
    ?assertEqual(false, is_lattice_strict_inflation(riak_dt_orset, A2, B2)),

    %% A3 after A2.
    ?assertEqual(true, is_lattice_strict_inflation(riak_dt_orset, A2, A3)).

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


-endif.
