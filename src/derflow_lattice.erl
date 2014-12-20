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

-module(derflow_lattice).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-include("derflow.hrl").

%% Exported utility functions.
-export([threshold_met/3,
         is_inflation/3,
         is_lattice_inflation/3,
         is_strict_inflation/3,
         is_lattice_strict_inflation/3,
         generate_operations/2]).

%% @doc Given an object type from riak_dt; generate a series of
%%      operations for that type which are representative of a partial
%%      order of operations on this object yielding this state.
%%
generate_operations(riak_dt_gset, Set) ->
    Values = riak_dt_gset:value(Set),
    {ok, [{add, Value} || Value <- Values]}.

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
threshold_met(derflow_ivar, undefined, {strict, undefined}) ->
    false;
threshold_met(derflow_ivar, undefined, undefined) ->
    true;
threshold_met(derflow_ivar, _Value, {strict, undefined}) ->
    true;
threshold_met(riak_dt_gset, Value, {strict, Threshold}) ->
    is_strict_inflation(riak_dt_gset, Threshold, Value);
threshold_met(riak_dt_gset, Value, Threshold) ->
    is_inflation(riak_dt_gset, Threshold, Value);
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
is_lattice_inflation(derflow_ivar, undefined, undefined) ->
    true;
is_lattice_inflation(derflow_ivar, undefined, _Current) ->
    true;
is_lattice_inflation(derflow_ivar, Previous, Current) when Previous =/= Current ->
    false;
is_lattice_inflation(riak_dt_gcounter, undefined, _) ->
    true;
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
            end, true, PreviousList);
is_lattice_inflation(riak_dt_gset, undefined, _) ->
    true;
is_lattice_inflation(riak_dt_gset, Previous, Current) ->
    sets:is_subset(
        sets:from_list(riak_dt_gset:value(Previous)),
        sets:from_list(riak_dt_gset:value(Current))).

%% @doc Determine if a change for a given type is a strict inflation or
%%      not.
%%
%%      Given a particular type and two instances of that type,
%%      determine if `Current' is a strict inflation of `Previous'.
%%
is_lattice_strict_inflation(derflow_ivar, undefined, undefined) ->
    false;
is_lattice_strict_inflation(derflow_ivar, undefined, Current) when Current =/= undefined ->
    true;
is_lattice_strict_inflation(derflow_ivar, _Previous, _Current) ->
    false;
is_lattice_strict_inflation(riak_dt_gset, undefined, Current) ->
    is_lattice_inflation(riak_dt_gset, undefined, Current);
is_lattice_strict_inflation(riak_dt_gset, Previous, Current) ->
    is_lattice_inflation(riak_dt_gset, Previous, Current) andalso
        lists:usort(riak_dt_gset:value(Previous)) =/=
        lists:usort(riak_dt_gset:value(Current)).
