%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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

-module(state_orset_ext).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([intersect/2,
         map/2,
         union/2,
         product/2,
         filter/2]).

union(LValue, RValue) ->
    state_orset:merge(LValue, RValue).

product({state_orset, LValue}, {state_orset, RValue}) ->
    FolderFun = fun({X, XCausality}, {state_orset, Acc}) ->
        {state_orset, Acc ++ [{{X, Y}, causal_product(XCausality, YCausality)} || {Y, YCausality} <- RValue]}
    end,
    lists:foldl(FolderFun, new(), LValue).

intersect({state_orset, LValue}, RValue) ->
    lists:foldl(intersect_folder(RValue), new(), LValue).

%% @private
intersect_folder({state_orset, RValue}) ->
    fun({X, XCausality}, {state_orset, Acc}) ->
            Values = case lists:keyfind(X, 1, RValue) of
                         {_Y, YCausality} ->
                             [{X, causal_union(XCausality, YCausality)}];
                         false ->
                             []
                     end,
            {state_orset, Acc ++ Values}
    end.

map(Function, {state_orset, V}) ->
    FolderFun = fun({X, Causality}, {state_orset, Acc}) ->
                        {state_orset, Acc ++ [{Function(X), Causality}]}
                end,
    lists:foldl(FolderFun, new(), V).

filter(Function, {state_orset, V}) ->
    FolderFun = fun({X, Causality}, {state_orset, Acc}) ->
                        case Function(X) of
                            true ->
                                {state_orset, Acc ++ [{X, Causality}]};
                            false ->
                                {state_orset, Acc}
                        end
                end,
    lists:foldl(FolderFun, new(), V).

%% @private
new() ->
    state_orset:new().

%% @private
causal_product(Xs, Ys) ->
    lists:foldl(fun({X, XDeleted}, XAcc) ->
                lists:foldl(fun({Y, YDeleted}, YAcc) ->
                            [{[X, Y], XDeleted orelse YDeleted}] ++ YAcc
                    end, [], Ys) ++ XAcc
        end, [], Xs).

%% @private
causal_union(Xs, Ys) ->
        Xs ++ Ys.
