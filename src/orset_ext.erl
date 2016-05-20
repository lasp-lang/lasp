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

-module(orset_ext).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([intersect/2,
         map/2,
         union/2,
         product/2,
         filter/2]).

union(LValue, RValue) ->
    orset:merge(LValue, RValue).

product({orset, LValue}, {orset, RValue}) ->
    FolderFun = fun({X, XCausality}, {orset, Acc}) ->
        {orset, Acc ++ [{{X, Y}, causal_product(XCausality, YCausality)} || {Y, YCausality} <- RValue]}
    end,
    lists:foldl(FolderFun, new(), LValue).

intersect({orset, LValue}, RValue) ->
    lists:foldl(intersect_folder(RValue), new(), LValue).

%% @private
intersect_folder({orset, RValue}) ->
    fun({X, XCausality}, {orset, Acc}) ->
            Values = case lists:keyfind(X, 1, RValue) of
                         {_Y, YCausality} ->
                             [{X, causal_union(XCausality, YCausality)}];
                         false ->
                             []
                     end,
            {orset, Acc ++ Values}
    end.

map(Function, {orset, V}) ->
    FolderFun = fun({X, Causality}, {orset, Acc}) ->
                        {orset, Acc ++ [{Function(X), Causality}]}
                end,
    lists:foldl(FolderFun, new(), V).

filter(Function, {orset, V}) ->
    FolderFun = fun({X, Causality}, {orset, Acc}) ->
                        case Function(X) of
                            true ->
                                {orset, Acc ++ [{X, Causality}]};
                            false ->
                                {orset, Acc}
                        end
                end,
    lists:foldl(FolderFun, new(), V).

%% @private
new() ->
    orset:new().

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
