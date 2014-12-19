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

%% @doc derpflow transform, for remote code loading.

-module(derpflow_transform).
-author('Christopher Meiklejohn <cmeiklejohn@basho.com>').

-include("derpflow.hrl").

%% Public API
-export([parse_transform/2]).

%% @private
parse_transform(AST, Options) ->
    {store, Store} = lists:keyfind(store, 1, Options),
    put(store, Store),

    {node, Node} = lists:keyfind(node, 1, Options),
    put(node, Node),

    {partition, Partition} = lists:keyfind(partition, 1, Options),
    put(partition, Partition),

    {module, Module} = lists:keyfind(module, 1, Options),
    put(module, Module),

    walk_ast([], AST).

%% @private
walk_ast(Acc, []) ->
    lists:reverse(Acc);
walk_ast(Acc, [{attribute, _, module, {_Module, _PmodArgs}}=H|T]) ->
    walk_ast([H|Acc], T);
walk_ast(Acc, [{attribute, Line, module, _Module}=_H|T]) ->
    H1 = {attribute, Line, module, get(module)},
    walk_ast([H1|Acc], T);
walk_ast(Acc, [{function, Line, Name, Arity, Clauses}|T]) ->
    walk_ast([{function, Line, Name, Arity,
                walk_clauses([], Clauses)}|Acc], T);
walk_ast(Acc, [{attribute, _, record, {_Name, _Fields}}=H|T]) ->
    walk_ast([H|Acc], T);
walk_ast(Acc, [H|T]) ->
    walk_ast([H|Acc], T).

%% @private
walk_clauses(Acc, []) ->
    lists:reverse(Acc);
walk_clauses(Acc, [{clause, Line, Arguments, Guards, Body}|T]) ->
    walk_clauses([{clause,
                   Line, Arguments, Guards, walk_body([], Body)}|Acc],
                 T).

%% @private
walk_body(Acc, []) ->
    lists:reverse(Acc);
walk_body(Acc, [H|T]) ->
    walk_body([transform_statement(H)|Acc], T).

%% @private
transform_statement({call, Line1,
                     {remote, Line2,
                      {atom, Line3, derpflow}, {atom, Line4, Func}},
                     Arguments}) ->
    {call, Line1,
     {remote, Line2,
      {atom, Line3, ?BACKEND}, {atom, Line4, Func}},
     Arguments ++ [{atom, Line4, get(store)}]};
transform_statement(Stmt) when is_tuple(Stmt) ->
    list_to_tuple(transform_statement(tuple_to_list(Stmt)));
transform_statement(Stmt) when is_list(Stmt) ->
    [transform_statement(S) || S <- Stmt];
transform_statement(Stmt) ->
    Stmt.
