%% -------------------------------------------------------------------
%%
%% gb_trees_ext: Extensions to the gb_trees library.
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

-module(gb_trees_ext).

-export([merge/3]).

merge(Tree1, Tree2, Fun) ->
    Iter1 = gb_trees:iterator(Tree1),
    Next1 = gb_trees:next(Iter1),
    Iter2 = gb_trees:iterator(Tree2),
    Next2 = gb_trees:next(Iter2),
    do_merge(Next1, Next2, Fun, gb_trees:empty()).

do_merge({Key1, Val1, Iter1Next}, {Key2, Val2, Iter2Next}, Fun, Tree0) when Key1 == Key2 ->
    Tree = gb_trees:insert(Key1, Fun(Val1, Val2), Tree0),
    do_merge(gb_trees:next(Iter1Next),
             gb_trees:next(Iter2Next), Fun, Tree);

do_merge({Key1, Val1, Iter1Next}, {Key2, _, _} = Next2, Fun, Tree0) when Key1 < Key2 ->
    Tree = gb_trees:insert(Key1, Val1, Tree0),
    do_merge(gb_trees:next(Iter1Next), Next2, Fun, Tree);

do_merge({Key1, Val1, Iter1Next}, none, Fun, Tree0) ->
    Tree = gb_trees:insert(Key1, Val1, Tree0),
    do_merge(gb_trees:next(Iter1Next), none, Fun, Tree);

do_merge({Key1, _, _} = Next1, {Key2, Val2, Iter2Next}, Fun, Tree0) when Key1 > Key2 ->
    Tree = gb_trees:insert(Key2, Val2, Tree0),
    do_merge(Next1, gb_trees:next(Iter2Next), Fun, Tree);

do_merge(none, {Key2, Val2, Iter2Next}, Fun, Tree0) ->
    Tree = gb_trees:insert(Key2, Val2, Tree0),
    do_merge(none, gb_trees:next(Iter2Next), Fun, Tree);

do_merge(none, none, _Fun, Tree) ->
    Tree.
