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
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com").

-export([merge/3, equal/2, foldl/3, iterate/3]).

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

equal(Tree1, Tree2) ->
    Iter1 = gb_trees:iterator(Tree1),
    Next1 = gb_trees:next(Iter1),
    Iter2 = gb_trees:iterator(Tree2),
    Next2 = gb_trees:next(Iter2),
    do_equal(Next1, Next2).

do_equal({Key, Value, Iter1Next}, {Key, Value, Iter2Next}) ->
    do_equal(gb_trees:next(Iter1Next), gb_trees:next(Iter2Next));
do_equal(none, none) ->
    true;
do_equal(_, _) ->
    false.

foldl(Fun, Acc, Tree) ->
    Iter = gb_trees:iterator(Tree),
    do_foldl(gb_trees:next(Iter), Acc, Fun).

do_foldl(none, Acc, _Fun) ->
    Acc;
do_foldl({Key, Value, Iter}, Acc0, Fun) ->
    Acc = Fun(Key, Value, Acc0),
    do_foldl(gb_trees:next(Iter), Acc, Fun).

iterate(VisitFun, FinishFun, Tree) ->
    Iter = gb_trees:iterator(Tree),
    do_iterate(gb_trees:next(Iter), VisitFun, FinishFun).

do_iterate(none, _NodeFun, FinishFun) ->
    FinishFun();
do_iterate({Key, Value, Iter}, VisitFun, FinishFun) ->
    VisitFun(Key, Value),
    do_iterate(gb_trees:next(Iter), VisitFun, FinishFun).
