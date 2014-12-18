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

-module(derflow_example_program).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-behavior(derflow_program).

-export([init/0,
         execute/1,
         execute/2,
         merge/1]).

init() ->
    {ok, riak_dt_gset:new()}.

execute(Acc) ->
    ok = example(),
    {ok, Acc}.

execute(Acc, _X) ->
    ok = example(),
    {ok, Acc}.

merge([Reply|_]) ->
    Reply.

example() ->
    {ok, Id1} = derflow:declare(),

    {ok, Id2} = derflow:declare(),

    {ok, _} = derflow:bind_to(Id2, Id1),
    lager:info("Successful bind."),

    {ok, _} = derflow:bind(Id1, 1),
    lager:info("Successful bind."),

    {ok, _, Value1, _} = derflow:read(Id1),
    lager:info("Successful read: ~p", [Value1]),

    error = derflow:bind(Id1, 2),
    lager:info("Unsuccessful bind."),

    {ok, _, Value1, _} = derflow:read(Id1),
    lager:info("Successful read: ~p", [Value1]),

    {ok, _, Value1, _} = derflow:read(Id2),
    lager:info("Successful read: ~p", [Value1]),

    {ok, Id3} = derflow:declare(),

    {ok, _} = derflow:bind_to(Id3, Id1),
    lager:info("Successful bind."),

    {ok, _, Value1, _} = derflow:read(Id3),
    lager:info("Successful read: ~p", [Value1]),

    ok.
