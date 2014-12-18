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
    %% Single-assignment variables.
    {ok, I1} = derflow:declare(),
    {ok, I2} = derflow:declare(),
    {ok, I3} = derflow:declare(),

    V1 = 1,

    %% Attempt pre, and post- dataflow variable bind operations.
    {ok, _} = derflow:bind_to(I2, I1),
    {ok, _} = derflow:bind(I1, V1),
    {ok, _} = derflow:bind_to(I3, I1),

    %% Perform invalid bind.
    error = derflow:bind(I1, 2),

    %% Verify the same value is contained by all.
    {ok, _, V1, _} = derflow:read(I3),
    {ok, _, V1, _} = derflow:read(I2),
    {ok, _, V1, _} = derflow:read(I1),

    %% G-Set variables.
    {ok, L1} = derflow:declare(riak_dt_gset),
    {ok, L2} = derflow:declare(riak_dt_gset),
    {ok, L3} = derflow:declare(riak_dt_gset),

    {ok, S1} = riak_dt_gset:update({add, 1},
                                   undefined, riak_dt_gset:new()),

    %% Attempt pre, and post- dataflow variable bind operations.
    {ok, _} = derflow:bind_to(L2, L1),
    {ok, _} = derflow:bind(L1, S1),
    {ok, _} = derflow:bind_to(L3, L1),

    %% Verify the same value is contained by all.
    {ok, _, S1, _} = derflow:read(L3),
    {ok, _, S1, _} = derflow:read(L2),
    {ok, _, S1, _} = derflow:read(L1),

    %% Test inflation.
    {ok, S2} = riak_dt_gset:update({add, 2},
                                   undefined, S1),
    {ok, _} = derflow:bind(L1, S2),

    %% Verify the same value is contained by all.
    {ok, _, S2, _} = derflow:read(L3),
    {ok, _, S2, _} = derflow:read(L2),
    {ok, _, S2, _} = derflow:read(L1),

    ok.
