%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Christopher Meiklejohn.  All Rights Reserved.
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

-module(lasp_partisan_peer_service).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-behaviour(lasp_peer_service).

-define(PEER_SERVICE, partisan_peer_service).

-export([join/1,
         leave/0,
         members/0,
         manager/0,
         stop/0,
         stop/1]).

%%%===================================================================
%%% External API
%%%===================================================================

%% @doc Prepare node to join a cluster.
join({_Name, _IPAddress, _Port} = Node) ->
    do(join, [Node]).

%% @doc Leave the cluster.
leave() ->
    do(leave, []).

%% @doc Leave the cluster.
members() ->
    do(?PEER_SERVICE, members, []).

%% @doc Leave the cluster.
manager() ->
    do(?PEER_SERVICE, manager, []).

%% @doc Stop node.
stop() ->
    stop("received stop request").

%% @doc Stop node for a given reason.
stop(Reason) ->
    do(stop, [Reason]).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Execute call to the proper backend.
do(join, Args) ->
    erlang:apply(?PEER_SERVICE, join, Args);
do(Function, Args) ->
    erlang:apply(?PEER_SERVICE, Function, Args).

%% @doc Execute call to the proper backend.
do(Module, Function, Args) ->
    erlang:apply(Module, Function, Args).
