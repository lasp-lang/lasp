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

-module(lasp_peer_service).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-export([peer_service/0]).

-export([join/1,
         leave/0,
         members/0,
         manager/0,
         stop/0,
         stop/1]).

%%%===================================================================
%%% Callback Interface
%%%===================================================================

%% Attempt to join node.
-callback join(node()) -> ok | {error, atom()}.

%% Remove a node from the cluster.
-callback leave() -> ok.

%% Return members of the cluster.
-callback members() -> {ok, [node()]}.

%% Return manager.
-callback manager() -> module().

%% Stop the peer service on a given node.
-callback stop() -> ok.

%% Stop the peer service on a given node for a particular reason.
-callback stop(iolist()) -> ok.

%%%===================================================================
%%% External API
%%%===================================================================

%% @doc Prepare node to join a cluster.
join(Node) ->
    do(join, [Node]).

%% @doc Return cluster members.
members() ->
    do(members, []).

%% @doc Return manager.
manager() ->
    do(manager, []).

%% @doc Leave the cluster.
leave() ->
    do(leave, []).

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
do(Function, Args) ->
    Backend = peer_service(),
    erlang:apply(Backend, Function, Args).

%% @doc Return the currently active peer service.
peer_service() ->
    application:get_env(?APP,
                        peer_service,
                        lasp_partisan_peer_service).
