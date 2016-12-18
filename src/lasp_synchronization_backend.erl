%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(lasp_synchronization_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([broadcast_tree_mode/0,
         tutorial_mode/0,
         client_server_mode/0,
         peer_to_peer_mode/0,
         i_am_server/0,
         i_am_client/0,
         reactive_server/0,
         seed/0,
         membership/0,
         compute_exchange/1,
         without_me/1]).

%% @private
membership() ->
    lasp_peer_service:members().

%% @private
seed() ->
    rand_compat:seed(erlang:phash2([node()]),
                     erlang:monotonic_time(),
                     erlang:unique_integer()).

%% @private
compute_exchange(Peers) ->
    PeerServiceManager = lasp_config:peer_service_manager(),

    Probability = lasp_config:get(partition_probability, 0),
    lasp_logger:extended("Probability of partition: ~p", [Probability]),
    Percent = lasp_support:puniform(100),

    case Percent =< Probability of
        true ->
            case PeerServiceManager of
                partisan_client_server_peer_service_manager ->
                    lager:info("Partitioning from server."),
                    [];
                _ ->
                    lager:info("Partitioning ~p% of the network.",
                               [Percent]),

                    %% Select percentage, minus one node which will be
                    %% the server node.
                    K = round((Percent / 100) * length(Peers)),
                    lager:info("Partitioning ~p%: ~p nodes.",
                               [Percent, K]),
                    ServerNodes = case PeerServiceManager:active(server) of
                        {ok, undefined} ->
                            [];
                        {ok, Server} ->
                            [Server];
                        error ->
                            []
                    end,
                    lager:info("ServerNodes: ~p", [ServerNodes]),

                    Random = select_random_sublist(Peers, K),
                    RandomAndServer = lists:usort(ServerNodes ++ Random),
                    lager:info("Partitioning ~p from ~p during sync.",
                               [RandomAndServer, Peers -- RandomAndServer]),
                    Peers -- RandomAndServer
            end;
        false ->
            Peers
    end.

%% @private
without_me(Members) ->
    Members -- [node()].

%% @private
select_random_sublist(List, K) ->
    lists:sublist(shuffle(List), K).

%% @reference http://stackoverflow.com/questions/8817171/shuffling-elements-in-a-list-randomly-re-arrange-list-elements/8820501#8820501
shuffle(L) ->
    [X || {_, X} <- lists:sort([{lasp_support:puniform(65535), N} || N <- L])].

%% @private
broadcast_tree_mode() ->
    lasp_config:get(broadcast, false).

%% @private
tutorial_mode() ->
    lasp_config:get(tutorial, false).

%% @private
client_server_mode() ->
    lasp_config:peer_service_manager() == partisan_client_server_peer_service_manager.

%% @private
peer_to_peer_mode() ->
    lasp_config:peer_service_manager() == partisan_hyparview_peer_service_manager orelse
    lasp_config:peer_service_manager() == partisan_default_peer_service_manager.

%% @private
i_am_server() ->
    partisan_config:get(tag, undefined) == server.

%% @private
i_am_client() ->
    partisan_config:get(tag, undefined) == client.

%% @private
reactive_server() ->
    lasp_config:get(reactive_server, false).
