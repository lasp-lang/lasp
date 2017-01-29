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

-callback(extract_log_type_and_payload(term()) -> [{term(), term()}]).

-export([send/3,
         log_transmission/2]).

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
    Peers.

%% @private
without_me(Members) ->
    Members -- [node()].

%% @private
% select_random_sublist(List, K) ->
%     lists:sublist(shuffle(List), K).

% %% @reference http://stackoverflow.com/questions/8817171/shuffling-elements-in-a-list-randomly-re-arrange-list-elements/8820501#8820501
% shuffle(L) ->
%     [X || {_, X} <- lists:sort([{lasp_support:puniform(65535), N} || N <- L])].

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

%% @private
send(Mod, Msg, Peer) ->
    log_transmission(Mod:extract_log_type_and_payload(Msg), 1),
    PeerServiceManager = lasp_config:peer_service_manager(),
    case PeerServiceManager:forward_message(Peer, Mod, Msg) of
        ok ->
            ok;
        _Error ->
            % lager:error("Failed send to ~p for reason ~p", [Peer, Error]),
            ok
    end.

%% @private
log_transmission(ToLog, PeerCount) ->
    try
        case lasp_config:get(instrumentation, false) of
            true ->
                lists:foreach(
                    fun({Type, Payload}) ->
                        ok = lasp_instrumentation:transmission(Type, Payload, PeerCount)
                    end,
                    ToLog
                ),
                ok;
            false ->
                ok
        end
    catch
        _:Error ->
            lager:error("Couldn't log transmission: ~p", [Error]),
            ok
    end.
