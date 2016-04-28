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

%% @todo Once we know this works, convert it to a gen_server.

-module(lasp_peer_protocol_client).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([start_link/1,
         init/1]).

-include("lasp.hrl").

start_link(Peer) ->
    Pid = spawn_link(?MODULE, init, [Peer]),
    {ok, Pid}.

-ifdef(TEST).

%% @doc If we're running a local test, we have to use the same IP
%%      address for every bind operation, but a different port instead of the
%%      standard port.
%%
init(Peer) ->
    %% Bootstrap with disterl.
    PeerPort = rpc:call(Peer,
                        lasp_config,
                        get,
                        [peer_port, ?PEER_PORT]),

    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, PeerPort, [binary, {packet, 2}]),

    ok = gen_tcp:send(Socket, term_to_binary({hello, node(), peer_port()})),
    ok = gen_tcp:close(Socket).

%% @private
peer_port() ->
    lasp_config:get(peer_port, ?PEER_PORT).

-else.

%% @doc Assume that under normal circumstances, we are running on the
%%      proper ports and have been supplied an IP address of a peer to
%%      connect to; avoid disterl.
%%
init(Peer) ->
    {ok, Socket} = gen_tcp:connect(Peer, ?PEER_PORT, [binary, {packet, 2}]),
    ok = gen_tcp:close(Socket).

-endif.
