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

-module(lasp_peer_protocol).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([start_link/4]).

-export([init/4]).

start_link(ListenerPid, Socket, Transport, Options) ->
    Pid = spawn_link(?MODULE, init, [ListenerPid, Socket, Transport, Options]),
    {ok, Pid}.

%% @doc Initialize connection.
init(ListenerPid, Socket, Transport, _Options) ->
    %% Acknowledge the connection.
    ok = ranch:accept_ack(ListenerPid),

    %% Send welcome message with node name.
    HelloMessage = list_to_binary("HELO " ++ atom_to_list(node()) ++ "\r\n"),
    Transport:send(Socket, HelloMessage),
    {ok, Data} = Transport:recv(Socket, 0, 30000),

    join_remote(Socket, Transport, Data).

%% @doc Wait for remote node to respond with node name.
join_remote(Socket, Transport, <<"HELO ", Data/binary>>) ->
    Node = list_to_atom(strip(Data)),
    lager:info("Remote node joined: ~p~n", [Node]),

    %% Connect the node with Distributed Erlang, just for now for
    %% control messaging in the test suite execution.
    case net_adm:ping(Node) of
        pong ->
            lager:info("Node connected via disterl."),
            Transport:send(Socket, <<"PONG\r\n">>);
        pang ->
            lager:info("Node could not be connected."),
            Transport:send(Socket, <<"PANG\r\n">>)
    end,

    ok;
join_remote(Socket, Transport, _Data) ->
    Transport:send(Socket, <<"NO\r\n">>),
    ok.

%% @private
strip(Data) ->
    re:replace(Data, "\\s+", "", [global, {return, list}]).
