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

-module(lasp_peer_protocol_server).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(ranch_protocol).

%% ranch_protocol callbacks.
-export([start_link/4,
         init/4]).

-define(TIMEOUT, 10000).

start_link(ListenerPid, Socket, Transport, Options) ->
    Pid = spawn_link(?MODULE, init, [ListenerPid, Socket, Transport, Options]),
    {ok, Pid}.

%% @doc Initialize connection.
init(ListenerPid, Socket, Transport, _Options) ->
    %% Acknowledge the connection.
    ok = ranch:accept_ack(ListenerPid),

    %% Link to the socket.
    link(Socket),

    %% Set the socket modes.
    ok = inet:setopts(Socket, [{packet, 2}, {active, true}]),

    %% Generate the welcome message, encode it and transmit the message.
    send_message(Socket, Transport, {hello, node()}),

    %% Wait for response.
    Message = receive_message(Socket, Transport),
    case Message of
        {hello, Node} ->
            %% Connect the node with Distributed Erlang, just for now for
            %% control messaging in the test suite execution.
            case net_adm:ping(Node) of
                pong ->
                    lager:info("Node connected via disterl."),
                    send_message(Socket, Transport, ok);
                pang ->
                    lager:info("Node could not be connected."),
                    send_message(Socket, Transport, {error, pang})
            end;
        Response ->
            lager:info("Invalid response: ~p", [Response]),
            send_message(Socket, Transport,
                         {error, {invalid_response, Response}})
    end,

    %% Close socket.
    Transport:close(Socket).

%% @private
receive_message(Socket, Transport) ->
    receive
        {tcp_closed, Socket} ->
            Transport:close(Socket),
            bo;
        Message ->
            decode(Message)
    end.

%% @private
send_message(Socket, Transport, Message) ->
    EncodedMessage = encode(Message),
    Transport:send(Socket, EncodedMessage).

%% @private
encode(Message) ->
    term_to_binary(Message).

%% @private
decode(Message) ->
    binary_to_term(Message).
