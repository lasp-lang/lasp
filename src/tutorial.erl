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

-module(tutorial).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([connect/0,
         sync/0,
         mutate/3,
         query/2]).

-define(NODES, ['alice@127.0.0.1', 'bob@127.0.0.1']).

-include("lasp.hrl").

%% @doc Connect to other nodes in the cluster.
connect() ->
    lists:foreach(fun(Node) ->
                lager:info("Connecting to ~p", [Node]),

                %% First, establish the EPMD connection.
                pong = net_adm:ping(Node),

                %% Get peer port.
                PeerPort = rpc:call(Node,
                                    partisan_config,
                                    get,
                                    [peer_port, ?PEER_PORT]),

                %% Now, connect with the Lasp peer service.
                Result = lasp_peer_service:join({Node, {127,0,0,1}, PeerPort}),
                lager:info("Join result: ~p", [Result])

        end, lists:delete(node(), ?NODES)),

    ok.

query(Id, Type) ->
    %% Get actual identifier.
    Identifier = identifier(Id, Type),

    %% Ensure the object is declared.
    {ok, Value} = lasp:query(Identifier),

    sets:to_list(Value).

mutate(Id, Type, Operation) ->
    %% Convert actor to binary representation.
    Actor = list_to_binary(atom_to_list(node())),

    %% Get actual identifier.
    Identifier = identifier(Id, Type),

    %% Ensure the object is declared.
    {ok, _} = lasp:declare(Identifier, Type),

    %% Perform mutation.
    {ok, _} = lasp:update(Identifier, Operation, Actor),

    ok.

identifier(Id, Type) ->
    %% Convert identifier to binary.
    BinaryId = list_to_binary(atom_to_list(Id)),

    %% Combine into id and type pair.
    Identifier = {BinaryId, Type},

    Identifier.

sync() ->
    Pid = whereis(lasp_default_broadcast_distribution_backend),
    Pid ! aae_sync,
    ok.
