%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(lasp_partial_replication).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-export([is_filtered/3]).

is_filtered(Peer, Metadata, Store) ->
    PeerInterests = interests(Peer, Store),

    ObjectTopics = case orddict:find(topics, Metadata) of
        {ok, T} ->
            ObjectInterestType = lasp_type:get_type(?OBJECT_INTERESTS_TYPE),
            {ok, Topics} = ObjectInterestType:query(T),
            Topics;
        _ ->
            sets:new()
    end,

    case sets:size(ObjectTopics) > 0 andalso sets:size(PeerInterests) > 0 of
        true ->
            %% If the node has interests, and the object is on a topic, only allow 
            %% if they are not disjoint.
            not sets:is_disjoint(ObjectTopics, PeerInterests);
        false ->
            %% Otherwise, send.
            true
    end.

%% @private
interests(Peer, Store) ->
    Interests = try ?CORE:query(?INTERESTS_ID, Store) of
        {ok, Value} ->
            Value
    catch
        _:{error, killed} ->
            sets:new()
    end,
    proplists:get_value(Peer, Interests, sets:new()).