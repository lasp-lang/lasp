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

-module(lasp_kv_resource).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([init/1,
         allowed_methods/2,
         process_post/2,
         content_types_provided/2,
         resource_exists/2,
         to_msgpack/2]).

-include("lasp.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {id, type, object}).

-spec init(list()) -> {ok, term()}.
init(_) ->
    {ok, #ctx{}}.

allowed_methods(ReqData, Ctx) ->
    {['GET', 'POST', 'HEAD'], ReqData, Ctx}.

content_types_provided(ReqData, Ctx) ->
    {[{"application/msgpack", to_msgpack}], ReqData, Ctx}.

process_post(ReqData, Ctx) ->
    Body = wrq:req_body(ReqData),

    Id = wrq:path_info(id, ReqData),
    Type = wrq:path_info(type, ReqData),

    case {Id, Type} of
        {undefined, _} ->
            {false, ReqData, Ctx#ctx{id=Id, type=Type}};
        {_, undefined} ->
            {false, ReqData, Ctx#ctx{id=Id, type=Type}};
        {_, _} ->
            Decoded = Type:decode(msgpack, Body),

            case lasp:bind({binary(Id), atomize(Type)}, Decoded) of
                {ok, Object} ->
                    {true, ReqData, Ctx#ctx{id=Id, type=Type, object=Object}};
                Error ->
                    lager:info("Received error response: ~p", [Error]),
                    {false, ReqData, Ctx#ctx{id=Id, type=Type}}
            end
    end.

resource_exists(ReqData, Ctx) ->
    Id = wrq:path_info(id, ReqData),
    Type = wrq:path_info(type, ReqData),

    case {Id, Type} of
        {undefined, _} ->
            {false, ReqData, Ctx#ctx{id=Id, type=Type}};
        {_, undefined} ->
            {false, ReqData, Ctx#ctx{id=Id, type=Type}};
        {_, _} ->
            case lasp:read({binary(Id), atomize(Type)}, undefined) of
                {ok, Object} ->
                    {true, ReqData, Ctx#ctx{id=Id, type=Type, object=Object}};
                Error ->
                    lager:info("Received error response: ~p", [Error]),
                    {false, ReqData, Ctx#ctx{id=Id, type=Type}}
            end
   end.

to_msgpack(ReqData, #ctx{type=Type, object=Object}=Ctx) ->
    Encoded = Type:encode(msgpack, Object),
    {Encoded, ReqData, Ctx}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
atomize(Type) when is_list(Type) ->
    list_to_existing_atom(Type).

%% @private
binary(Id) when is_list(Id) ->
    term_to_binary(Id).
