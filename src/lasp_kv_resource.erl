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
         to_erlang/2]).

-include("lasp.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {id, type, object}).

-spec init(list()) -> {ok, term()}.
init(_) ->
    {ok, #ctx{}}.

allowed_methods(ReqData, Ctx) ->
    {['GET', 'POST', 'HEAD'], ReqData, Ctx}.

content_types_provided(ReqData, Ctx) ->
    {[{"application/erlang", to_erlang}], ReqData, Ctx}.

process_post(ReqData, Ctx) ->
    Body = wrq:req_body(ReqData),

    Id = wrq:path_info(id, ReqData),
    Type = wrq:path_info(type, ReqData),

    case {Id, Type} of
        {undefined, _} ->
            {false, ReqData, Ctx};
        {_, undefined} ->
            {false, ReqData, Ctx};
        {_, _} ->
            AtomType = atomize(Type),
            BinaryId = binary(Id),
            Decoded = lasp_type:decode(AtomType, erlang, Body),

            case lasp:bind({BinaryId, AtomType}, Decoded) of
                {ok, Object} ->
                    {true, ReqData, Ctx#ctx{id=BinaryId, type=AtomType, object=Object}};
                Error ->
                    lager:info("Received error response: ~p", [Error]),
                    {false, ReqData, Ctx}
            end
    end.

resource_exists(ReqData, Ctx) ->
    Id = wrq:path_info(id, ReqData),
    Type = wrq:path_info(type, ReqData),

    case {Id, Type} of
        {undefined, _} ->
            {false, ReqData, Ctx};
        {_, undefined} ->
            {false, ReqData, Ctx};
        {_, _} ->
            AtomType = atomize(Type),
            BinaryId = binary(Id),

            case lasp:read({BinaryId, AtomType}, undefined) of
                {ok, Object} ->
                    {true, ReqData, Ctx#ctx{id=BinaryId, type=AtomType, object=Object}};
                Error ->
                    lager:info("Received error response: ~p", [Error]),
                    {false, ReqData, Ctx}
            end
   end.

to_erlang(ReqData, #ctx{type=Type, object={_Id, _Type, _Metadata, Value}}=Ctx) ->
    Encoded = lasp_type:encode(Type, erlang, Value),
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
