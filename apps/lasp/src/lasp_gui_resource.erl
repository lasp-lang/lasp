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

-module(lasp_gui_resource).

-export([init/1,
         content_types_provided/2,
         to_resource/2]).

-include_lib("lasp/include/lasp.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {
          base_url,
          base_path
         }).
-type context() :: #ctx{}.

%% mappings to the various content types supported for this resource
-define(CONTENT_TYPES,[{"text/css", to_resource},
                       {"text/html",to_resource},
                       {"text/plain",to_resource},
                       {"text/javascript",to_resource}
                      ]).

%% entry-point for the resource from webmachine
-spec init(any()) -> {ok, any()}.
init(Resource) -> {ok,Resource}.

%% return the list of available content types for webmachine
-spec content_types_provided(wrq:reqdata(), context()) ->
         {[{ContentType::string(), HandlerFunction::atom()}],
          wrq:reqdata(), context()}.
content_types_provided(Req,Ctx=index) ->
    {[{"text/html", to_resource}],Req, Ctx};
content_types_provided(Req,Ctx) ->
    Index = file_path(Req),
    MimeType = webmachine_util:guess_mime(Index),
    {[{MimeType, to_resource}],Req, Ctx}.

%% return file path
-spec file_path(wrq:reqdata() | list()) -> string().
file_path(Path) when is_list(Path) ->
    filename:join([code:priv_dir(?APP)] ++ [Path]);
file_path(Req) ->
    Path=wrq:path_tokens(Req),
    filename:join([code:priv_dir(?APP)] ++ Path).

%% loads a resource file from disk and returns it
-spec get_file(wrq:reqdata()) -> binary().
get_file(Req) ->
    Index = file_path(Req),
    {ok,Source}=file:read_file(Index),
    Source.

%% read a file from disk and return it
read_file(File) ->
    Index = file_path(File),
    {ok,Source}=file:read_file(Index),
    Source.

%% respond to an index or file request
-spec to_resource(wrq:reqdata(), context()) ->
                         {binary(), wrq:reqdata(), context()}.
to_resource(Req,Ctx=index) ->
    {read_file("index.html"),Req,Ctx};
to_resource(Req,Ctx) ->
    {get_file(Req),Req,Ctx}.
