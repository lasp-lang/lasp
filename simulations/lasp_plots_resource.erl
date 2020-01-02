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

-module(lasp_plots_resource).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([init/1,
         content_types_provided/2,
         to_json/2]).

-include("lasp.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-spec init(list()) -> {ok, term()}.
init(_) ->
    {ok, undefined}.

%% return the list of available content types for webmachine
content_types_provided(Req, Ctx) ->
    {[{"application/json", to_json}], Req, Ctx}.

to_json(ReqData, State) ->
    Filenames = filelib:wildcard("*.pdf", code:lib_dir(?APP) ++ "/plots"),
    Filenames1 = [list_to_binary(Filename) || Filename <- Filenames],
    Encoded = jsx:encode(#{plots => Filenames1}),
    {Encoded, ReqData, State}.
