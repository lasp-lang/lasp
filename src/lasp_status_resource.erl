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

-module(lasp_status_resource).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([init/1,
         content_types_provided/2,
         to_json/2]).

-include_lib("webmachine/include/webmachine.hrl").

-spec init(list()) -> {ok, term()}.
init(_) ->
    {ok, undefined}.

%% return the list of available content types for webmachine
content_types_provided(Req, Ctx) ->
    {[{"application/json", to_json}], Req, Ctx}.

-ifdef(TEST).

to_json(ReqData, State) ->
    NumNodes = rand_compat:uniform(2),
    NodeList = lists:seq(0, NumNodes),
    Nodes = [#{id => Name, name => Name, group => 1} || Name <- NodeList],
    Links = lists:flatten(generate_links(NodeList)),
    Encoded = jsx:encode(#{nodes => Nodes, links => Links}),
    {Encoded, ReqData, State}.

generate_links(NodeList) ->
    lists:map(fun(Source) ->
                lists:map(fun(Target) ->
                            #{source => Source, target => Target}
                    end, NodeList) end, NodeList).

-else.

to_json(ReqData, State) ->
    {ok, {Vertices, Edges}} = case lasp_config:get(broadcast, false) of
        true ->
            sprinter:tree();
        false ->
            sprinter:graph()
    end,
    Nodes = [#{id => Name, name => Name, group => 1} || Name <- Vertices],
    Links = [#{source => V1, target => V2} || {V1, V2} <- Edges],
    Encoded = jsx:encode(#{nodes => Nodes, links => Links}),
    {Encoded, ReqData, State}.

-endif.
