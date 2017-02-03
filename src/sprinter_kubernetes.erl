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

-module(sprinter_kubernetes).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("sprinter.hrl").

-export([clients/0,
         servers/0,
         upload_artifact/3,
         download_artifact/2]).

%% @private
upload_artifact(#state{eredis=Eredis}, Node, Membership) ->
    {ok, <<"OK">>} = eredis:q(Eredis, ["SET", Node, Membership]),
    lager:info("Pushed artifact to Redis: ~p ~p", [Node, Membership]),
    ok.

%% @private
download_artifact(#state{eredis=Eredis}, Node) ->
    {ok, Membership} = eredis:q(Eredis, ["GET", Node]),
    lager:info("Received artifact from Redis: ~p ~p", [Node, Membership]),
    Membership.

%% @private
clients() ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    LabelSelector = "tag%3Dclient,evaluation-timestamp%3D" ++ integer_to_list(EvalTimestamp),
    pods_from_kubernetes(LabelSelector).

%% @private
servers() ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    LabelSelector = "tag%3Dserver,evaluation-timestamp%3D" ++ integer_to_list(EvalTimestamp),
    pods_from_kubernetes(LabelSelector).

%% @private
pods_from_kubernetes(LabelSelector) ->
    DecodeFun = fun(Body) -> jsx:decode(Body, [return_maps]) end,

    case get_request(generate_pods_url(LabelSelector), DecodeFun) of
        {ok, PodList} ->
            generate_pod_nodes(PodList);
        Error ->
            _ = lager:info("Invalid Marathon response: ~p", [Error]),
            sets:new()
    end.

%% @private
generate_pods_url(LabelSelector) ->
    APIServer = os:getenv("APISERVER"),
    APIServer ++ "/api/v1/pods?labelSelector=" ++ LabelSelector.

%% @private
generate_pod_nodes(#{<<"items">> := Items}) ->
    case Items of
        null ->
            sets:new();
        _ ->
            Nodes = lists:map(fun(Item) ->
                        #{<<"metadata">> := Metadata} = Item,
                        #{<<"name">> := Name} = Metadata,
                        #{<<"status">> := Status} = Item,
                        #{<<"podIP">> := PodIP} = Status,
                        generate_pod_node(Name, PodIP)
                end, Items),
            sets:from_list(Nodes)
    end.

%% @private
generate_pod_node(Name, Host) ->
    {ok, IPAddress} = inet_parse:address(binary_to_list(Host)),
    Port = list_to_integer(os:getenv("PEER_PORT", "9090")),
    {list_to_atom(binary_to_list(Name) ++ "@" ++ binary_to_list(Host)), IPAddress, Port}.

%% @private
get_request(Url, DecodeFun) ->
    Headers = headers(),
    case httpc:request(get, {Url, Headers}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, DecodeFun(Body)};
        Other ->
            _ = lager:info("Request failed; ~p", [Other]),
            {error, invalid}
    end.

%% @private
headers() ->
    Token = os:getenv("TOKEN"),
    [{"Authorization", "Bearer " ++ Token}].
