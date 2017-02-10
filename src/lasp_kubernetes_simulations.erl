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

-module(lasp_kubernetes_simulations).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([stop/0]).

stop() ->
    lists:foreach(
        fun(Deployment) ->
            lager:info("Deleting Kubernetes deployment: ~p", [Deployment]),
            delete_deployment(Deployment),
            lager:info("Deleting Kubernetes pods."),
            delete_pods(Deployment)
        end,
        deployments()).

%% @private
delete_deployment(Deployment) ->
    DecodeFun = fun(Body) -> jsx:decode(Body, [return_maps]) end,
    DeploymentURL = deployment_url(Deployment),

    case delete_request(DeploymentURL, DecodeFun) of
        {ok, Response} ->
            _ = lager:info("Response: ~p", [Response]),
            ok;
        Error ->
            _ = lager:info("Invalid Kubernetes response: ~p", [Error]),
            {error, Error}
    end.

%% @private
deployment_url(Deployment) ->
    APIServer = os:getenv("APISERVER"),
    APIServer ++ "/apis/extensions/v1beta1/namespaces/default/deployments/" ++ Deployment.

%% @private
delete_pod(#{<<"metadata">> := Metadata}) ->
    DecodeFun = fun(Body) -> jsx:decode(Body, [return_maps]) end,
    #{<<"selfLink">> := SelfUrl} = Metadata,

    APIServer = os:getenv("APISERVER"),
    PodUrl = APIServer ++ SelfUrl,

    case delete_request(PodUrl, DecodeFun) of
        {ok, Response} ->
            _ = lager:info("Response: ~p", [Response]),
            ok;
        Error ->
            _ = lager:info("Invalid Kubernetes response: ~p", [Error]),
            {error, Error}
    end.

%% @private
delete_pods(Run) ->
    DecodeFun = fun(Body) -> jsx:decode(Body, [return_maps]) end,

    APIServer = os:getenv("APISERVER"),
    PodsUrl = APIServer ++ "/api/v1/pods?labelSelector=run%3D" ++ Run,

    case get_request(PodsUrl, DecodeFun) of
        {ok, Response} ->
            _ = lager:info("Response: ~p", [Response]),
            #{<<"items">> := Items} = Response,
            [delete_pod(Item) || Item <- Items],
            ok;
        Error ->
            _ = lager:info("Invalid Kubernetes response: ~p", [Error]),
            {error, Error}
    end.

%% @private
deployments() ->
    ["lasp-server", "lasp-client", "redis"].

%% @private
get_request(Url, DecodeFun) ->
    lager:info("Issuing GET request to: ~p", [Url]),

    Headers = headers(),
    case httpc:request(get, {Url, Headers}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, DecodeFun(Body)};
        Other ->
            _ = lager:info("Request failed; ~p", [Other]),
            {error, invalid}
    end.

%% @private
delete_request(Url, DecodeFun) ->
    lager:info("Issuing DELETE request to: ~p", [Url]),

    Headers = headers(),
    case httpc:request(delete, {Url, Headers}, [], [{body_format, binary}]) of
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
