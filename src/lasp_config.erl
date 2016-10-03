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

-module(lasp_config).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-export([dispatch/0,
         set/2,
         get/2,
         peer_service_manager/0,
         web_config/0]).

get(Key, Default) ->
    mochiglobal:get(Key, Default).

set(Key, Value) ->
    application:set_env(?APP, Key, Value),
    mochiglobal:put(Key, Value).

dispatch() ->
    lists:flatten([
        {["api", "kv", id, type],   lasp_kv_resource,           undefined},
        {["api", "plots"],          lasp_plots_resource,        undefined},
        {["api", "logs"],           lasp_logs_resource,         undefined},
        {["api", "health"],         lasp_health_check_resource, undefined},
        {["api", "status"],         lasp_status_resource,       undefined},
        {["api", "dag"],            lasp_dag_resource,          undefined},
        {[],                        lasp_gui_resource,          index},
        {['*'],                     lasp_gui_resource,          undefined}
    ]).

web_config() ->
    {ok, App} = application:get_application(?MODULE),
    {ok, Ip} = application:get_env(App, web_ip),
    Port = lasp_config:get(web_port, 8080),
    Config = [
        {ip, Ip},
        {port, Port},
        {log_dir, "priv/log"},
        {dispatch, dispatch()}
    ],
    Node = node(),
    lager:info("Node ~p enabling web configuration: ~p", [Node, Config]),
    Config.

%% @private
peer_service_manager() ->
    partisan_config:get(partisan_peer_service_manager,
                        partisan_default_peer_service_manager).
