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

-module(sprinter_mesos).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([clients/0,
         servers/0]).

%% @private
clients() ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    ClientApp = "lasp-client-" ++ integer_to_list(EvalTimestamp),
    app_tasks_from_marathon(ClientApp).

%% @private
servers() ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    ServerApp = "lasp-server-" ++ integer_to_list(EvalTimestamp),
    app_tasks_from_marathon(ServerApp).

%% @private
app_tasks_from_marathon(App) ->
    DecodeFun = fun(Body) -> jsx:decode(Body, [return_maps]) end,

    case get_request(generate_mesos_task_url(App), DecodeFun) of
        {ok, Tasks} ->
            generate_mesos_nodes(Tasks);
        Error ->
            _ = lager:info("Invalid Marathon response: ~p", [Error]),
            sets:new()
    end.

%% @private
dcos() ->
    os:getenv("DCOS", "false").

%% @private
ip() ->
    os:getenv("IP", "127.0.0.1").

%% @private
generate_mesos_task_url(Task) ->
    IP = ip(),
    DCOS = dcos(),
    case DCOS of
        "false" ->
          "http://" ++ IP ++ ":8080/v2/apps/" ++ Task ++ "?embed=app.taskStats";
        _ ->
          DCOS ++ "/marathon/v2/apps/" ++ Task ++ "?embed=app.taskStats"
    end.

%% @doc Generate a list of Erlang node names.
generate_mesos_nodes(#{<<"app">> := App}) ->
    #{<<"tasks">> := Tasks} = App,
    Nodes = lists:map(fun(Task) ->
                #{<<"host">> := Host,
                  <<"ports">> := [_WebPort, PeerPort]} = Task,
        generate_mesos_node(Host, PeerPort)
        end, Tasks),
    sets:from_list(Nodes).

%% @doc Generate a single Erlang node name.
generate_mesos_node(Host, PeerPort) ->
    Name = "lasp-" ++ integer_to_list(PeerPort) ++ "@" ++ binary_to_list(Host),
    {ok, IPAddress} = inet_parse:address(binary_to_list(Host)),
    Node = {list_to_atom(Name), IPAddress, PeerPort},
    Node.

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
    [].
