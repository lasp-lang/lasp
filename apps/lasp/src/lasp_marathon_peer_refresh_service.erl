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

%% @doc Read configuration information from Marathon and auto-cluster
%%      Erlang nodes based on this.

-module(lasp_marathon_peer_refresh_service).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

-define(REFRESH_INTERVAL, 1000).
-define(REFRESH_MESSAGE,  refresh).

-define(NODES_INTERVAL, 20000).
-define(NODES_MESSAGE,  nodes).

%% State record.
-record(state, {nodes = [] :: [node()]}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Don't start the timer if we're not running in Mesos.
    case os:getenv("MESOS_TASK_ID") of
        false ->
            ok;
        _ ->
            %% @todo Stall messages, because Plumtree has a race on startup, again.
            timer:send_after(?NODES_INTERVAL, ?NODES_MESSAGE),
            timer:send_after(?REFRESH_INTERVAL, ?REFRESH_MESSAGE)
    end,
    {ok, #state{}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(?REFRESH_MESSAGE, #state{nodes=SeenNodes}=State) ->
    timer:send_after(?REFRESH_INTERVAL, ?REFRESH_MESSAGE),

    Nodes = case request() of
        {ok, Response} ->
            generate_nodes(Response);
        _ ->
            SeenNodes
    end,

    %% Attempt to connect nodes that are not connected.
    ConnectedNodes = maybe_connect(Nodes, SeenNodes),

    {noreply, State#state{nodes=ConnectedNodes}};
handle_info(?NODES_MESSAGE, State) ->
    timer:send_after(?NODES_INTERVAL, ?NODES_MESSAGE),
    lager:info("Currently connected nodes via distributed erlang: ~p",
               [nodes()]),
    lager:info("Currently connected nodes via Lasp peer service: ~p",
               [lasp_peer_service:members()]),
    {noreply, State};
handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Generate a list of Erlang node names.
generate_nodes(#{<<"app">> := App}) ->
    #{<<"tasks">> := Tasks} = App,
    [generate_node(Host, Port)
     || #{<<"host">> := Host, <<"ports">> := [Port]} <- Tasks].

%% @doc Generate a single Erlang node name.
generate_node(Host, Port) ->
    Name = "lasp-" ++ integer_to_list(Port) ++ "@" ++ binary_to_list(Host),
    list_to_atom(Name).

%% @doc Attempt to connect disconnected nodes.
%% @private
maybe_connect(Nodes, SeenNodes) ->
    ToConnect = Nodes -- (SeenNodes ++ [node()]),

    %% Attempt connection.
    Attempted = case ToConnect of
        [] ->
            [];
        _ ->
            lists:map(fun connect/1, ToConnect)
    end,

    %% Log the output of the attempt.
    case Attempted of
        [] ->
            ok;
        _ ->
            lager:info("Attempted to connect: ~p", [Attempted])
    end,

    %% Return list of connected nodes.
    nodes().

%% @private
connect(Node) ->
    Ping = net_adm:ping(Node),
    case Ping of
        pang ->
            ok;
        pong ->
            lasp_peer_service:join(Node)
    end,
    {Node, Ping}.

%% @private
request() ->
    IP = os:getenv("IP", "127.0.0.1"),
    Url = "http://" + IP + ":8080/v2/apps/lasp?embed=app.taskStats",
    case httpc:request(get, {Url, []}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, jsx:decode(Body, [return_maps])};
        Other ->
            Other
    end.
