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

%% @doc Read configuration information from Mesos DNS and auto-cluster
%%      Erlang nodes based on this.

-module(lasp_peer_refresh_service).
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

-define(NODES_INTERVAL, 5000).
-define(NODES_MESSAGE,  nodes).

%% @todo Fix me to make me get my information from mesos.
-define(OPTIONS,  [{nameservers, [{{10,141,141,10}, 53}]}]).

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
            timer:send_after(0, ?NODES_MESSAGE),
            timer:send_after(0, ?REFRESH_MESSAGE)
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

    Nodes = case resolve() of
        {ok, DnsMsg} ->
            lists:usort(generate_nodes(DnsMsg));
        _Error ->
            SeenNodes
    end,

    %% Attempt to connect nodes that are not connected.
    ConnectedNodes = maybe_connect(Nodes, SeenNodes),

    {noreply, State#state{nodes=ConnectedNodes}};
handle_info(?NODES_MESSAGE, State) ->
    timer:send_after(?NODES_INTERVAL, ?NODES_MESSAGE),
    lager:info("Currently connected nodes: ~p", [nodes()]),
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

%% @doc Generate a list of Erlang node names from DNS response.
generate_nodes(DnsMsg) ->
    ArList = inet_dns:msg(DnsMsg, arlist),
    AnList = inet_dns:msg(DnsMsg, anlist),
    Additionals = parse_additionals(ArList),
    lists:map(fun(DnsRr) -> generate_node(DnsRr, Additionals) end, AnList).

%% @doc Generate a single Erlang node name from DNS records.
generate_node(DnsRr, Additionals) ->
    {_, _, Port, Host} = inet_dns:rr(DnsRr, data),
    {Host, IP} = lists:keyfind(Host, 1, Additionals),
    Name = "lasp-" ++ integer_to_list(Port) ++ "@" ++ inet:ntoa(IP),
    list_to_atom(Name).

%% @private
parse_additionals(ArList) ->
    lists:map(fun(DnsRr) ->
                      Domain = inet_dns:rr(DnsRr, domain),
                      Data = inet_dns:rr(DnsRr, data),
                      {Domain, Data}
              end, ArList).

%% @private
resolve() ->
    inet_res:resolve("_lasp._tcp.marathon.mesos", any, srv, ?OPTIONS).

%% @doc Attempt to connect disconnected nodes.
%% @private
maybe_connect(Nodes, SeenNodes) ->
    ToConnect = Nodes -- (SeenNodes ++ [node()]),

    %% Attempt connection.
    Attempted = case ToConnect of
        [] ->
            [];
        _ ->
            lists:map(fun(Node) -> {Node, net_adm:ping(Node)} end, ToConnect)
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
