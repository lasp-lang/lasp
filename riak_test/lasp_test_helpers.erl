%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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

%% @doc Helpers for remotely running code.

-module(lasp_test_helpers).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([load/1,
         wait_for_cluster/1]).

-export([build_clusters/1]).

%% @doc Ensure cluster is properly configured.
wait_for_cluster(Nodes) ->
    PeerService = lasp_peer_service:peer_service(),
    case PeerService of
        lasp_riak_core_peer_service ->
            lager:info("Waiting for transfers to complete."),
            ok = rt:wait_until_transfers_complete(Nodes),
            lager:info("Transfers complete."),
            ok;
        _ ->
            ok
    end.

%% @doc Remotely load test code on a series of nodes.
load(Nodes) when is_list(Nodes) ->
    _ = [ok = load(Node) || Node <- Nodes],
    ok;
load(Node) ->
    TestGlob = rt_config:get(tests_to_remote_load, undefined),
    case TestGlob of
        undefined ->
            ok;
        TestGlob ->
            Tests = filelib:wildcard(TestGlob),

            Directory = case file:get_cwd() of
                {ok, Dir} ->
                    Dir;
                _ ->
                    exit("Failed to get working directory!")
            end,

            [ok = remote_compile_and_load(Node,
                                          Directory ++ "/" ++ Test) || Test <- Tests],
            ok
    end.

%% @doc Remotely compile and load a test on a given node.
remote_compile_and_load(Node, F) ->
    {ok, _, Bin} = rpc:call(Node, compile, file,
                            [F, [binary,
                                 {parse_transform, lager_transform}]]),
    ModName = list_to_atom(filename:basename(F, ".erl")),
    {module, _} = rpc:call(Node, code, load_binary, [ModName, F, Bin]),
    ok.

-include_lib("eunit/include/eunit.hrl").

%% @doc Build a series of cluster.
build_clusters(Settings) ->
    Clusters = deploy_clusters(Settings),
    [begin
         ok = join_cluster(Nodes),
         lager:info("Cluster built: ~p", [Nodes])
     end || Nodes <- Clusters],
    Clusters.

%% @private
join_cluster(Nodes) ->
    PeerService = lasp_peer_service:peer_service(),

    case PeerService of
        lasp_riak_core_peer_service ->
            %% Ensure each node owns 100% of it's own ring
            [?assertEqual([Node], rt:owners_according_to(Node)) || Node <- Nodes],

            %% Join nodes
            [Node1|OtherNodes] = Nodes,
            case OtherNodes of
                [] ->
                    %% no other nodes, nothing to join/plan/commit
                    ok;
                _ ->
                    %% ok do a staged join and then commit it, this eliminates the
                    %% large amount of redundant handoff done in a sequential join
                    [rt:staged_join(Node, Node1) || Node <- OtherNodes],
                     rt:plan_and_commit(Node1),
                     rt:try_nodes_ready(Nodes, 3, 500)
            end,

            %% Wait until nodes are ready.
            ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),

            %% Ensure each node owns a portion of the ring
            rt:wait_until_nodes_agree_about_ownership(Nodes),
            ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),
            ok;
        lasp_plumtree_peer_service ->
            %% Ensure plumtree has started.
            lager:info("Wait until nodes are ready : ~p", [Nodes]),
            [?assertEqual(ok, rt:wait_until(Node, fun plumtree_started/1)) || Node <- Nodes],

            %% Join nodes
            [Node1|OtherNodes] = Nodes,
            case OtherNodes of
                [] ->
                    %% no other nodes, nothing to join/plan/commit
                    ok;
                _ ->
                    %% ok do a staged join and then commit it, this eliminates the
                    %% large amount of redundant handoff done in a sequential join
                    [peer_service_join(PeerService, Node, Node1) || Node <- OtherNodes]
            end,
            ok;
        Unknown ->
            lager:error("Unknown peer service: ~p", [Unknown]),
            {error, unknown_peer_service}
    end.

%% @private
plumtree_started(Node) ->
    Applications = rpc:call(Node, application, which_applications, []),
    case lists:keyfind(plumtree, 1, Applications) of
        false ->
            false;
        _ ->
            true
    end.

%% @private
peer_service_join(PeerService, Node, PNode) ->
    R = rpc:call(Node, PeerService, join, [PNode]),
    lager:info("[join] ~p to (~p): ~p", [Node, PNode, R]),
    ?assertEqual(ok, R),
    ok.

%% @private
deploy_clusters(Settings) ->
    ClusterConfigs = [case Setting of
                          Configs when is_list(Configs) ->
                              Configs;
                          NumNodes when is_integer(NumNodes) ->
                              [{current, default} || _ <- lists:seq(1, NumNodes)];
                          {NumNodes, InitialConfig} when is_integer(NumNodes) ->
                              [{current, InitialConfig} || _ <- lists:seq(1,NumNodes)];
                          {NumNodes, Vsn, InitialConfig} when is_integer(NumNodes) ->
                              [{Vsn, InitialConfig} || _ <- lists:seq(1,NumNodes)]
                      end || Setting <- Settings],
    NumNodes = rt_config:get(num_nodes, 6),
    RequestedNodes = lists:flatten(ClusterConfigs),

    case length(RequestedNodes) > NumNodes of
        true ->
            erlang:error("Requested more nodes than available");
        false ->
            Nodes = deploy_nodes(RequestedNodes),
            {DeployedClusters, _} = lists:foldl(
                    fun(Cluster, {Clusters, RemNodes}) ->
                        {A, B} = lists:split(length(Cluster), RemNodes),
                        {Clusters ++ [A], B}
                end, {[], Nodes}, ClusterConfigs),
            DeployedClusters
    end.

-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(PATH, (rt_config:get(rtdev_path))).

%% @private
deploy_nodes(NodeConfig) ->
    Path = relpath(root),
    lager:info("Riak path: ~p", [Path]),
    NumNodes = length(NodeConfig),
    NodesN = lists:seq(1, NumNodes),
    Nodes = [?DEV(N) || N <- NodesN],
    NodeMap = orddict:from_list(lists:zip(Nodes, NodesN)),
    {Versions, Configs} = lists:unzip(NodeConfig),
    VersionMap = lists:zip(NodesN, Versions),

    %% Check that you have the right versions available
    [ check_node(Version) || Version <- VersionMap ],
    rt_config:set(rt_nodes, NodeMap),
    rt_config:set(rt_versions, VersionMap),

    create_dirs(Nodes),

    %% Set initial config
    add_default_node_config(Nodes),
    rt:pmap(fun({_, default}) ->
                    ok;
               ({Node, {cuttlefish, Config}}) ->
                    set_conf(Node, Config);
               ({Node, Config}) ->
                    update_app_config(Node, Config)
            end,
            lists:zip(Nodes, Configs)),

    %% create snmp dirs, for EE
    create_dirs(Nodes),

    %% Start nodes
    rt:pmap(fun(N) -> run_riak(N, relpath(node_version(N)), "start") end, NodesN),

    %% Ensure nodes started
    [ok = rt:wait_until_pingable(N) || N <- Nodes],

    %% %% Enable debug logging
    %% [rpc:call(N, lager, set_loglevel, [lager_console_backend, debug]) || N <- Nodes],

    %% We have to make sure that riak_core_ring_manager is running before we can go on.
    % [ok = rt:wait_until_registered(N, riak_core_ring_manager) || N <- Nodes],

    %% Ensure nodes are singleton clusters
    % [ok = rt:check_singleton_node(?DEV(N)) || {N, Version} <- VersionMap,
    %                                           Version /= "0.14.2"],

    lager:info("Deployed nodes: ~p", [Nodes]),
    Nodes.

%% @private
check_node({_N, Version}) ->
    case proplists:is_defined(Version, rt_config:get(rtdev_path)) of
        true -> ok;
        _ ->
            lager:error("You don't have Riak ~s installed or configured", [Version]),
            erlang:error(lists:flatten(io_lib:format("You don't have Riak ~p installed or configured", [Version])))
    end.

%% @private
create_dirs(Nodes) ->
    Snmp = [node_path(Node) ++ "/data/snmp/agent/db" || Node <- Nodes],
    [?assertCmd("mkdir -p " ++ Dir) || Dir <- Snmp].

%% @private
node_path(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    lists:flatten(io_lib:format("~s/dev/dev~b", [Path, N])).

%% @private
node_id(Node) ->
    NodeMap = rt_config:get(rt_nodes),
    orddict:fetch(Node, NodeMap).

%% @private
node_version(N) ->
    VersionMap = rt_config:get(rt_versions),
    orddict:fetch(N, VersionMap).

%% @private
relpath(Vsn) ->
    Path = ?PATH,
    relpath(Vsn, Path).

%% @private
relpath(Vsn, Paths=[{_,_}|_]) ->
    orddict:fetch(Vsn, orddict:from_list(Paths));
relpath(current, Path) ->
    Path;
relpath(root, Path) ->
    Path;
relpath(_, _) ->
    throw("Version requested but only one path provided").

%% @private
run_riak(N, Path, Cmd) ->
    lager:info("Running: ~s", [riakcmd(Path, N, Cmd)]),
    R = os:cmd(riakcmd(Path, N, Cmd)),
    case Cmd of
        "start" ->
            rt_cover:maybe_start_on_node(?DEV(N), node_version(N)),
            %% Intercepts may load code on top of the cover compiled
            %% modules. We'll just get no coverage info then.
            case rt_intercept:are_intercepts_loaded(?DEV(N)) of
                false ->
                    ok = rt_intercept:load_intercepts([?DEV(N)]);
                true ->
                    ok
            end,
            R;
        "stop" ->
            rt_cover:maybe_stop_on_node(?DEV(N)),
            R;
        _ ->
            R
    end.

%% @private
riakcmd(Path, N, Cmd) ->
    ExecName = rt_config:get(exec_name, "riak"),
    io_lib:format("~s/dev/dev~b/bin/~s ~s", [Path, N, ExecName, Cmd]).

%% @private
add_default_node_config(Nodes) ->
    case rt_config:get(rt_default_config, undefined) of
        undefined -> ok;
        Defaults when is_list(Defaults) ->
            rt:pmap(fun(Node) ->
                            update_app_config(Node, Defaults)
                    end, Nodes),
            ok;
        BadValue ->
            lager:error("Invalid value for rt_default_config : ~p", [BadValue]),
            throw({invalid_config, {rt_default_config, BadValue}})
    end.

%% @private
update_app_config(all, Config) ->
    lager:info("rtdev:update_app_config(all, ~p)", [Config]),
    [ update_app_config(DevPath, Config) || DevPath <- devpaths()];
update_app_config(Node, Config) when is_atom(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    FileFormatString = "~s/dev/dev~b/etc/~s.config",

    AppConfigFile = io_lib:format(FileFormatString, [Path, N, "app"]),
    AdvConfigFile = io_lib:format(FileFormatString, [Path, N, "advanced"]),
    %% If there's an app.config, do it old style
    %% if not, use cuttlefish's adavnced.config
    case filelib:is_file(AppConfigFile) of
        true ->
            update_app_config_file(AppConfigFile, Config);
        _ ->
            update_app_config_file(AdvConfigFile, Config)
    end;
update_app_config(DevPath, Config) ->
    [update_app_config_file(AppConfig, Config) || AppConfig <- all_the_app_configs(DevPath)].

%% @private
update_app_config_file(ConfigFile, Config) ->
    lager:info("rtdev:update_app_config_file(~s, ~p)", [ConfigFile, Config]),

    BaseConfig = case file:consult(ConfigFile) of
        {ok, [ValidConfig]} ->
            ValidConfig;
        {error, enoent} ->
            []
    end,
    MergeA = orddict:from_list(Config),
    MergeB = orddict:from_list(BaseConfig),
    NewConfig =
        orddict:merge(fun(_, VarsA, VarsB) ->
                              MergeC = orddict:from_list(VarsA),
                              MergeD = orddict:from_list(VarsB),
                              orddict:merge(fun(_, ValA, _ValB) ->
                                                    ValA
                                            end, MergeC, MergeD)
                      end, MergeA, MergeB),
    NewConfigOut = io_lib:format("~p.", [NewConfig]),
    ?assertEqual(ok, file:write_file(ConfigFile, NewConfigOut)),
    ok.

%% @private
devpaths() ->
    lists:usort([ DevPath || {_Name, DevPath} <- proplists:delete(root, rt_config:get(rtdev_path))]).

%% @private
set_conf(all, NameValuePairs) ->
    lager:info("rtdev:set_conf(all, ~p)", [NameValuePairs]),
    [ set_conf(DevPath, NameValuePairs) || DevPath <- devpaths()],
    ok;
set_conf(Node, NameValuePairs) when is_atom(Node) ->
    append_to_conf_file(get_riak_conf(Node), NameValuePairs),
    ok;
set_conf(DevPath, NameValuePairs) ->
    [append_to_conf_file(RiakConf, NameValuePairs) || RiakConf <- all_the_files(DevPath, "etc/riak.conf")],
    ok.

%% @private
append_to_conf_file(File, NameValuePairs) ->
    Settings = lists:flatten(
        [io_lib:format("~n~s = ~s~n", [Name, Value]) || {Name, Value} <- NameValuePairs]),
    file:write_file(File, Settings, [append]).

%% @private
get_riak_conf(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    io_lib:format("~s/dev/dev~b/etc/riak.conf", [Path, N]).

%% @private
all_the_files(DevPath, File) ->
    case filelib:is_dir(DevPath) of
        true ->
            Wildcard = io_lib:format("~s/dev/dev*/~s", [DevPath, File]),
            filelib:wildcard(Wildcard);
        _ ->
            lager:debug("~s is not a directory.", [DevPath]),
            []
    end.

%% @private
all_the_app_configs(DevPath) ->
    AppConfigs = all_the_files(DevPath, "etc/app.config"),
    case length(AppConfigs) =:= 0 of
        true ->
            AdvConfigs = filelib:wildcard(DevPath ++ "/dev/dev*/etc"),
            [ filename:join(AC, "advanced.config") || AC <- AdvConfigs];
        _ ->
            AppConfigs
    end.
