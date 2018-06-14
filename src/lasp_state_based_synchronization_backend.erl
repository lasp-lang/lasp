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

-module(lasp_state_based_synchronization_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).
-behaviour(lasp_synchronization_backend).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([blocking_sync/1,
         propagate/1]).

%% lasp_synchronization_backend callbacks
-export([extract_log_type_and_payload/1]).

-include("lasp.hrl").

%% State record.
-record(state, {store :: store(),
                gossip_peers :: [],
                actor :: binary(),
                blocking_syncs :: dict:dict()}).

%%%===================================================================
%%% lasp_synchronization_backend callbacks
%%%===================================================================

%% state_based messages:
extract_log_type_and_payload({state_ack, _From, _Id, {_Id, _Type, _Metadata, State}}) ->
    [{state_ack, State}];
extract_log_type_and_payload({state_send, _Node, {Id, Type, _Metadata, State}, _AckRequired}) ->
    [{Id, State}, {Type, State}, {state_send, State}, {state_send_protocol, Id}].

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

propagate(ObjectFilterFun) ->
    gen_server:call(?MODULE, {propagate, ObjectFilterFun}, infinity).

blocking_sync(ObjectFilterFun) ->
    gen_server:call(?MODULE, {blocking_sync, ObjectFilterFun}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([Store, Actor]) ->
    %% Seed the process at initialization.
    ?SYNC_BACKEND:seed(),

    %% Schedule periodic state synchronization.
    case lasp_config:get(blocking_sync, false) of
        true ->
            case partisan_config:get(tag, undefined) of
                server ->
                    schedule_state_synchronization();
                _ ->
                    ok
            end;
        false ->
            schedule_state_synchronization()
    end,

    %% Schedule periodic plumtree refresh.
    schedule_plumtree_peer_refresh(),

    BlockingSyncs = dict:new(),

    {ok, #state{gossip_peers=[],
                blocking_syncs=BlockingSyncs,
                store=Store,
                actor=Actor}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({propagate, ObjectFilterFun}, _From,
            #state{gossip_peers=GossipPeers,
                   store=Store}=State) ->
    %% Get the peers to synchronize with.
    Members = case ?SYNC_BACKEND:broadcast_tree_mode() of
        true ->
            GossipPeers;
        false ->
            {ok, Members1} = ?SYNC_BACKEND:membership(),
            Members1
    end,

    %% Remove ourself and compute exchange peers.
    Peers = ?SYNC_BACKEND:compute_exchange(?SYNC_BACKEND:without_me(Members)),

    case length(Peers) > 0 of
         true ->
            %% Send the object.
            SyncFun = fun(Peer) ->
                            {ok, Os} = init_state_sync(Peer, ObjectFilterFun, true, Store),
                            [{Peer, O} || O <- Os]
                      end,
            lists:flatmap(SyncFun, Peers),

            {reply, ok, State};
        false ->
            % lager:info("No peers, not blocking.", []),
            {reply, ok, State}
    end;

handle_call({blocking_sync, ObjectFilterFun}, From,
            #state{gossip_peers=GossipPeers,
                   store=Store,
                   blocking_syncs=BlockingSyncs0}=State) ->
    %% Get the peers to synchronize with.
    Members = case ?SYNC_BACKEND:broadcast_tree_mode() of
        true ->
            GossipPeers;
        false ->
            {ok, Members1} = ?SYNC_BACKEND:membership(),
            Members1
    end,

    %% Remove ourself and compute exchange peers.
    Peers = ?SYNC_BACKEND:compute_exchange(?SYNC_BACKEND:without_me(Members)),

    case length(Peers) > 0 of
         true ->
            %% Send the object.
            SyncFun = fun(Peer) ->
                            {ok, Os} = init_state_sync(Peer, ObjectFilterFun, true, Store),
                            [{Peer, O} || O <- Os]
                      end,
            Objects = lists:flatmap(SyncFun, Peers),

            %% Mark response as waiting.
            BlockingSyncs = dict:store(From, Objects, BlockingSyncs0),

            % lager:info("Blocking sync initialized for ~p ~p", [From, Objects]),
            {noreply, State#state{blocking_syncs=BlockingSyncs}};
        false ->
            % lager:info("No peers, not blocking.", []),
            {reply, ok, State}
    end;

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.

handle_cast({state_ack, From, Id, {Id, _Type, _Metadata, Value}},
            #state{store=Store,
                   blocking_syncs=BlockingSyncs0}=State) ->
    lager:info("Received ack from ~p for ~p with value ~p", [From, Id, Value]),

    BlockingSyncs = dict:fold(fun(K, V, Acc) ->
                % lager:info("Was waiting ~p ~p", [Key, Value]),
                StillWaiting = lists:delete({From, Id}, V),
                % lager:info("Now waiting ~p ~p", [Key, StillWaiting]),
                case length(StillWaiting) == 0 of
                    true ->
                        %% Bind value locally from server response.
                        %% TODO: Do we have to merge metadata here?
                        {ok, _} = ?CORE:bind_var(Id, Value, Store),

                        %% Reply to return from the blocking operation.
                        gen_server:reply(K, ok),

                        Acc;
                    false ->
                        dict:store(K, StillWaiting, Acc)
                end
              end, dict:new(), BlockingSyncs0),

    lager:info("Finished ack."),

    {noreply, State#state{blocking_syncs=BlockingSyncs}};

handle_cast({state_send, From, {Id, Type, _Metadata, Value}, AckRequired},
            #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("state_send"),

    {ok, Object} = ?CORE:receive_value(Store, {state_send,
                                       From,
                                       {Id, Type, _Metadata, Value},
                                       ?CLOCK_INCR(Actor),
                                       ?CLOCK_INIT(Actor)}),

    lager:info("Receiving updates..."),

    case AckRequired of
        true ->
            lager:info("Ack required, sending responses", []),
            ?SYNC_BACKEND:send(?MODULE, {state_ack, lasp_support:mynode(), Id, Object}, From);
        false ->
            ok
    end,

    %% Send back just the updated state for the object received.
    case ?SYNC_BACKEND:client_server_mode() andalso
         ?SYNC_BACKEND:i_am_server() andalso
         ?SYNC_BACKEND:reactive_server() of
        true ->
            lager:info("Initializing reverse sync...", []),

            ObjectFilterFun = fun(Id1, _) ->
                                      Id =:= Id1
                              end,
            init_state_sync(From, ObjectFilterFun, false, Store),
            ok;
        false ->
            ok
    end,

    lager:info("State sync completed.", []),

    {noreply, State};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.

handle_info({state_sync, ObjectFilterFun},
            #state{store=Store, gossip_peers=GossipPeers} = State) ->
    lasp_marathon_simulations:log_message_queue_size("state_sync"),

    % PeerServiceManager = lasp_config:peer_service_manager(),
    % lasp_logger:extended("Beginning state synchronization: ~p",
    %                      [PeerServiceManager]),

    lager:info("Initializing state sync..."),

    Members = case ?SYNC_BACKEND:broadcast_tree_mode() of
        true ->
            GossipPeers;
        false ->
            {ok, Members1} = ?SYNC_BACKEND:membership(),
            Members1
    end,

    lager:info("=> State sync members: ~p", [Members]),

    %% Remove ourself and compute exchange peers.
    Peers = ?SYNC_BACKEND:compute_exchange(?SYNC_BACKEND:without_me(Members)),

    lager:info("=> Exchange computed: ~p", [Peers]),

    %% Ship buffered updates for the fanout value.
    SyncFun = fun(Peer) ->
                      lager:info("=> About to start for peer: ~p", [Peer]),
                      case lasp_config:get(reverse_topological_sync, ?REVERSE_TOPOLOGICAL_SYNC) of
                          true ->
                              init_reverse_topological_sync(Peer, ObjectFilterFun, Store);
                          false ->
                              lager:info("=> => State sync initialized for peer: ~p", [Peer]),
                              init_state_sync(Peer, ObjectFilterFun, false, Store),
                              lager:info("=> => Out of state sync.")
                      end,
                      lager:info("=> Done for peer: ~p", [Peer])
              end,
    lists:foreach(SyncFun, Peers),

    lager:info("=> Done, scheduling next sync..."),

    %% Schedule next synchronization.
    schedule_state_synchronization(),

    {noreply, State};

handle_info(plumtree_peer_refresh, State) ->
    %% TODO: Temporary hack until the Plumtree can propagate tree
    %% information in the metadata messages.  Therefore, manually poll
    %% periodically with jitter.
    {ok, Servers} = sprinter_backend:servers(),

    GossipPeers = case length(Servers) of
        0 ->
            [];
        _ ->
            Root = hd(Servers),
            plumtree_gossip_peers(Root)
    end,

    %% Schedule next synchronization.
    schedule_plumtree_peer_refresh(),

    {noreply, State#state{gossip_peers=GossipPeers}};

handle_info(Msg, State) ->
    lager:warning("Unhandled info messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
schedule_state_synchronization() ->
    ShouldSync = true
            andalso (not ?SYNC_BACKEND:tutorial_mode())
            andalso (
              ?SYNC_BACKEND:peer_to_peer_mode()
              orelse
              (
               ?SYNC_BACKEND:client_server_mode()
               andalso
               not (?SYNC_BACKEND:i_am_server() andalso ?SYNC_BACKEND:reactive_server())
              )
            ),

    case ShouldSync of
        true ->
            Interval = lasp_config:get(state_interval, 10000),
            ObjectFilterFun = fun(_, _) -> true end,
            case lasp_config:get(jitter, false) of
                true ->
                    %% Add random jitter.
                    JitterPercent = lasp_config:get(jitter_percent, 1) * 0.01,
                    MinimalInterval = round(Interval * JitterPercent),

                    case MinimalInterval of
                        0 ->
                            %% No jitter.
                            timer:send_after(Interval, {state_sync, ObjectFilterFun});
                        JitterInterval ->
                            Jitter = rand_compat:uniform(JitterInterval * 2) - JitterInterval,
                            timer:send_after(Interval + Jitter, {state_sync, ObjectFilterFun})
                    end;
                false ->
                    timer:send_after(Interval, {state_sync, ObjectFilterFun})
            end;
        false ->
            ok
    end.

%% @private
schedule_plumtree_peer_refresh() ->
    case ?SYNC_BACKEND:broadcast_tree_mode() of
        true ->
            Interval = lasp_config:get(plumtree_peer_refresh_interval,
                                       ?PLUMTREE_PEER_REFRESH_INTERVAL),
            Jitter = rand_compat:uniform(Interval),
            timer:send_after(Jitter + ?PLUMTREE_PEER_REFRESH_INTERVAL,
                             plumtree_peer_refresh);
        false ->
            ok
    end.

%% @private
init_reverse_topological_sync(Peer, ObjectFilterFun, Store) ->
    % lasp_logger:extended("Initializing reverse toplogical state synchronization with peer: ~p", [Peer]),

    PeerInterests = get_peer_interests(Peer, Store),

    SendFun = fun({Id, #dv{type=Type, metadata=Metadata, value=Value}}) ->
                    Dynamic = is_dynamic(Metadata),
                    Filtered = is_filtered(PeerInterests, Metadata),

                    %% Sync as long as it's not dynamically scoped, and is filtered.
                    ShouldSync = not Dynamic andalso Filtered,

                    case ShouldSync of
                        false ->
                            %% Ignore: this is a dynamic variable.
                            ok;
                        true ->
                            case ObjectFilterFun(Id, Metadata) of
                                true ->
                                    ?SYNC_BACKEND:send(?MODULE, {state_send, lasp_support:mynode(), {Id, Type, Metadata, Value}, false}, Peer);
                                false ->
                                    ok
                            end
                    end
               end,

    SyncFun = fun({{_, _Type} = Id, _Depth}) ->
                      {ok, Object} = lasp_storage_backend:get(Store, Id),
                      SendFun({Id, Object});
                 (_) ->
                      ok
              end,

    {ok, Vertices} = lasp_dependence_dag:vertices(),

    SortFun = fun({_, A}, {_, B}) ->
                      A > B;
                 (_, _) ->
                      true
              end,
    SortedVertices = lists:sort(SortFun, Vertices),

    lists:map(SyncFun, SortedVertices),

    % lasp_logger:extended("Completed back propagation state synchronization with peer: ~p", [Peer]),

    ok.

%% @private
get_peer_interests(Peer, Store) ->
    case partisan_config:get(use_peer_interests, false) of
        false ->
            sets:new();
        true ->
            case ?CORE:query(?INTERESTS_ID, Store) of
                {ok, Value} ->
                    lager:info("Got value: ~p", [Value]),
                    Result = proplists:get_value(Peer, Value, sets:new()),
                    lager:info("Returning result: ~p", [Result]),
                    Result;
                Other ->
                    lager:warning("Got invalid value: ~p", [Other]),
                    sets:new()
            end
    end.

%% @private
init_state_sync(Peer, ObjectFilterFun, Blocking, Store) ->
    lager:info("Initializing state propagation with peer: ~p", [Peer]),

    lager:info("Getting peer interests for peer: ~p", [Peer]),
    PeerInterests = get_peer_interests(Peer, Store),
    lager:info("=> PeerInterests for peer ~p are ~p", [Peer, PeerInterests]),

    Function = fun({Id, #dv{type=Type, metadata=Metadata, value=Value}}, Acc0) ->
                    lager:info("Processing id: ~p ~p", [Id, Value]),
                    Dynamic = is_dynamic(Metadata),
                    Filtered = is_filtered(PeerInterests, Metadata),

                    %% Sync as long as it's not dynamically scoped, and is filtered.
                    ShouldSync = not Dynamic andalso Filtered,
                    lager:info("=> fold, is shouldsync: ~p", [ShouldSync]),

                    case ShouldSync of
                        true ->
                            lager:info("=> Should sync ~p ~p", [Id, Value]),
                            try ObjectFilterFun(Id, Metadata) of
                                true ->
                                    lager:info("=> Passed filter ~p ~p", [Id, Value]),
                                    ?SYNC_BACKEND:send(?MODULE, {state_send, lasp_support:mynode(), {Id, Type, Metadata, Value}, Blocking}, Peer),
                                    [Id|Acc0];
                                false ->
                                    lager:info("=> DID NOT pass filter ~p ~p", [Id, Value]),
                                    Acc0
                            catch 
                                _:Error ->
                                    lager:info("=> Exception!! ~p ~p ~p", [Id, Value, Error]),
                                    case ObjectFilterFun(Id) of
                                        true ->
                                            lager:info("=> Passed filter ~p", [Id]),
                                            ?SYNC_BACKEND:send(?MODULE, {state_send, lasp_support:mynode(), {Id, Type, Metadata, Value}, Blocking}, Peer),
                                            [Id|Acc0];
                                        false ->
                                            lager:info("=> DID NOT filter ~p", [Id]),
                                            Acc0
                                    end
                            end;
                        false ->
                            Acc0
                    end
               end,
    lager:info("=> Starting backend fold at backend...", []),
    Trace = try throw(42) catch 42 -> erlang:get_stacktrace() end,
    lager:info("~p", [Trace]),
    %% TODO: Should this be parallel?
    {ok, Objects} = lasp_storage_backend:fold(Store, Function, []),
    lager:info("Completed state propagation with peer: ~p", [Peer]),
    {ok, Objects}.

%% @private
plumtree_gossip_peers(Root) ->
    {ok, Nodes} = sprinter_backend:nodes(),
    Tree = sprinter_backend:debug_get_tree(Root, Nodes),
    FolderFun = fun({Node, Peers}, In) ->
                        case Peers of
                            down ->
                                In;
                            {Eager, _Lazy} ->
                                case lists:member(lasp_support:mynode(), Eager) of
                                    true ->
                                        In ++ [Node];
                                    false ->
                                        In
                                end
                        end
                end,
    InLinks = lists:foldl(FolderFun, [], Tree),

    {EagerPeers, _LazyPeers} = plumtree_broadcast:debug_get_peers(lasp_support:mynode(), Root),
    OutLinks = ordsets:to_list(EagerPeers),

    GossipPeers = lists:usort(InLinks ++ OutLinks),
    lager:info("PLUMTREE DEBUG: Gossip Peers: ~p", [GossipPeers]),

    GossipPeers.

%% @private
is_filtered(PeerInterests, Metadata) ->
    ObjectTopics = case orddict:find(topics, Metadata) of
        {ok, T} ->
            ObjectInterestType = lasp_type:get_type(?OBJECT_INTERESTS_TYPE),
            {ok, Topics} = ObjectInterestType:query(T),
            Topics;
        _ ->
            sets:new()
    end,

    case sets:size(ObjectTopics) > 0 andalso sets:size(PeerInterests) > 0 of
        true ->
            %% If the node has interests, and the object is on a topic, only allow 
            %% if they are not disjoint.
            not sets:is_disjoint(ObjectTopics, PeerInterests);
        false ->
            %% Otherwise, send.
            true
    end.

%% @private
is_dynamic(Metadata) ->
    case orddict:find(dynamic, Metadata) of
        {ok, true} ->
            true;
        _ ->
            false
    end.