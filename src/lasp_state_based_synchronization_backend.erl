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

%% lasp_synchronization_backend callbacks
-export([extract_log_type_and_payload/1]).

-include("lasp.hrl").

%% State record.
-record(state, {store :: store(), gossip_peers :: [], actor :: binary()}).

%%%===================================================================
%%% lasp_synchronization_backend callbacks
%%%===================================================================

%% state_based messages:
extract_log_type_and_payload({state_send, _Node, {Id, Type, _Metadata, State}}) ->
    [{Id, State}, {Type, State}, {state_send, State}, {state_send_protocol, Id}].

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([Store, Actor]) ->
    %% Seed the process at initialization.
    ?SYNC_BACKEND:seed(),

    %% Schedule periodic state synchronization.
    schedule_state_synchronization(),

    %% Schedule periodic plumtree refresh.
    schedule_plumtree_peer_refresh(),

    {ok, #state{gossip_peers=[], store=Store, actor=Actor}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.

handle_cast({state_send, From, {Id, Type, _Metadata, Value}},
            #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("state_send"),

    ?CORE:receive_value(Store, {state_send,
                                From,
                               {Id, Type, _Metadata, Value},
                               ?CLOCK_INCR(Actor),
                               ?CLOCK_INIT(Actor)}),

    %% Send back just the updated state for the object received.
    case ?SYNC_BACKEND:client_server_mode() andalso
         ?SYNC_BACKEND:i_am_server() andalso
         ?SYNC_BACKEND:reactive_server() of
        true ->
            ObjectFilterFun = fun(Id1) ->
                                      Id =:= Id1
                              end,
            init_state_sync(From, ObjectFilterFun, Store);
        false ->
            ok
    end,

    {noreply, State};

handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.

handle_info(state_sync, #state{store=Store, gossip_peers=GossipPeers} = State) ->
    lasp_marathon_simulations:log_message_queue_size("state_sync"),

    PeerServiceManager = lasp_config:peer_service_manager(),
    lasp_logger:extended("Beginning state synchronization with backend: ~p",
                         [PeerServiceManager]),

    Members = case ?SYNC_BACKEND:broadcast_tree_mode() of
        true ->
            GossipPeers;
        false ->
            {ok, Members1} = ?SYNC_BACKEND:membership(),
            Members1
    end,

    %% Remove ourself and compute exchange peers.
    Peers = ?SYNC_BACKEND:compute_exchange(?SYNC_BACKEND:without_me(Members)),

    lasp_logger:extended("Beginning sync for peers: ~p", [Peers]),

    %% Ship buffered updates for the fanout value.
    lists:foreach(fun(Peer) -> init_state_sync(Peer, Store) end, Peers),

    %% Schedule next synchronization.
    schedule_state_synchronization(),

    {noreply, State};

handle_info(plumtree_peer_refresh, State) ->
    %% TODO: Temporary hack until the Plumtree can propagate tree
    %% information in the metadata messages.  Therefore, manually poll
    %% periodically with jitter.
    {ok, Servers} = sprinter:servers(),

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
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
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
            case lasp_config:get(jitter, false) of
                true ->
                    %% Add random jitter.
                    Jitter = rand_compat:uniform(Interval),
                    timer:send_after(Interval + Jitter, state_sync);
                false ->
                    timer:send_after(Interval, state_sync)
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
init_state_sync(Peer, Store) ->
    ObjectFilterFun = fun(_) -> true end,
    init_state_sync(Peer, ObjectFilterFun, Store).

%% @private
init_state_sync(Peer, ObjectFilterFun, Store) ->
    lasp_logger:extended("Initializing state synchronization with peer: ~p", [Peer]),
    Function = fun({Id, #dv{type=Type, metadata=Metadata, value=Value}}, Acc0) ->
                    case orddict:find(dynamic, Metadata) of
                        {ok, true} ->
                            %% Ignore: this is a dynamic variable.
                            Acc0;
                        _ ->
                            case ObjectFilterFun(Id) of
                                true ->
                                    ?SYNC_BACKEND:send(?MODULE, {state_send, node(), {Id, Type, Metadata, Value}}, Peer),
                                    [{ok, {Id, Type, Metadata, Value}}|Acc0];
                                false ->
                                    Acc0
                            end
                    end
               end,
    %% TODO: Should this be parallel?
    {ok, _} = lasp_storage_backend:do(fold, [Store, Function, []]),
    lasp_logger:extended("Completed state synchronization with peer: ~p", [Peer]),
    ok.

%% @private
plumtree_gossip_peers(Root) ->
    {ok, Nodes} = sprinter:nodes(),
    Tree = sprinter:debug_get_tree(Root, Nodes),
    FolderFun = fun({Node, Peers}, In) ->
                        case Peers of
                            down ->
                                In;
                            {Eager, _Lazy} ->
                                case lists:member(node(), Eager) of
                                    true ->
                                        In ++ [Node];
                                    false ->
                                        In
                                end
                        end
                end,
    InLinks = lists:foldl(FolderFun, [], Tree),

    {EagerPeers, _LazyPeers} = plumtree_broadcast:debug_get_peers(node(), Root),
    OutLinks = ordsets:to_list(EagerPeers),

    GossipPeers = lists:usort(InLinks ++ OutLinks),
    lager:info("PLUMTREE DEBUG: Gossip Peers: ~p", [GossipPeers]),

    GossipPeers.
