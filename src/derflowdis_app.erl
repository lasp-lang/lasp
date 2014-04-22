-module(derflowdis_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case derflowdis_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, derflowdis_vnode}]),

            ok = riak_core_ring_events:add_guarded_handler(derflowdis_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(derflowdis_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(derflowdis, self()),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
