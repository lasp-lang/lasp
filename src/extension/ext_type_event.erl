-module(ext_type_event).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

%% API
-export([
    new_event/3,
    get_node_id/1]).
-export([
    event_to_atom/1,
    atom_to_event/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([
    ext_event/0]).

-define(SEPARATOR_LIST, "|").

-type ext_node_id() :: term().
-type ext_replica_id() :: term().
-type ext_event() :: {ext_node_id(), ext_replica_id(), non_neg_integer()} | atom().

-spec new_event(ext_node_id(), ext_replica_id(), pos_integer()) -> ext_event().
new_event(NodeId, ReplicaId, Count) ->
    {NodeId, ReplicaId, Count}.

-spec get_node_id(ext_event()) -> ext_node_id().
get_node_id({NodeId, _ReplicaId, _Count}=_Event) ->
    NodeId;
get_node_id(Event) when erlang:is_atom(Event) ->
    {NodeId, _ReplicaId, _Count}=_RealEvent = atom_to_event(Event),
    NodeId.

-spec event_to_atom(ext_event()) -> ext_event().
event_to_atom({NodeId, ReplicaId, Count}=_Event) ->
    StringNodeId = erlang:binary_to_list(NodeId),
    StringCount = erlang:integer_to_list(Count),
    erlang:list_to_atom(
        StringNodeId ++ ?SEPARATOR_LIST ++ ReplicaId ++ ?SEPARATOR_LIST ++ StringCount).

-spec atom_to_event(ext_event()) -> ext_event().
atom_to_event(AtomEvent) ->
    StringEvent = erlang:atom_to_list(AtomEvent),
    [StringNodeId, StringReplicaId, StringCount]=_TokensEvent =
        string:tokens(StringEvent, ?SEPARATOR_LIST),
    {erlang:list_to_binary(StringNodeId), StringReplicaId, list_to_integer(StringCount)}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

event_to_atom_test() ->
    Event0 = {<<"gXuFGO">>, "replica", 5},
    AtomEvent0 = 'gXuFGO|replica|5',

    ?assertEqual(AtomEvent0, ?MODULE:event_to_atom(Event0)),

    ?assertEqual(Event0, ?MODULE:atom_to_event(AtomEvent0)).

-endif.
