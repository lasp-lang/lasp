-module(derflow_helpers).

-export([declare/1,
         read/2,
         bind/3]).

%% @doc Remotely declare a dataflow variable at a given node.
declare(Node) ->
    rpc:call(Node, derflow, declare, []).

%% @doc Remotely bind a dataflow variable at a given node.
bind(Node, Id, Value) ->
    rpc:call(Node, derflow, bind, [Id, Value]).

%% @doc Remotely read a dataflow variable at a given node.
read(Node, Id) ->
    rpc:call(Node, derflow, read, [Id]).
