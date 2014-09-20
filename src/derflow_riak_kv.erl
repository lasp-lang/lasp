-module(derflow_riak_kv).
-include("derflow.hrl").

-export([produce/3]).

produce(Object, Reason, Partition) ->
    lager:info("Received object: ~p reason: ~p, partition: ~p",
               [Object, Reason, Partition]).
