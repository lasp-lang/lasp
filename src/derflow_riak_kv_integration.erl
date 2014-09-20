-module(derflow_riak_kv_integration).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([postcommit_log_object/1]).

%% @doc Trigger a log message when postcommit hook is triggered.
postcommit_log_object(Object) ->
    lager:info("Postcommit hook triggered for ~p", [Object]).
