-define(TIMEOUT, 100000).

-define(BUCKET, <<"derflow">>).

-define(N, 1).
-define(W, 1).
-define(R, 1).

-define(PROGRAM_N, 3).
-define(PROGRAM_W, 2).
-define(PROGRAM_R, 2).

-record(derflow_execute_request_v1, {
        module :: atom(),
        req_id :: non_neg_integer(),
        caller :: pid()}).

-define(EXECUTE_REQUEST, #derflow_execute_request_v1).

-record(dv, {value,
             next,
             waiting_threads = [],
             binding_list = [],
             creator,
             type,
             lazy = false,
             bound = false}).

-define(LATTICES, [riak_dt_gset]).
