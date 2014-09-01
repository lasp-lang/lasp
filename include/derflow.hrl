-define(BUCKET, <<"derflow">>).

-define(N, 1).
-define(W, 1).
-define(R, 1).

-define(PROGRAM_N, 3).
-define(PROGRAM_W, 2).
-define(PROGRAM_R, 2).

-record(dv, {value,
             next,
             waiting_threads = [],
             binding_list = [],
             creator,
             type,
             lazy = false,
             bound = false}).

-define(LATTICES, [riak_dt_gcounter, riak_dt_lwwreg, riak_dt_gset]).
