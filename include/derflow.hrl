-define(BUCKET, <<"derflow">>).
-define(W, 2).
-define(N, 1).

-record(dv, {value,
             next,
             waiting_threads = [],
             binding_list = [],
             creator,
             type,
             lazy = false,
             bound = false}).

-define(LATTICES, [riak_dt_gcounter, riak_dt_lwwreg, riak_dt_gset]).
