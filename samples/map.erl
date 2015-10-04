%% Create initial set.
{ok, S1} = lasp:declare(riak_dt_orset),

%% Add elements to initial set and update.
{ok, _} = lasp:update(S1, {add_all, [1,2,3]}, a),

%% Create a second set.
{ok, S2} = lasp:declare(riak_dt_orset),

%% Apply map.
ok = lasp:map(S1, fun(X) -> X * 2 end, S2).
