%% Create initial set.
{ok, {Id1, _, _, _}} = lasp:declare(orset),

%% Add elements to initial set and update.
{ok, _} = lasp:update(Id1, {add_all, [1,2,3]}, a),

%% Create a second set.
{ok, {Id2, _, _, _}} = lasp:declare(orset),

%% Apply map.
ok = lasp:map(Id1, fun(X) -> X * 2 end, Id2),

%% Query map.
{ok, Value1} = lasp:query(Id2), sets:to_list(Value1).
