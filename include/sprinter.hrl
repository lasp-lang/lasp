%% State record.
-record(state, {backend,
                is_connected,
                was_connected,
                attempted_nodes,
                graph,
                tree,
                eredis,
                servers,
                nodes}).
