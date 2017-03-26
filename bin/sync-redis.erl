#!/usr/bin/env escript

%%! -pa _build/exp/lib/eredis/ebin/

main(_) ->
    RedisHost = os:getenv("REDIS_SERVICE_HOST", "127.0.0.1"),
    RedisPort = os:getenv("REDIS_SERVICE_PORT", "6379"),
    {ok, C} = eredis:start_link(RedisHost, list_to_integer(RedisPort)),
    {ok, Keys} = eredis:q(C, ["KEYS", "*"]),

    lists:foreach(fun(Filename) ->
                          {ok, Content} = eredis:q(C, ["GET", Filename]),
                          Path = "logs/" ++ binary_to_list(Filename),
                          io:format("Adding to /tmp, file: ~p~n", [Path]),
                          ok = filelib:ensure_dir(Path),
                          ok = file:write_file(Path, Content)
                  end, Keys),

    ok.
