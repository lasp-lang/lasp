%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(lasp_plot_gen).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-export([generate_plot/0]).

-include("lasp.hrl").

%% @doc Generate plots.
generate_plot() ->
    LogFiles = log_files(),
    lager:info("Will analyse the following logs: ~p", [LogFiles]),

    {Map0, Types, Times} = lists:foldl(
        fun(File0, {Map0, Types0, Times0}) ->
            %% Load this file to the map
            %% Also get the types and times found on that log file
            {Map1, Types1, Times1} = load_to_map(File0, Map0),

            %% Update set of types
            Types2 = ordsets:union(Types0, Types1),
            %% Update set of times
            Times2 = ordsets:union(Times0, Times1),

            {Map1, Types2, Times2}
        end,
        {orddict:new(), ordsets:new(), ordsets:new()},
        LogFiles
    ),

    lager:info("Types found: ~p", [Types]),

    %% Assume unknown logs with last known values
    Map1 = assume_unknown_logs(Types, Times, Map0),
    lager:info("Unknown logs assumed!"),

    %% Do the average of `Map1`
    TypeToTimeAndBytes = average(Types, Times, Map1),
    lager:info("Average computed!"),

    %% Write average to files (one file per type)
    InputFiles = write_to_files(TypeToTimeAndBytes),
    lager:info("Wrote average to files: ~p", [InputFiles]),

    Titles = get_titles(Types),

    Result = run_gnuplot(InputFiles, Titles),
    _ = lager:info("Generating plot. Output: ~p", [Result]),
    ok.

%% @private
priv_dir() ->
    code:priv_dir(?APP).

%% @private
log_dir() ->
    priv_dir() ++ "/logs".

%% @private
log_dir(File) ->
    log_dir() ++ "/" ++ File.

%% @private
plot_dir() ->
    priv_dir() ++ "/plots".

%% @private
plot_dir(File) ->
    plot_dir() ++ "/" ++ File.

%% @private
output_file() ->
    plot_dir("out.pdf").

%% @private
plot_file() ->
    plot_dir("transmission.gnuplot").

%% @private
log_files() ->
    {ok, LogFiles} = file:list_dir(log_dir()),

    % Ignore not csv files
    lists:filter(
        fun(Elem) ->
            case re:run(Elem, ".*.csv") of
                {match, _} ->
                    true;
                nomatch ->
                    false
            end
        end,
        LogFiles
    ).

%% @private
load_to_map(File, Map) ->
    %% Open log file
    {ok, FileDescriptor} = file:open(log_dir(File), [read]),

    %% Ignore the first line
    [_ | Lines] = read_lines(File, FileDescriptor),

    lists:foldl(
        fun(Line, {Map0, Types0, Times0}) ->
            %% Parse log line
            [Type, Time, Bytes] = string:tokens(Line, ",\n"),
            {TimeF, _} = string:to_integer(Time),
            {BytesF, _} = string:to_float(Bytes),

            %% Get dictionary that maps time to logs of this file
            TimeToLogs0 = case orddict:find(File, Map0) of
                {ok, Value} ->
                    Value;
                error ->
                    orddict:new()
            end,

            %% Update dictionary `TimeToLogs0` adding new pair log to
            %% the list of logs mapped to time `TimeF`
            TimeToLogs1 = orddict:append(TimeF, {BytesF, Type}, TimeToLogs0),

            %% Update dictionary `Map0` with new value `TimeToLogs1`
            Map1 = orddict:store(File, TimeToLogs1, Map0),
            %% Update set of types
            Types1 = ordsets:add_element(Type, Types0),
            %% Update set of times
            Times1 = ordsets:add_element(TimeF, Times0),

            {Map1, Types1, Times1}
        end,
        {Map, ordsets:new(), ordsets:new()},
        Lines
    ).

%% @private
read_lines(File, FileDescriptor) ->
    case io:get_line(FileDescriptor, '') of
        eof ->
            [];
        {error, Error} ->
            lager:warning("Error while reading line from file ~p. Error: ~p", [File, Error]),
            [];
        Line ->
            [Line | read_lines(File, FileDescriptor)]
    end.

%% @private
%% If in the logs of one node, we don't find some reference to some
%% time, for every type of log, assume the last known value
assume_unknown_logs(Types, Times, Map) ->
    orddict:fold(
        fun(Node, TimeToLogs0, MapAcc) ->
            LastKnown = create_empty_last_known(Types),
            TimeToLogs1 = assume_per_node(TimeToLogs0, LastKnown, Times),
            orddict:store(Node, TimeToLogs1, MapAcc)
        end,
        orddict:new(),
        Map
    ).

%% @private
assume_per_node(TimeToLogsIn, LastKnownIn, Times) ->
    {TimeToLogs, _} = lists:foldl(
        fun(Time, {TimeToLogsAcc, LastKnownAcc}) ->
            {Logs1, LastKnownAcc1} = case orddict:find(Time, TimeToLogsIn) of
                {ok, Logs0} ->
                    %% If the logs exist for this time,
                    %% check if there is a log for all types
                    %% and update the last known values
                    orddict:fold(
                        fun(Type, BytesKnown, {Logs2, LastKnownAcc2}) ->
                            case lists:keyfind(Type, 2, Logs2) of
                                {Bytes, Type} ->
                                    %% If there is a log,
                                    %% update the last known values
                                    LastKnownAcc3 = orddict:store(Type, Bytes, LastKnownAcc2),
                                    {Logs2, LastKnownAcc3};
                                false ->
                                    %% If there isn't a log,
                                    %% create it with the last known value
                                    Logs3 = [{BytesKnown, Type} | Logs2],
                                    {Logs3, LastKnownAcc2}
                            end
                        end,
                        {Logs0, LastKnownAcc},
                        LastKnownAcc
                    );
                error ->
                    %% If the logs do not exist for this time,
                    %% use the last known values of all types
                    %% Here there's no value in `LastKnownAcc`
                    %% to be updated
                    {revert_tuple_order(LastKnownAcc), LastKnownAcc}
            end,

            TimeToLogsAcc1 = orddict:store(Time, Logs1, TimeToLogsAcc),
            {TimeToLogsAcc1, LastKnownAcc1}
        end,
        {orddict:new(), LastKnownIn},
        Times
    ),
    TimeToLogs.

%% @private
create_empty_last_known(Types) ->
    lists:foldl(
        fun(Type, Acc) ->
            orddict:store(Type, 0, Acc)
        end,
        orddict:new(),
        Types
    ).

%% @private
revert_tuple_order(LastKnown) ->
    orddict:fold(
        fun(Type, Bytes, List) ->
            lists:append(List, [{Bytes, Type}])
        end,
        [],
        LastKnown
    ).

%% @private
%% Do the average of all logs.
%% - Receives:
%%   * set of known types
%%   * set of known times
%%   * a dictionary that maps nodes to dictionaries
%%     (from times to pairs {bytes, type})
%% - Produces a dictionary that maps types to a list of
%%   pairs {time, bytes}
average(Types, Times, Map) ->
    Empty = create_empty_dict_type_to_time_and_bytes(Types, Times),

    %% Create dictionary the maps types to a lists of
    %% pairs {time, bytes}
    %% where bytes is the sum of bytes from all nodes
    TypeToTimeAndBytesSum = orddict:fold(
        fun(_Node, Dict, Map1) ->
            orddict:fold(
                fun(Time, Logs, Map2) ->
                    lists:foldl(
                        fun({Bytes, Type}, Map3) ->
                            update_average_dict(Type, Time, Bytes, Map3)
                        end,
                        Map2,
                        Logs
                    )
                end,
                Map1,
                Dict
            )
        end,
        Empty,
        Map
    ),

    TimeZero = lists:min(Times),
    NodesNumber = orddict:size(Map),

    %% Divide each sum by the number of nodes
    %% Also subtract `TimeZero` to all times
    orddict:map(
        fun(_Type, List) ->
            lists:map(
                fun({Time, Sum}) ->
                    case Sum == 0 of
                        true ->
                            {Time - TimeZero, Sum};
                        false ->
                            {Time - TimeZero, Sum / NodesNumber}
                    end
                end,
                List
            )
        end,
        TypeToTimeAndBytesSum
    ).

%% @private
create_empty_dict_type_to_time_and_bytes(Types, Times) ->
    lists:foldl(
        fun(Type, Map0) ->
            lists:foldl(
                fun(Time, Map1) ->
                    orddict:append(Type, {Time, 0}, Map1)
                end,
                Map0,
                Times
            )
        end,
        orddict:new(),
        Types
    ).

%% @private
update_average_dict(Type, Time, Bytes, Map) ->
    case orddict:find(Type, Map) of
        {ok, TimeToBytes0} ->
            TimeToBytes1 = case lists:keyfind(Time, 1, TimeToBytes0) of
                {Time, BytesSum} ->
                    lists:keyreplace(Time, 1, TimeToBytes0, {Time, BytesSum + Bytes});
                false ->
                    %% This will never happen
                    lager:warning("Unknown time ~p in list ~p", [Time, TimeToBytes0]),
                    TimeToBytes0
            end,
            orddict:store(Type, TimeToBytes1, Map);
        error ->
            %% This will never happen
            lager:warning("Unknown type ~p in dictionary ~p", [Type, Map]),
            Map
    end.

%% @private
%% Write the average to files and return the name of the files.
write_to_files(TypeToTimeAndBytes) ->
    lists:foldl(
        fun({Type, List}, InputFiles) ->
            InputFile = plot_dir(Type) ++ ".csv",
            %% truncate file
            file:write_file(InputFile, "", [write]),
            lists:foreach(
                fun({Time, Bytes}) ->
                    Line = io_lib:format("~w,~w\n", [Time, Bytes]),
                    file:write_file(InputFile, Line, [append])
                end,
                List
            ),
            lists:append(InputFiles, [InputFile])
        end,
        [],
        TypeToTimeAndBytes
    ).

%% @private
get_titles(Types) ->
    lists:map(
        fun(Type) ->
            get_title(list_to_atom(Type))
        end,
        Types
    ).

%% @private
get_title(aae_send)   -> "AAE Send";
get_title(ack_send)   -> "Ack Send";
get_title(delta_send) -> "Delta Send";
get_title(broadcast)  -> "Broadcast".

%% @private
run_gnuplot(InputFiles, Titles) ->
    Bin = case os:getenv("MESOS_TASK_ID", "false") of
        "false" ->
            "gnuplot";
        _ ->
            "/usr/bin/gnuplot"
    end,
    Command = Bin ++ " -e \""
                  ++ "outputname='" ++ output_file() ++ "'; "
                  ++ "inputnames='" ++ join_filenames(InputFiles) ++ "'; "
                  ++ "titles='" ++  join_titles(Titles) ++ "'\" " ++ plot_file(),
    os:cmd(Command).

%% @private
join_filenames(InputFiles) ->
    Line = lists:foldl(
        fun(Elem, Acc) ->
            Acc ++ Elem
                ++ " "
        end,
        "",
        InputFiles
    ),
    string:strip(Line).

%% @private
join_titles(Titles) ->
    Line = lists:foldl(
        fun(Elem, Acc) ->
            % "transmission.gnuplot" does not support titles with spaces
            % But it converts all the "_" in the titles to " "
            Acc ++ re:replace(Elem, " ", "_", [global, {return, list}])
                ++ " "
        end,
        "",
        Titles
    ),
    string:strip(Line).
