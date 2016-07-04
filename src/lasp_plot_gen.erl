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

-export([generate_plots/1]).

-include("lasp.hrl").

%% @doc Generate plots.
generate_plots(Options) ->
    EvalId = proplists:get_value(evaluation_identifier, Options),
    EvalIdDir = root_log_dir() ++ "/" ++ atom_to_list(EvalId),
    EvalNumbers = only_dirs(EvalIdDir),

    lists:foreach(
        fun(EvalNumber) ->
            EvalDir = EvalIdDir ++ "/" ++ EvalNumber,
            generate_plot(EvalDir, EvalId, EvalNumber)
        end,
        EvalNumbers
    ).

%% @private
generate_plot(EvalDir, EvalId, EvalNumber) ->
    lager:info("Will analyse the following directory: ~p", [EvalDir]),

    LogFiles = only_csv_files(EvalDir),
    lager:info("Will analyse the following logs: ~p", [LogFiles]),

    {Map, Types, Times, ConvergenceTimes} = lists:foldl(
        fun(File, {Map0, Types0, Times0, ConvergenceTimes0}) ->
            FilePath = EvalDir ++ "/" ++ File,

            %% Load this file to the map
            %% Also get the types and times found on that log file
            {Map1, Types1, Times1, ConvergenceTimes1} = load_to_map(FilePath, Map0),

            %% Update set of types
            Types2 = ordsets:union(Types0, Types1),
            %% Update set of times
            Times2 = ordsets:union(Times0, Times1),
            %% Update set of convergence times
            ConvergenceTimes2 = ordsets:union(ConvergenceTimes0, ConvergenceTimes1),

            {Map1, Types2, Times2, ConvergenceTimes2}
        end,
        {orddict:new(), ordsets:new(), ordsets:new(), ordsets:new()},
        LogFiles
    ),

    Types1 = lists:delete(convergence, Types),
    lager:info("Types found: ~p", [Types1]),

    %% `ConvergenceTime` is the max of all `ConvergenceTimes`
    TimeZero = lists:min(Times),
    ConvergenceTime = lists:max(ConvergenceTimes) - TimeZero,
    lager:info("Convergence time: ~p", [ConvergenceTime]),

    %% Assume unknown logs with last known values
    Map1 = assume_unknown_logs(Types1, Times, TimeZero, Map),
    lager:info("Unknown logs assumed!"),

    %% Write average in files (one file per type) to `PlotDir`
    PlotDir = root_plot_dir() ++ "/"
           ++ atom_to_list(EvalId) ++ "/"
           ++ EvalNumber ++ "/",
    filelib:ensure_dir(PlotDir),

    generate_per_node_plot(Map1, PlotDir),
    generate_average_plot(Types1, Times, Map1, ConvergenceTime, PlotDir),
    ok.

%% @private
priv_dir() ->
    code:priv_dir(?APP).

%% @private
root_log_dir() ->
    priv_dir() ++ "/logs".

%% @private
root_plot_dir() ->
    priv_dir() ++ "/plots".

%% @private
root_plot_dir(File) ->
    root_plot_dir() ++ "/" ++ File.

%% @private
plot_file() ->
    root_plot_dir("transmission.gnuplot").

%% @private
output_file(PlotDir, Name) ->
    PlotDir ++ Name ++ ".pdf".

%% @private
only_dirs(Dir) ->
    {ok, DirFiles} = file:list_dir(Dir),

    %% Ignore files
    lists:filter(
        fun(Elem) ->
            filelib:is_dir(Dir ++ "/" ++ Elem)
        end,
        DirFiles
    ).

%% @private
only_csv_files(LogDir) ->
    {ok, LogFiles} = file:list_dir(LogDir),

    %% Ignore not csv files
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
load_to_map(FilePath, Map) ->
    %% Open log file
    {ok, FileDescriptor} = file:open(FilePath, [read]),

    %% Ignore the first line
    [_ | Lines] = read_lines(FilePath, FileDescriptor),

    lists:foldl(
        fun(Line, {Map0, Types0, Times0, ConvergenceTimes0}) ->
            %% Parse log line
            [Type0, Time0, Bytes0] = string:tokens(Line, ",\n"),
            TypeA = list_to_atom(Type0),
            {TimeI, _} = string:to_integer(Time0),
            {BytesF, _} = string:to_float(Bytes0),

            {Map2, ConvergenceTimes2} = case TypeA of
                convergence ->
                    ConvergenceTimes1 = ordsets:add_element(TimeI, ConvergenceTimes0),
                    {Map0, ConvergenceTimes1};
                _ ->
                    %% Get dictionary that maps time to logs of this file
                    TimeToLogs0 = case orddict:find(FilePath, Map0) of
                        {ok, Value} ->
                            Value;
                        error ->
                            orddict:new()
                    end,

                    %% Update dictionary `TimeToLogs0` adding new pair log to
                    %% the list of logs mapped to time `TimeI`
                    TimeToLogs1 = orddict:append(TimeI, {BytesF, TypeA}, TimeToLogs0),

                    %% Update dictionary `Map0` with new value `TimeToLogs1`
                    Map1 = orddict:store(FilePath, TimeToLogs1, Map0),
                    {Map1, ConvergenceTimes0}
            end,

            %% Update set of types
            Types1 = ordsets:add_element(TypeA, Types0),
            %% Update set of times
            Times1 = ordsets:add_element(TimeI, Times0),

            {Map2, Types1, Times1, ConvergenceTimes2}
        end,
        {Map, ordsets:new(), ordsets:new(), ordsets:new()},
        Lines
    ).

%% @private
read_lines(FilePath, FileDescriptor) ->
    case io:get_line(FileDescriptor, '') of
        eof ->
            [];
        {error, Error} ->
            lager:warning("Error while reading line from file ~p. Error: ~p", [FilePath, Error]),
            [];
        Line ->
            [Line | read_lines(FilePath, FileDescriptor)]
    end.

%% @private
append_to_file(InputFile, Time, Bytes) ->
    Line = io_lib:format("~w,~w\n", [Time, Bytes]),
    file:write_file(InputFile, Line, [append]).

%% @private
%% If in the logs of one node, we don't find some reference to some
%% time, for every type of log, assume the last known value
assume_unknown_logs(Types, Times, TimeZero, Map) ->
    orddict:fold(
        fun(Node, TimeToLogs0, MapAcc) ->
            LastKnown = create_empty_last_known(Types),
            TimeToLogs1 = assume_per_node(TimeToLogs0, LastKnown, Times, TimeZero),
            orddict:store(Node, TimeToLogs1, MapAcc)
        end,
        orddict:new(),
        Map
    ).

%% @private
assume_per_node(TimeToLogsIn, LastKnownIn, Times, TimeZero) ->
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

            %% Store `Time` minus `TimeZero`
            TimeToLogsAcc1 = orddict:store(Time - TimeZero, Logs1, TimeToLogsAcc),
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
generate_per_node_plot(Map, PlotDir) ->
    {Titles, InputFiles} = write_per_node_to_files(Map, PlotDir),
    OutputFile = output_file(PlotDir, "per_node"),
    Result = run_gnuplot(InputFiles, Titles, OutputFile),
    lager:info("Generating per node plot ~p. Output: ~p", [OutputFile, Result]).

%% @private
write_per_node_to_files(Map, PlotDir) ->
    InputFileToTitle = orddict:fold(
        fun(FileLogPath, TimeToLogs, InputFileToTitle0) ->
            NodeName = node_name(FileLogPath),

            orddict:fold(
                fun(Time, Logs, InputFileToTitle1) ->
                    lists:foldl(
                        fun({Bytes, Type}, InputFileToTitle2) ->
                            Title = atom_to_list(Type) ++ "_" ++ NodeName,
                            InputFile = PlotDir ++ Title ++ ".csv",
                            append_to_file(InputFile, Time, Bytes),

                            case orddict:find(InputFile, InputFileToTitle2) of
                                {ok, _} ->
                                    InputFileToTitle2;
                                error ->
                                    orddict:store(InputFile, Title, InputFileToTitle2)
                            end
                        end,
                        InputFileToTitle1,
                        Logs
                    )
                end,
                InputFileToTitle0,
                TimeToLogs
            )
        end,
        orddict:new(),
        Map
    ),

    {Titles, InputFiles} = orddict:fold(
        fun(InputFile, Title, {Titles0, InputFiles0}) ->
            {[Title | Titles0], [InputFile | InputFiles0]}
        end,
        {[], []},
        InputFileToTitle
    ),
    {Titles, InputFiles}.

%% @private
node_name(FileLogPath) ->
    Tokens = string:tokens(FileLogPath, "\/\."),
    NodeName = lists:nth(length(Tokens) - 1, Tokens),
    re:replace(NodeName, "@", "_", [global, {return, list}]).

%% @private
generate_average_plot(Types, Times, Map, _ConvergenceTime, PlotDir) ->
    %% Do the average of `Map1`
    TypeToTimeAndBytes = average(Types, Times, Map),
    lager:info("Average computed!"),

    InputFiles = write_average_to_files(TypeToTimeAndBytes, PlotDir),
    Titles = get_titles(Types),
    OutputFile = output_file(PlotDir, "average"),
    Result = run_gnuplot(InputFiles, Titles, OutputFile),
    lager:info("Generating average plot ~p. Output: ~p", [OutputFile, Result]).


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

    NodesNumber = orddict:size(Map),

    %% Divide each sum by the number of nodes
    orddict:map(
        fun(_Type, List) ->
            lists:map(
                fun({Time, Sum}) ->
                    case Sum == 0 of
                        true ->
                            {Time, Sum};
                        false ->
                            {Time, Sum / NodesNumber}
                    end
                end,
                List
            )
        end,
        TypeToTimeAndBytesSum
    ).

%% @private
create_empty_dict_type_to_time_and_bytes(Types, Times) ->
    TimeZero = lists:min(Times),

    lists:foldl(
        fun(Type, Map0) ->
            lists:foldl(
                fun(Time, Map1) ->
                    orddict:append(Type, {Time - TimeZero, 0}, Map1)
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
write_average_to_files(TypeToTimeAndBytes, PlotDir) ->
    lists:foldl(
        fun({Type, List}, InputFiles) ->
            InputFile = PlotDir ++ atom_to_list(Type) ++ ".csv",

            lists:foreach(
                fun({Time, Bytes}) ->
                    append_to_file(InputFile, Time, Bytes)
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
            get_title(Type)
        end,
        Types
    ).

%% @private
get_title(aae_send)   -> "AAE Send";
get_title(delta_ack)  -> "Delta Ack";
get_title(delta_send) -> "Delta Send";
get_title(broadcast)  -> "Broadcast";
get_title(convergence)  -> "Convergence". %% @todo

%% @private
run_gnuplot(InputFiles, Titles, OutputFile) ->
    Bin = case os:getenv("MESOS_TASK_ID", "false") of
        "false" ->
            "gnuplot";
        _ ->
            "/usr/bin/gnuplot"
    end,
    Command = Bin ++ " -e \""
                  ++ "outputname='" ++ OutputFile ++ "'; "
                  ++ "inputnames='" ++ join_filenames(InputFiles) ++ "'; "
                  ++ "titles='" ++  join_titles(Titles) ++ "'\" " ++ plot_file(),
    lager:info("~p", [Command]),
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
