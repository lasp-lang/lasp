#!/usr/bin/env escript

-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

main(_) ->
    %% Delete plot directory
    os:cmd("rm -rf " ++ root_plot_dir()),

    %% Generate plots
    EvalIds = only_dirs(root_log_dir()),
    generate_plots(EvalIds).

%% @doc Generate plots.
generate_plots(EvalIds) ->
    TitlesToInputFiles = lists:foldl(
        fun(EvalId, Acc) ->
            EvalIdDir = root_log_dir() ++ "/" ++ EvalId,
            EvalTimestamps = only_dirs(EvalIdDir),

            T = lists:foldl(
                fun(EvalTimestamp, {_Types0, Times0, ToAverage0}) ->
                    EvalDir = EvalIdDir ++ "/" ++ EvalTimestamp,
                    {Types1, TypeToTimesAndBytes, ConvergenceTime}
                        = generate_plot(EvalDir, EvalId, EvalTimestamp),

                    Times2 = ordsets:union(Times0, get_times(TypeToTimesAndBytes)),
                    ToAverage1 = orddict:store(
                        EvalTimestamp,
                        {TypeToTimesAndBytes, ConvergenceTime},
                        ToAverage0
                    ),
                    {Types1, Times2, ToAverage1}

                end,
                {ordsets:new(), ordsets:new(), orddict:new()},
                EvalTimestamps
            ),

            TitlesToInputFiles = generate_executions_average_plot(T, EvalId),
            lists:append(Acc, TitlesToInputFiles)

        end,
        orddict:new(),
        EvalIds
    ),

    {{Titles, InputFiles}, {TitlesPS, InputFilesPS}} = orddict:fold(
				fun(Title, InputFile, {{Titles0, InputFiles0}, {TitlesPS0, InputFilesPS0}}) ->
            case re:run(InputFile, ".*based_ps.*") of
                {match, _} ->
                    {
                        {Titles0, InputFiles0},
                        {
                            lists:append(TitlesPS0, [Title]),
                            lists:append(InputFilesPS0, [InputFile])
                        }
                    };
                nomatch ->
                    {
                        {
                            lists:append(Titles0, [Title]),
                            lists:append(InputFiles0, [InputFile])
                        },
                        {TitlesPS0, InputFilesPS0}
                    }
            end
        end,
        {{[], []}, {[], []}},
        TitlesToInputFiles
    ),
    
    PlotDir = root_plot_dir() ++ "/",

    OutputFile = output_file(PlotDir, "multi-mode"),
    %% Convergence time not supported yet on multi-mode plot
    Result = run_gnuplot(InputFiles, Titles, OutputFile, -1),
    ct:pal("Generating multi-mode plot ~p. Output: ~p", [OutputFile, Result]),

    OutputFilePS = output_file(PlotDir, "multi-mode-ps"),
    %% Convergence time not supported yet on multi-mode plot
    ResultPS = run_gnuplot(InputFilesPS, TitlesPS, OutputFilePS, -1),
    ct:pal("Generating multi-mode-ps plot ~p. Output: ~p", [OutputFilePS, ResultPS]),

    %% Remove input files
    delete_files(InputFiles),
    delete_files(InputFilesPS).

%% @private
generate_plot(EvalDir, EvalId, EvalTimestamp) ->
    ct:pal("Will analyse the following directory: ~p", [EvalDir]),

    LogFiles = only_csv_files(EvalDir),
    ct:pal("Will analyse the following logs: ~p", [LogFiles]),

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
    ct:pal("Types found: ~p", [Types1]),

    %% `ConvergenceTime` is the max of all `ConvergenceTimes`
    TimeZero = lists:min(Times),
    ConvergenceTime = lists:max(ConvergenceTimes) - TimeZero,
    ct:pal("Convergence time: ~p", [ConvergenceTime]),

    %% Assume unknown logs with last known values
    Map1 = assume_unknown_logs(Types1, Times, TimeZero, Map),
    ct:pal("Unknown logs assumed!"),

    %% Write average in files (one file per type) to `PlotDir`
    PlotDir = root_plot_dir() ++ "/"
           ++ EvalId ++ "/"
           ++ EvalTimestamp ++ "/",
    filelib:ensure_dir(PlotDir),

    generate_per_node_plot(Map1, PlotDir),
    TypeToTimesAndBytes = generate_nodes_average_plot(Types1, Times, Map1, ConvergenceTime, PlotDir),

    {Types1, TypeToTimesAndBytes, ConvergenceTime}.

%% @private
priv_dir() ->
    "../priv".

%% @private
eval_dir() ->
    priv_dir() ++ "/evaluation".

%% @private
root_log_dir() ->
    eval_dir() ++ "/logs".

%% @private
root_plot_dir() ->
    eval_dir() ++ "/plots".

%% @private
gnuplot_file() ->
    priv_dir() ++ "/gnuplot_scripts/transmission.gnuplot".

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
    %% This plot does not show the convergence time per node,
    %% thus the -1
    Result = run_gnuplot(InputFiles, Titles, OutputFile, -1),
    ct:pal("Generating per node plot ~p. Output: ~p", [OutputFile, Result]),

    %% Remove input files
    delete_files(InputFiles).

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
generate_nodes_average_plot(Types, Times, Map, ConvergenceTime, PlotDir) ->
    %% Do the average of `Map1`
    TypeToTimeAndBytes = nodes_average(Types, Times, Map),
    ct:pal("Average computed!"),

    InputFiles = write_average_to_files(TypeToTimeAndBytes, PlotDir),
    Titles = get_titles(Types),
    OutputFile = output_file(PlotDir, "average"),
    Result = run_gnuplot(InputFiles, Titles, OutputFile, ConvergenceTime),
    ct:pal("Generating average plot ~p. Output: ~p", [OutputFile, Result]),

    %% Remove input files
    delete_files(InputFiles),

    TypeToTimeAndBytes.

%% @private
%% Do the average of all logs.
%% - Receives:
%%   * set of known types
%%   * set of known times
%%   * a dictionary that maps nodes to dictionaries
%%     (from times to pairs {bytes, type})
%% - Produces a dictionary that maps types to a list of
%%   pairs {time, bytes}
nodes_average(Types, Times, Map) ->
    TimeZero = lists:min(Times),
    Empty = create_empty_dict_type_to_time_and_bytes(
        Types,
        lists:map(fun(Time) -> Time - TimeZero end, Times)
    ),

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
get_times(TypeToTimesAndBytes) ->
    lists:foldl(
        fun({_Type, TimesAndBytes}, Acc) ->
            lists:foldl(
                fun({Time, _Bytes}, Acc1) ->
                    ordsets:add_element(Time, Acc1)
                end,
                Acc,
                TimesAndBytes
            )
        end,
        ordsets:new(),
        TypeToTimesAndBytes
    ).

%% @doc Average all executions
generate_executions_average_plot({Types, Times, ToAverage}, EvalId) ->
    Empty = create_empty_dict_type_to_time_and_bytes(Types, Times),
    TimestampToLastKnown = lists:foldl(
        fun(Timestamp, Acc) ->
            orddict:store(Timestamp, create_empty_last_known(Types), Acc)
        end,
        orddict:new(),
        orddict:fetch_keys(ToAverage)
    ),

    {Map0, _, ConvergenceTimes} = lists:foldl(
        %% For all the times
        fun(Time, Triple0) ->
            orddict:fold(
                %% For all the executions
                fun(Timestamp, {TypeToTimesAndBytes, ConvergenceTime}, Triple1) ->
                    lists:foldl(
                        %% For all the types
                        fun(Type, Triple2) ->
                            update_triple(Type, Time, TypeToTimesAndBytes, Timestamp, ConvergenceTime, Triple2)
                        end,
                        Triple1,
                        Types
                    )
                end,
                Triple0,
                ToAverage
            )
        end,
        {Empty, TimestampToLastKnown, []},
        Times
    ),

    %% Divide bytes by the number of executions
    NumberOfExecutions = length(orddict:fetch_keys(ToAverage)),
    Map1 = orddict:map(
        fun(_Type, TimesAndBytes) ->
            orddict:map(
                fun(_Time, Bytes) ->
                    Bytes / NumberOfExecutions
                end,
                TimesAndBytes
            )
        end,
        Map0
    ),

    %% Compute average convergence time
    AverageConvergenceTime = round(lists:sum(ConvergenceTimes) / length(ConvergenceTimes)),

    PlotDir = root_plot_dir() ++ "/" ++ EvalId ++ "/average/",
    filelib:ensure_dir(PlotDir),

    InputFiles = write_average_to_files(Map1, PlotDir),
    Titles = get_titles(Types),
    OutputFile = output_file(PlotDir, "average"),
    Result = run_gnuplot(InputFiles, Titles, OutputFile, AverageConvergenceTime),
    ct:pal("Generating average plot of all executions ~p. Output: ~p", [OutputFile, Result]),

    lists:foldl(
        fun(N, TitlesToInputFiles) ->
            Type = lists:nth(N, Types),
            InputFile = lists:nth(N, InputFiles),
            Title = get_title(list_to_atom(EvalId)) ++ " - " ++ get_title(Type),
            orddict:store(Title, InputFile, TitlesToInputFiles)
        end,
        orddict:new(),
        lists:seq(1, length(Types))
    ).

%% @private
update_triple(Type, Time, TypeToTimesAndBytes, Timestamp, ConvergenceTime, {Map, TimestampToLastKnown, ConvergenceTimes0}) ->
    TimesAndBytes = orddict:fetch(Type, TypeToTimesAndBytes),
    ConvergenceTimes1 = [ConvergenceTime | ConvergenceTimes0],

    case orddict:find(Time, TimesAndBytes) of
        %% If exits, use it
        {ok, Bytes} ->
            {
                update_entry(Type, Time, Bytes, Map),
                update_last_known_value(Type, Timestamp, TimestampToLastKnown, Bytes),
                ConvergenceTimes1
            };
        %% If not, use last known value
        error ->
            Bytes = get_latest_value(Type, Timestamp, TimestampToLastKnown),
            {
                update_entry(Type, Time, Bytes, Map),
                TimestampToLastKnown,
                ConvergenceTimes1
            }
    end.

%% @private
update_entry(Type, Time, Bytes, Map) ->
    TimesAndBytes0 = orddict:fetch(Type, Map),
    CurrentBytes = orddict:fetch(Time, TimesAndBytes0),
    TimesAndBytes1 = orddict:store(Time, CurrentBytes + Bytes, TimesAndBytes0),
    orddict:store(Type, TimesAndBytes1, Map).

%% @private
get_latest_value(Type, Timestamp, TimestampToLastKnown) ->
    LastKnown = orddict:fetch(Timestamp, TimestampToLastKnown),
    orddict:fetch(Type, LastKnown).

update_last_known_value(Type, Timestamp, TimestampToLastKnown, Bytes) ->
    LastKnown0 = orddict:fetch(Timestamp, TimestampToLastKnown),
    LastKnown1 = orddict:store(Type, Bytes, LastKnown0),
    orddict:store(Timestamp, LastKnown1, TimestampToLastKnown).

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
get_title(state_based_with_aae)             -> "State Based";
get_title(state_based_with_aae_and_tree)    -> "State Based + tree";
get_title(delta_based_with_aae)             -> "Delta based";
get_title(state_based_ps_with_aae)          -> "State Based PS";
get_title(state_based_ps_with_aae_and_tree) -> "State Based PS + tree";
get_title(delta_based_ps_with_aae)          -> "Delta based PS".

%% @private
run_gnuplot(InputFiles, Titles, OutputFile, ConvergenceTime) ->
    Bin = case os:getenv("MESOS_TASK_ID", "false") of
        "false" ->
            "gnuplot";
        _ ->
            "/usr/bin/gnuplot"
    end,
    Command = Bin ++ " -e \""
                  ++ "convergence_time='" ++ integer_to_list(ConvergenceTime) ++ "'; "
                  ++ "outputname='" ++ OutputFile ++ "'; "
                  ++ "inputnames='" ++ join_filenames(InputFiles) ++ "'; "
                  ++ "titles='" ++  join_titles(Titles) ++ "'\" " ++ gnuplot_file(),
    %ct:pal("~p", [Command]),
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

%% @private
delete_files(Files) ->
    lists:foreach(
      fun(File) ->
        ok = file:delete(File)
      end,
      Files
    ).

