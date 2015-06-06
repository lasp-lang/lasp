%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Christopher Meiklejohn.  All Rights Reserved.
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

-module(lasp_programs).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-export([register/4,
         register/0,
         execute/2,
         process/4,
         process/6]).

%% Public API

%% @doc Register an application.
%%
%%      Register a program with a lasp cluster.
%%
%%      This function will, given the path to a local file on each node,
%%      read, compile, parse_transform, and load the library into
%%      memory, with a copy at each replica, customized with a unique
%%      name, to ensure multiple replicas can run on the same physical
%%      node.
%%
%%      `Module' should be the name you want to refer to the program as
%%      when executing it using {@link execute/2}.
%%
%%      `File' is the path to the file on the node receiving the
%%      register request.
%%
%%      When the last argument is `preflist', the programs name will be
%%      hashed and installed on `?N' replicas; when the last argument is
%%      `global', a copy will be installed on all replicas.
%%
%%      Programs must implement the `lasp_program' behavior to be
%%      correct.
%%
-spec register(module(), file(), registration(), [{atom(), term()}]) ->
    ok | {error, timeout} | error.
register(Module, File, global, Options) ->
    {ok, ReqId} = lasp_register_global_fsm:register(Module, File, Options),
    ?WAIT(ReqId, ?TIMEOUT);
register(Module, File, preflist, _Options) ->
    {ok, ReqId} = lasp_register_fsm:register(Module, File),
    ?WAIT(ReqId, ?TIMEOUT);
register(_Module, _File, _Registration, _Options) ->
    error.

%% @doc Execute an application.
%%
%%      Given a registered program using {@link register/3}, execute the
%%      program and receive it's result.
%%
%%      When executing a `preflist' program, contact `?N' replicas, wait
%%      for a majority and return the merge of the results.  When
%%      executing a `global' program, use the coverage facility of
%%      `riak_core' to compute a minimal covering set (or specify a `?R'
%%      value, and contact all nodes, merging the results.
%%
-spec execute(module(), registration()) ->
    {ok, result()} | {error, timeout} | error.
execute(Module, preflist) ->
    {ok, ReqId} = lasp_execute_fsm:execute(Module),
    ?WAIT(ReqId, ?TIMEOUT);
execute(Module, global) ->
    {ok, ReqId} = lasp_execute_coverage_fsm:execute(Module),
    ?WAIT(ReqId, ?TIMEOUT);
execute(_Module, _Registration) ->
    error.

%% @doc Register all known programs; used by Lasp during initialization.
-spec register() -> ok.
register() ->
    lists:foreach(fun({Type, Program}) ->
                File = code:lib_dir(?APP, src) ++ "/" ++ atom_to_list(Program) ++ ".erl",
                ok = lasp_programs:register(Program, File, Type, [])
        end, ?PROGRAMS),
    ok.

%% @doc Process notifications for all registered programs.
%%
%%      Notify a given partition at a given node that an object has
%%      changed for a particular reason.
%%
-spec process(object(), reason(), idx(), node()) -> ok.
process(Object, Reason, Idx, Node) ->
    case riak_core_metadata:get(?PROGRAM_PREFIX, ?PROGRAM_KEY, []) of
        undefined ->
            ok;
        Programs0 ->
            Programs = ?SET:value(Programs0),
            _ = [process(Program, global, Object, Reason, Idx, Node) || Program <- Programs],
            ok
    end,
    ok.

%% @doc Notification of value change.
%%
%%      Notify a given partition at a given node that an object has
%%      changed for a particular reason.
%%
-spec process(module(), registration(), object(), reason(), idx(),
              node()) -> {ok, result()} | {error, timeout}.
process(Module, global, Object, Reason, Idx, Node) ->
    {ok, ReqId} = lasp_process_fsm:process(Module, Object, Reason, Idx, Node),
    ?WAIT(ReqId, ?TIMEOUT).

