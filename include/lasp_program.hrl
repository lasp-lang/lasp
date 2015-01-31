%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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

% -module(lasp_program).

%% @doc Only CRDTs are able to be processed.
-type crdt() :: riak_dt:crdt().

%% @doc Output of program must be a CRDT.
-type output() :: crdt().

%% @doc Any state that needs to be tracked from one execution to the
%%      next execution.
-type state() :: term().

%% @doc Reasons provided by the riak_kv vnode for change.
-type reason() :: put | handoff | delete.

%% @doc The type of objects that we can be notified about.
-type object() :: crdt().

%% @doc Initialize the program.  Perform whatever initial configuration
%%      is required.
-callback init() -> {ok, state()}.

%% @doc Given a notification from the underlying system about an object
%%      having been put, handed off, or deleted, notify all programs that
%%      need to be notified.
-callback process(object(), reason(), actor(), state()) -> {ok, state()}.

%% @doc Return the current result of a given program.
-callback execute(state()) -> {ok, output()}.

%% @doc Merge the results of a replicated program, which should compute
%%      the results least-upper-bound for the given output type.
-callback merge(list(output())) -> {ok, output()}.

%% @doc Sum the results of a sharded computation; this should compute an
%%      associatative, commutative sum across all known executions of the
%%      program; given a list of sharded CRDTs, sum the results.  This is
%%      essentially a commutative monoid.
-callback sum(list(output())) -> {ok, output()}.
