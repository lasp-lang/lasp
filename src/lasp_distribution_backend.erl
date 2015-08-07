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

-module(lasp_distribution_backend).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

%% Declare a new dataflow variable of a given type.
%%
%% Valid values for `Type' are any of lattices supporting the
%% `riak_dt' behavior.  Type is declared with the provided `Id'.
%%
-callback declare(id(), type()) -> {ok, id()} | error().

%% Update a dataflow variable.
%%
%% Read the given `Id' and update it given the provided
%% `Operation', which should be valid for the type of CRDT stored
%% at the given `Id'.
%%
-callback update(id(), operation(), actor()) -> {ok, {value(), id()}} | error().

%% Bind a dataflow variable to a value.
%%
%% The provided `Id' is identifier of a previously declared (see:
%% {@link declare/0}) dataflow variable.  The `Value' provided is
%% the value to bind.
%%
-callback bind(id(), value()) -> {ok, var()} | error().

%% Bind a dataflow variable to another dataflow variable.
%%
%% The provided `Id' is identifier of a previously declared (see:
%% {@link declare/0}) dataflow variable.  The `Value' provided is
%% the value to bind.
%%
-callback bind_to(id(), id()) -> {ok, id()} | error().

%% Blocking monotonic read operation for a given dataflow variable.
%%
%% Block until the variable identified by `Id' has been bound, and
%% is monotonically greater (as defined by the lattice) then the
%% provided `Threshold' value.
%%
-callback read(id(), threshold()) -> {ok, var()} | error().

%% Blocking monotonic read operation for a list of given dataflow
%% variables.
%%
-callback read_any([{id(), threshold()}]) -> {ok, var()} | error().

%% Compute the cartesian product of two sets.
%%
%% Computes the cartestian product of two sets and bind the result
%% to a third.
%%
-callback product(id(), id(), id()) -> ok | error().

%% Compute the union of two sets.
%%
%% Computes the union of two sets and bind the result
%% to a third.
%%
-callback union(id(), id(), id()) -> ok | error().

%% Compute the intersection of two sets.
%%
%% Computes the intersection of two sets and bind the result
%% to a third.
%%
-callback intersection(id(), id(), id()) -> ok | error().

%% Map values from one lattice into another.
%%
%% Applies the given `Function' as a map over the items in `Id',
%% placing the result in `AccId', both of which need to be declared
%% variables.
%%
-callback map(id(), function(), id()) -> ok | error().

%% Fold values from one lattice into another.
%%
%% Applies the given `Function' as a fold over the items in `Id',
%% placing the result in `AccId', both of which need to be declared
%% variables.
%%
-callback fold(id(), function(), id()) -> ok | error().

%% Filter values from one lattice into another.
%%
%% Applies the given `Function' as a filter over the items in `Id',
%% placing the result in `AccId', both of which need to be declared
%% variables.
%%
-callback filter(id(), function(), id()) -> ok | error().

%% Spawn a function.
%%
%% Spawn a process executing `Module:Function(Args)'.
%%
-callback thread(module(), func(), args()) -> ok | error().

%% Pause execution until value requested with given threshold.
%%
%% Pause execution of calling thread until a read operation is
%% issued for the given `Id'.  Used to introduce laziness into a
%% computation.
%%
-callback wait_needed(id(), threshold()) -> ok | error().
