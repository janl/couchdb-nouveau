%% Copyright 2022 Robert Newson
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-

-module(nouveau_rpc).

-export([search/3]).

-include("nouveau.hrl").
-import(nouveau_util, [index_path/1]).

search(DbName, #index{} = Index0, QueryArgs) ->
    %% Incorporate the shard name into the record.
    Index1 = Index0#index{dbname = DbName},
    Update = maps:get(update, QueryArgs, "true") == "true",

    %% check if index is up to date
    case Update andalso nouveau_index_updater:outdated(Index1) of
        true ->
            case nouveau_index_manager:update_index(Index1) of
                ok ->
                    ok;
                {error, Reason} ->
                    rexi:reply({error, Reason})
            end;
        false ->
            ok;
        {error, Reason} ->
            rexi:reply({error, Reason})
    end,

    %% Run the search
    rexi:reply(nouveau_api:search(index_path(Index1), QueryArgs)).
