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

-module(nouveau_index_updater).
-include_lib("couch/include/couch_db.hrl").
-include("nouveau.hrl").

%% public api
-export([outdated/1]).

%% callbacks
-export([start_monitor/1, update/1]).

-import(couch_query_servers, [get_os_process/1, ret_os_process/1, proc_prompt/2]).
-import(nouveau_util, [index_name/1]).

outdated(#index{} = Index) ->
    case open_or_create_index(Index) of
        {ok, CurSeq} ->
            couch_log:notice("outdated ~p", [CurSeq]),
            get_update_seq(Index) > CurSeq;
        {error, Reason} ->
            {error, Reason}
    end.


start_monitor(#index{} = Index) ->
    proc_lib:start_monitor(?MODULE, update, [Index]).


update(#index{} = Index) ->
    proc_lib:init_ack(ok),
    {ok, Db} = couch_db:open_int(Index#index.dbname, []),
    try
        case open_or_create_index(Index) of
            {error, Reason} ->
                exit({error, Reason});
            {ok, CurSeq} ->
                TotalChanges = couch_db:count_changes_since(Db, CurSeq),
                couch_task_status:add_task([
                    {type, search_indexer},
                    {database, Index#index.dbname},
                    {design_document, Index#index.ddoc_id},
                    {index, Index#index.name},
                    {progress, 0},
                    {changes_done, 0},
                    {total_changes, TotalChanges}
                ]),

                %% update status every half second
                couch_task_status:set_update_frequency(500),

                Proc = get_os_process(Index#index.def_lang),
                try
                    true = proc_prompt(Proc, [<<"add_fun">>, Index#index.def]),
                    Acc0 = {Db, Index, Proc, 0, TotalChanges},
                    {ok, _} = couch_db:fold_changes(Db, CurSeq, fun load_docs/2, Acc0, [])
                after
                    ret_os_process(Proc)
                end,
                exit(normal)
        end
    after
        couch_db:close(Db)
    end.


load_docs(FDI, {Db, Index, Proc, ChangesDone, TotalChanges}) ->
    couch_task_status:update([{changes_done, ChangesDone}, {progress, (ChangesDone * 100) div TotalChanges}]),

    DI = couch_doc:to_doc_info(FDI),
    #doc_info{id = Id, high_seq = Seq, revs = [#rev_info{deleted = Del} | _]} = DI,

    case Del of
        true ->
            ok = nouveau_api:delete_doc(index_name(Index), Id, Seq);
        false ->
            {ok, Doc} = couch_db:open_doc(Db, DI, []),
            Json = couch_doc:to_json_obj(Doc, []),
            [Fields0 | _] = proc_prompt(Proc, [<<"index_doc">>, Json]),
            case Fields0 of
                [] ->
                    ok = nouveau_api:delete_doc(index_name(Index), Id, Seq);
                _ ->
                    Fields1 = convert_fields(Fields0),
                    ok = nouveau_api:update_doc(index_name(Index), Id, Seq, Fields1)
           end
    end,
    {ok, {Db, Index, Proc, ChangesDone + 1, TotalChanges}}.


open_or_create_index(#index{} = Index) ->
    case get_update_seq(Index) of
        {ok, UpdateSeq} ->
            {ok, UpdateSeq};
        {error, {not_found, _}} ->
            case nouveau_api:create_index(index_name(Index), index_definition(Index)) of
                ok ->
                    {ok, 0};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


get_update_seq(#index{} = Index) ->
    case nouveau_api:index_info(index_name(Index)) of
        {ok, {Fields}} ->
            {ok, couch_util:get_value(<<"update_seq">>, Fields)};
        {error, Reason} ->
            {error, Reason}
    end.


convert_fields(Fields) ->
    lists:reverse(convert_fields(Fields, [])).

convert_fields([], Acc) ->
    Acc;

convert_fields([[Name, Value, Options] | Rest], Acc) when is_binary(Name), is_binary(Value) ->
    Field = case facet(Options) of
        true ->
            {[
                {<<"@type">>, <<"string">>},
                {<<"name">>, Name},
                {<<"value">>, Value},
                {<<"stored">>, stored(Options)},
                {<<"facet">>, true}
            ]};
        false ->
            {[
                {<<"@type">>, <<"text">>},
                {<<"name">>, Name},
                {<<"value">>, Value},
                {<<"stored">>, stored(Options)}
            ]}
    end,
    convert_fields(Rest, [Field | Acc]);

convert_fields([[Name, Value, Options] | Rest], Acc) when is_binary(Name), is_number(Value) ->
    Field = {[
        {<<"@type">>, <<"double">>},
        {<<"name">>, Name},
        {<<"value">>, Value},
        {<<"stored">>, stored(Options)},
        {<<"facet">>, facet(Options)}
    ]},
    convert_fields(Rest, [Field | Acc]).


stored({Options}) ->
    case couch_util:get_value(<<"store">>, Options) of
        true ->
            true;
        false ->
            false;
        <<"YES">> ->
            true;
        <<"yes">> ->
            true;
        <<"NO">> ->
            false;
        <<"no">> ->
            false;
        _ ->
            false
    end.

facet({Options}) ->
    case couch_util:get_value(<<"facet">>, Options) of
        true ->
            true;
        false ->
            false;
        undefined ->
            false
    end.


%% TODO add clause for the field analyzers
index_definition(#index{} = Index)
  when is_binary(Index#index.analyzer) ->
    {[{<<"default_analyzer">>, Index#index.analyzer}]}.
