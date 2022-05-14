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
-export([update/1]).

-import(couch_query_servers, [get_os_process/1, ret_os_process/1, proc_prompt/2]).
-import(nouveau_util, [index_path/1]).

outdated(#index{} = Index) ->
    case open_or_create_index(Index) of
        {ok, IndexSeq} ->
            DbSeq = get_db_seq(Index),
            DbSeq > IndexSeq;
        {error, Reason} ->
            {error, Reason}
    end.


update(#index{} = Index) ->
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
                end
        end
    after
        couch_db:close(Db)
    end.


load_docs(#full_doc_info{id = <<"_design/", _/binary>>}, Acc) ->
    {ok, Acc};

load_docs(FDI, {Db, Index, Proc, ChangesDone, TotalChanges}) ->
    couch_task_status:update([{changes_done, ChangesDone}, {progress, (ChangesDone * 100) div TotalChanges}]),

    DI = couch_doc:to_doc_info(FDI),
    #doc_info{id = Id, high_seq = Seq, revs = [#rev_info{deleted = Del} | _]} = DI,

    case Del of
        true ->
            ok = nouveau_api:delete_doc(index_path(Index), Id, Seq);
        false ->
            {ok, Doc} = couch_db:open_doc(Db, DI, []),
            Json = couch_doc:to_json_obj(Doc, []),
            [Fields0 | _] = proc_prompt(Proc, [<<"index_doc">>, Json]),
            case Fields0 of
                [] ->
                    ok = nouveau_api:delete_doc(index_path(Index), Id, Seq);
                _ ->
                    Fields1 = convert_fields(Fields0),
                    ok = nouveau_api:update_doc(index_path(Index), Id, Seq, Fields1)
           end
    end,
    {ok, {Db, Index, Proc, ChangesDone + 1, TotalChanges}}.


open_or_create_index(#index{} = Index) ->
    case get_index_seq(Index) of
        {ok, UpdateSeq} ->
            {ok, UpdateSeq};
        {error, {not_found, _}} ->
            case nouveau_api:create_index(index_path(Index), index_definition(Index)) of
                ok ->
                    {ok, 0};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

get_db_seq(#index{} = Index) ->
    {ok, Db} = couch_db:open_int(Index#index.dbname, []),
    try
        couch_db:get_update_seq(Db)
    after
        couch_db:close(Db)
    end.

get_index_seq(#index{} = Index) ->
    case nouveau_api:index_info(index_path(Index)) of
        {ok, {Fields}} ->
            {ok, couch_util:get_value(<<"update_seq">>, Fields)};
        {error, Reason} ->
            {error, Reason}
    end.


convert_fields(Fields) ->
    lists:flatmap(fun convert_field/1, Fields).

convert_field([Name, Value, Options]) when is_binary(Name), is_binary(Value) ->
    case {tokenized(Options), facet(Options)} of
        {true, _} ->
            [text_field(Name, Value, stored(Options))];
        {false, true} ->
            [string_field(Name, Value, stored(Options)), sorted_dv(Name, Value), sorted_set_dv(Name, Value)];
        {false, false} ->
            [string_field(Name, Value, stored(Options)), sorted_dv(Name, Value)]
    end;

convert_field([Name, Value, Options]) when is_binary(Name), is_number(Value) ->
    case {facet(Options), stored(Options)} of
        {false, false} ->
            [double_point(Name, Value)];
        {true, false} ->
            [double_point(Name, Value), double_dv(Name, Value)];
        {true, true} ->
            [double_point(Name, Value), stored_double(Name, Value), double_dv(Name, Value)]
    end.


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


tokenized({Options}) ->
    case couch_util:get_value(<<"tokenized">>, Options) of
        true ->
            true;
        false ->
            false;
        undefined ->
            true
    end.


string_field(Name, Value, Stored) when is_binary(Name), is_binary(Value), is_boolean(Stored) ->
    {[
      {<<"@type">>, <<"string">>},
      {<<"name">>, Name},
      {<<"value">>, Value},
      {<<"stored">>, Stored}
     ]}.


text_field(Name, Value, Stored) when is_binary(Name), is_binary(Value), is_boolean(Stored) ->
    {[
      {<<"@type">>, <<"text">>},
      {<<"name">>, Name},
      {<<"value">>, Value},
      {<<"stored">>, Stored}
     ]}.


double_point(Name, Value) when is_binary(Name), is_number(Value) ->
    {[
      {<<"@type">>, <<"double_point">>},
      {<<"name">>, Name},
      {<<"value">>, Value}
     ]}.


sorted_dv(Name, Value) when is_binary(Name), is_binary(Value) ->
    {[
      {<<"@type">>, <<"sorted_dv">>},
      {<<"name">>, Name},
      {<<"value">>, base64:encode(Value)}
     ]}.

sorted_set_dv(Name, Value) when is_binary(Name), is_binary(Value) ->
    {[
      {<<"@type">>, <<"sorted_set_dv">>},
      {<<"name">>, Name},
      {<<"value">>, base64:encode(Value)}
     ]}.


stored_double(Name, Value) when is_binary(Name), is_number(Value) ->
    {[
      {<<"@type">>, <<"stored_double">>},
      {<<"name">>, Name},
      {<<"value">>, Value}
     ]}.

double_dv(Name, Value) when is_binary(Name), is_number(Value) ->
    {[
      {<<"@type">>, <<"double_dv">>},
      {<<"name">>, Name},
      {<<"value">>, Value}
     ]}.


index_definition(#index{} = Index) ->
    #{
      <<"default_analyzer">> => Index#index.default_analyzer,
      <<"field_analyzers">> => Index#index.field_analyzers
     }.
