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

-module(nouveau_fabric_search).

-export([go/4]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").
-include("nouveau.hrl").

-record(state, {
    limit,
    counters,
    top_docs
}).

go(DbName, DDoc, IndexName, #query_args{} = QueryArgs) ->
    {ok, Index} = design_doc_to_index(DDoc, IndexName),
    Shards = mem3:shards(DbName),
    Workers = fabric_util:submit_jobs(Shards, nouveau_rpc, search, [Index, QueryArgs]),
    Counters = fabric_dict:init(Workers, nil),
    RexiMon = fabric_util:create_monitors(Workers),    State = #state{limit = QueryArgs#query_args.limit, counters = Counters, top_docs = #top_docs{}},
    try
        rexi_utils:recv(Workers, #shard.ref, fun handle_message/3, State, infinity, 1000 * 60 * 60)
    of
        {ok, Result} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    after
        rexi_monitor:stop(RexiMon),
        fabric_util:cleanup(Workers)
    end.


handle_message({ok, Response}, Shard, State) ->
    case fabric_dict:lookup_element(Shard, State#state.counters) of
        undefined ->
            %% already heard from someone else in this range
            {ok, State};
        nil ->
            {Fields} = Response,
            TotalHits = couch_util:get_value(<<"total_hits">>, Fields),
            Hits = couch_util:get_value(<<"hits">>, Fields),

            TopDocs0 = State#state.top_docs,
            TopDocs1 = TopDocs0#top_docs{
                total_hits = TotalHits + TopDocs0#top_docs.total_hits,
                hits = merge_hits(Hits, TopDocs0#top_docs.hits, State#state.limit)
            },

            Counters1 = fabric_dict:store(Shard, ok, State#state.counters),
            Counters2 = fabric_view:remove_overlapping_shards(Shard, Counters1),
            State1 = State#state{counters = Counters2, top_docs = TopDocs1},
            case fabric_dict:any(nil, Counters2) of
                true ->
                    {ok, State1};
                false ->
                    {stop, TopDocs1}
            end
    end;

handle_message({rexi_DOWN, _, {_, NodeRef}, _}, _Shard, State) ->
    #state{counters = Counters0} = State,
    case fabric_util:remove_down_workers(Counters0, NodeRef, []) of
        {ok, Counters1} ->
            {ok, Counters1};
        error ->
            {error, {nodedown, <<"progress not possible">>}}
    end;

handle_message({error, Reason}, _Shard, State) ->
    {error, Reason};

handle_message(Else, _Shard, State) ->
    {error, Else}.


merge_hits(HitsA, HitsB, Limit) ->
    MergedHits = lists:merge(fun compare_hit/2, HitsA, HitsB),
    lists:sublist(MergedHits, Limit).


compare_hit({HitA}, {HitB}) ->
    OrderA = couch_util:get_value(<<"order">>, HitA),
    OrderB = couch_util:get_value(<<"order">>, HitB),
    couch_ejson_compare:less(OrderA, OrderB).

%% copied from dreyfus_index.erl
design_doc_to_index(#doc{id = Id, body = {Fields}}, IndexName) ->
    Language = couch_util:get_value(<<"language">>, Fields, <<"javascript">>),
    {RawIndexes} = couch_util:get_value(<<"indexes">>, Fields, {[]}),
    InvalidDDocError =
        {invalid_design_doc, <<"index `", IndexName/binary, "` must have parameter `index`">>},
    case lists:keyfind(IndexName, 1, RawIndexes) of
        false ->
            {error, {not_found, <<IndexName/binary, " not found.">>}};
        {IndexName, {Index}} ->
            Analyzer = couch_util:get_value(<<"analyzer">>, Index, <<"standard">>),
            case couch_util:get_value(<<"index">>, Index) of
                undefined ->
                    {error, InvalidDDocError};
                Def ->
                    Sig = ?l2b(
                        couch_util:to_hex(
                            couch_hash:md5_hash(
                                term_to_binary({Analyzer, Def})
                            )
                        )
                    ),
                    {ok, #index{
                        analyzer = Analyzer,
                        ddoc_id = Id,
                        def = Def,
                        def_lang = Language,
                        name = IndexName,
                        sig = Sig
                    }}
            end;
        _ ->
            {error, InvalidDDocError}
    end.
