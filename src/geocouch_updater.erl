%%% -*- erlang -*-
%%%
%%% This file is part of geocouch released under the Apache license 2. 
%%% See the NOTICE for more information.


-module(geocouch_updater).

-export([start_update/3, process_doc/3, finish_update/1, purge_index/4]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("geocouch/include/geocouch.hrl").

start_update(Partial, State, NumChanges) ->
    QueueOpts = [{max_size, 100000}, {max_items, 500}],
    {ok, DocQueue} = couch_work_queue:new(QueueOpts),
    {ok, WriteQueue} = couch_work_queue:new(QueueOpts),

    FirstState = State#gcst{
        first_build=State#gcst.update_seq==0,
        partial_resp_pid=Partial,
        doc_acc=[],
        doc_queue=DocQueue,
        write_queue=WriteQueue
    },

    InitState = start_query_server(FirstState),

    Self = self(),
    MapFun = fun() ->
        couch_task_status:add_task([
            {type, indexer},
            {database, State#gcst.db_name},
            {design_document, State#gcst.idx_name},
            {progress, 0},
            {changes_done, 0},
            {total_changes, NumChanges}
        ]),
        couch_task_status:set_update_frequency(500),
        map_docs(InitState)
    end,
    WriteFun = fun() -> write_results(Self, InitState) end,

    spawn_link(MapFun),
    spawn_link(WriteFun),

    {ok, InitState}.


process_doc(Doc, Seq, #gcst{doc_acc=Acc}=State) when length(Acc) > 100 ->
    couch_work_queue:queue(State#gcst.doc_queue, lists:reverse(Acc)),
    process_doc(Doc, Seq, State#gcst{doc_acc=[]});
process_doc(nil, Seq, #gcst{doc_acc=Acc}=State) ->
    {ok, State#gcst{doc_acc=[{nil, Seq, nil} | Acc]}};
process_doc(#doc{id=Id, deleted=true}, Seq, #gcst{doc_acc=Acc}=State) ->
    {ok, State#gcst{doc_acc=[{Id, Seq, deleted} | Acc]}};
process_doc(#doc{id=Id}=Doc, Seq, #gcst{doc_acc=Acc}=State) ->
    {ok, State#gcst{doc_acc=[{Id, Seq, Doc} | Acc]}}.

finish_update(#gcst{doc_acc=Acc}=State) ->
    if Acc /= [] ->
        couch_work_queue:queue(State#gcst.doc_queue, Acc);
        true -> ok
    end,
    couch_work_queue:close(State#gcst.doc_queue),
    receive
        {new_state, NewState} ->
            {ok, NewState#gcst{
                first_build=undefined,
                partial_resp_pid=undefined,
                doc_acc=undefined,
                doc_queue=undefined,
                write_queue=undefined,
                query_server=nil
            }}
    end.

purge_index(_Db, PurgeSeq, PurgedIdRevs, State) ->
    #gcst{
        id_btree=IdBtree,
        indexes=Idxs
    } = State,

    Ids = [Id || {Id, _Revs} <- PurgedIdRevs],
    {ok, Lookups, IdBtree2} = couch_btree:query_modify(IdBtree, Ids, [], Ids),

    MakeDictFun = fun
        ({ok, {DocId, ViewNumRowKeys}}, DictAcc) ->
            FoldFun = fun({ViewNum, RowKey}, DictAcc2) ->
                dict:append(ViewNum, {RowKey, DocId}, DictAcc2)
            end,
            lists:foldl(FoldFun, DictAcc, ViewNumRowKeys);
        ({not_found, _}, DictAcc) ->
            DictAcc
    end,
    KeysToRemove = lists:foldl(MakeDictFun, dict:new(), Lookups),

    RemKeysFun = fun(Idx) ->
        #gcidx{
            id_num=Num,
            fd=Fd,
            vtree=Vt,
            vtree_height=Vh
        } = Idx,
        case dict:find(Num, KeysToRemove) of
            {ok, RemKeys} ->
                {ok, Vt2, Vh2} = vtree:add_remove(Fd, Vt, Vh, [], RemKeys),
                NewPurgeSeq = case Vt2 /= Vt orelse Vh2 /= Vh of
                    true -> PurgeSeq;
                    _ -> Idx#gcidx.purge_seq
                end,
                Idx#gcidx{vtree=Vt2, vtree_height=Vh2, purge_seq=NewPurgeSeq};
            error ->
                Idx
        end
    end,

    {ok, State#gcst{
        id_btree=IdBtree2,
        indexes=lists:map(RemKeysFun, Idxs),
        purge_seq=PurgeSeq
    }}.

% ----------------------------------------------------------------------------

map_docs(State) ->
    case couch_work_queue:dequeue(State#gcst.doc_queue) of
        closed ->
            couch_query_servers:stop_doc_map(State#gcst.query_server),
            couch_work_queue:close(State#gcst.write_queue);
        {ok, Dequeued} ->
            % Run all the non deleted docs through the view engine and
            % then pass the results on to the writer process.
            DocFun = fun
                ({nil, Seq, _}, Results) ->
                    KVs = [{I#gcidx.id_num, []} || I <- State#gcst.indexes],
                    [{Seq, KVs, []} | Results];
                ({Id, Seq, deleted}, Results) ->
                    KVs = [{I#gcidx.id_num, []} || I <- State#gcst.indexes],
                    [{Seq, KVs, [{Id, []}]} | Results];
                ({Id, Seq, Doc}, Results) ->
                    GeoDoc = geodoc_convert(State#gcst.query_server, Doc),
                    {ViewKVs, DocIdKeys} = process_results(Id, State#gcst.indexes, GeoDoc),
                    [{Seq, ViewKVs, DocIdKeys} | Results]
            end,
            FoldFun = fun(Docs, Acc) ->
                update_task(length(Docs)),
                lists:foldl(DocFun, Acc, Docs)
            end,
            Results = lists:foldl(FoldFun, [], Dequeued),
            couch_work_queue:queue(State#gcst.write_queue, Results),
            map_docs(State)
    end.

write_results(Parent, State) ->
    case couch_work_queue:dequeue(State#gcst.write_queue) of
        closed ->
            Parent ! {new_state, State};
        {ok, Info} ->
            {Seq, ViewKVs, DocIdKeys} = lists:foldl(fun merge_kvs/2, nil, lists:flatten(Info)),
            NewState = write_kvs(State, Seq, ViewKVs, DocIdKeys),
            send_partial(NewState#gcst.partial_resp_pid, NewState),
            write_results(Parent, NewState)
    end.

% ----------------------------------------------------------------------------

start_query_server(State) ->
    #gcst{
        language=Language,
        lib=Lib,
        indexes=Idxs
    } = State,
    Defs = [I#gcidx.def || I <- Idxs],
    {ok, QServer} = couch_query_servers:start_doc_map(Language, Defs, Lib),
    State#gcst{query_server=QServer}.

geodoc_convert(Proc, Doc) ->
    Json = couch_doc:to_json_obj(Doc, []),
    FunsResults = couch_query_servers:proc_prompt(
        Proc, [<<"map_doc">>, Json]
    ),

    % the results are a json array of function map yields like this:
    % [FunResults1, FunResults2 ...]
    % where funresults is are json arrays of key value pairs:
    % [[Geom1, Value1], [Geom2, Value2]]
    % Convert the key, value pairs to tuples like
    % [{Bbox1, {Geom1, Value1}}, {Bbox, {Geom2, Value2}}]
    lists:map(fun process_js_results/1, FunsResults).


process_js_results(Result) ->
    JsToBBox = fun([Geo, Value]) ->
        BBox = get_bbox(Geo, nil),
        Geom = geocouch_util:from_geojson(Geo),
        {list_to_tuple(BBox), {Geom, Value}}
    end,
    lists:map(JsToBBox, Result).


get_bbox({Geo}, BBox) ->
    Type = couch_util:get_value(<<"type">>, Geo),
    case Type of
        <<"GeometryCollection">> ->
            Geometries = couch_util:get_value(<<"geometries">>, Geo),
            lists:foldl(fun get_bbox/2, BBox, Geometries);
        _ ->
            Coords = couch_util:get_value(<<"coordinates">>, Geo),
            case couch_util:get_value(<<"bbox">>, Geo) of
                undefined -> geocouch_util:make_bbox(Type, Coords);
                BBox2 -> BBox2
            end
    end.


process_results(DocId, Idxs, Results) ->
    process_results(DocId, Idxs, Results, {[], dict:new()}).

process_results(_, [], [], {KVAcc, ByIdAcc}) ->
    {lists:reverse(KVAcc), dict:to_list(ByIdAcc)};
process_results(DocId, [I | RIdxs], [KVs | RKVs], {KVAcc, ByIdAcc}) ->
    CombineDupsFun = fun
        ({Key, Val}, [{Key, {dups, Vals}} | Rest]) ->
            [{Key, {dups, [Val | Vals]}} | Rest];
        ({Key, Val1}, [{Key, Val2} | Rest]) ->
            [{Key, {dups, [Val1, Val2]}} | Rest];
        (KV, Rest) ->
            [KV | Rest]
    end,
    Duped = lists:foldl(CombineDupsFun, [], lists:sort(KVs)),

    ViewKVs0 = {I#gcidx.id_num, [{{Key, DocId}, Val} || {Key, Val} <- Duped]},
    ViewKVs = [ViewKVs0 | KVAcc],
    AddDocIdKeys = fun({Key, _}, ByIdAcc0) ->
        dict:append(DocId, {I#gcidx.id_num, Key}, ByIdAcc0)
    end,
    ByIdAcc1 = lists:foldl(AddDocIdKeys, ByIdAcc, Duped),
    process_results(DocId, RIdxs, RKVs, {ViewKVs, ByIdAcc1}).


merge_kvs({Seq, ViewKVs, DocIdKeys}, nil) ->
    {Seq, ViewKVs, DocIdKeys};
merge_kvs({Seq, ViewKVs, DocIdKeys}, {SeqAcc, ViewKVsAcc, DocIdKeysAcc}) ->
    KVCombine = fun({ViewNum, KVs}, {ViewNum, KVsAcc}) ->
        {ViewNum, KVs ++ KVsAcc}
    end,
    ViewKVs2 = lists:zipwith(KVCombine, ViewKVs, ViewKVsAcc),
    {max(Seq, SeqAcc), ViewKVs2, DocIdKeys ++ DocIdKeysAcc}.


write_kvs(State, UpdateSeq, ViewKVs, DocIdKeys) ->
    #gcst{
        id_btree=IdBtree,
        first_build=FirstBuild
    } = State,

    {ok, ToRemove, IdBtree2} = update_id_btree(IdBtree, DocIdKeys, FirstBuild),
    ToRemByView = collapse_rem_keys(ToRemove, dict:new()),

    UpdateIdx = fun(#gcidx{id_num=IdxId}=Idx, {IdxId, KVs}) ->
        #gcidx{fd=Fd, vtree=Vt, vtree_height=Vh} = Idx,
        ToRem = couch_util:dict_find(IdxId, ToRemByView, []),
        Ordered = lists:reverse(KVs),
        % io:format("Ordered: ~p~n", [Ordered]),
        {ok, Vt2, Vh2} = vtree:add_remove(Fd, Vt, Vh, Ordered, ToRem),
        NewUpdateSeq = case Vt2 /= Vt orelse Vh2 /= Vh of
            true -> UpdateSeq;
            _ -> Idx#gcidx.update_seq
        end,
        Idx#gcidx{vtree=Vt2, vtree_height=Vh2, update_seq=NewUpdateSeq}
    end,

    State#gcst{
        indexes=lists:zipwith(UpdateIdx, State#gcst.indexes, ViewKVs),
        update_seq=UpdateSeq,
        id_btree=IdBtree2
    }.


update_id_btree(Btree, DocIdKeys, true) ->
    ToAdd = [{Id, DIKeys} || {Id, DIKeys} <- DocIdKeys, DIKeys /= []],
    couch_btree:query_modify(Btree, [], ToAdd, []);
update_id_btree(Btree, DocIdKeys, _) ->
    ToFind = [Id || {Id, _} <- DocIdKeys],
    ToAdd = [{Id, DIKeys} || {Id, DIKeys} <- DocIdKeys, DIKeys /= []],
    ToRem = [Id || {Id, DIKeys} <- DocIdKeys, DIKeys == []],
    couch_btree:query_modify(Btree, ToFind, ToAdd, ToRem).


collapse_rem_keys([], Acc) ->
    Acc;
collapse_rem_keys([{ok, {DocId, ViewIdKeys}} | Rest], Acc) ->
    NewAcc = lists:foldl(fun({ViewId, Key}, Acc2) ->
        dict:append(ViewId, {Key, DocId}, Acc2)
    end, Acc, ViewIdKeys),
    collapse_rem_keys(Rest, NewAcc);
collapse_rem_keys([{not_found, _} | Rest], Acc) ->
    collapse_rem_keys(Rest, Acc).


send_partial(Pid, State) when is_pid(Pid) ->
    gen_server:cast(Pid, {new_state, State});
send_partial(_, _) ->
    ok.

update_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress = case Total of
        0 ->
            % updater restart after compaction finishes
            0;
        _ ->
            (Changes2 * 100) div Total
    end,
    couch_task_status:update([{progress, Progress}, {changes_done, Changes2}]).
