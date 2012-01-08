%%% -*- erlang -*-
%%%
%%% This file is part of geocouch released under the Apache license 2. 
%%% See the NOTICE for more information.


-module(geocouch_compactor).

-include_lib("couch/include/couch_db.hrl").
-include("geocouch.hrl").

-export([compact/3, swap_compacted/2]).


compact(_Db, State, Opts) ->
    case lists:member(recompact, Opts) of
        false -> compact(State);
        true -> recompact(State)
    end.

compact(State) ->
    #gcst{
        root_dir=RootDir,
        db_name=DbName,
        idx_name=IdxName,
        sig=Sig,
        update_seq=Seq,
        id_btree=IdBtree,
        indexes=Idxs
    } = State,

    EmptyState = couch_util:with_db(DbName, fun(Db) ->
        CompactFName = geocouch_util:compaction_file(RootDir, DbName, Sig),
        {ok, Fd} = geocouch_util:open_file(CompactFName),
        geocouch_util:reset_index(Db, Fd, State)
    end),

    #gcst{
        id_btree = EmptyIdBtree,
        indexes = EmptyIdxs
    } = EmptyState,

    Count = couch_btree:full_reduce(IdBtree),
    couch_task_status:add_task([
        {type, view_compaction},
        {database, DbName},
        {design_document, IdxName},
        {progress, 0}
    ]),


    BufferSize0 = couch_config:get(
        "view_compaction", "kevalue_buffer_size", "2097152"
    ),
    BufferSize = list_to_integer(BufferSize0),

    FoldFun = fun({DocId, _}=KV, {Bt, Acc, AccSize, Copied, LastId}) ->
        if DocId =:= LastId ->
            % COUCHDB-999 regression test
            ?LOG_ERROR("Duplicate docid `~s` detected in view group `~s`"
                ++ ", database `~s` - This view needs to be rebuilt.",
                [DocId, IdxName, DbName]
            ),
            exit({view_duplicate_id, DocId});
        true -> ok end,
        AccSize2 = AccSize + ?term_size(KV),
        case AccSize2 >= BufferSize of
            true ->
                {ok, Bt2} = couch_btree:add(Bt, lists:reverse([KV|Acc])),
                couch_task_status:update("Copied ~p of ~p Ids (~p%)",
                    [Copied, Count, (Copied * 100) div Count]),
                {ok, {Bt2, [], 0, Copied+1+length(Acc), DocId}};
            _ ->
                {ok, {Bt, [KV | Acc], AccSize2, Copied, DocId}}
        end
    end,


    InitAcc = {EmptyIdBtree, [], 0, 0, nil},
    {ok, _, FinalAcc} = couch_btree:foldl(IdBtree, FoldFun, InitAcc),
    {Bt3, Uncopied, _, _, _} = FinalAcc,
    {ok, NewIdBtree} = couch_btree:add(Bt3, lists:reverse(Uncopied)),

    NewIdxs = lists:map(fun({Idx, EmptyIdx}) ->
        compact_index(Idx, EmptyIdx, BufferSize)
    end, lists:zip(Idxs, EmptyIdxs)),

    unlink(EmptyState#gcst.fd),
    {ok, EmptyState#gcst{
        id_btree=NewIdBtree,
        indexes=NewIdxs,
        update_seq=Seq
    }}.


recompact(State) ->
    #gcst{
        db_name=DbName,
        idx_name=IdxName,
        update_seq=UpdateSeq
    } = State,
    link(State#gcst.fd),
    ?LOG_INFO("Recompacting index ~s ~s at ~p", [DbName, IdxName, UpdateSeq]),
    {_Pid, Ref} = erlang:spawn_monitor(fun() ->
        couch_index_updater:update(geocouch_index, State)
    end),
    receive
        {'DOWN', Ref, _, _, {updated, State2}} ->
            unlink(State#gcst.fd),
            {ok, State2}
    end.


compact_index(Idx, #gcidx{fd=EFd}=EmptyIdx, BufferSize) ->
    {ok, Count} = geocouch_util:get_row_count(Idx),
    FoldFun = fun(Node, {Vt, Vh, Acc, AccSize, Copied}) ->
        AccSize2 = AccSize + ?term_size(Node),
        case AccSize2 >= BufferSize of
            true ->
                {ok, Vt2, Vh2} = vtree_bulk:bulk_load(EFd, Vt, Vh, [Node|Acc]),
                couch_task_status:update("Spatial #~p: copied ~p of ~p (~p%)",
                    [Idx#gcidx.id_num, Copied, Count, (Copied*100) div Count]),
                {Vt2, Vh2, [], 0, Copied+1};
            _ ->
                {Vt, Vh, [Node|Acc], AccSize2, Copied}
        end
    end,

    #gcidx{fd=Fd, vtree=Vt0} = Idx,
    InitAcc = {EmptyIdx#gcidx.vtree, EmptyIdx#gcidx.vtree_height, [], 0, 0},
    {Vt3, Vh3, Uncopied, _, _} = vtree:foldl(Fd, Vt0, FoldFun, InitAcc),
    {ok, Vt4, Vh4} = vtree_bulk:bulk_load(EFd, Vt3, Vh3, Uncopied),

    EmptyIdx#gcidx{vtree=Vt4, vtree_height=Vh4}.

swap_compacted(OldState, NewState) ->
    #gcst{
        sig=Sig,
        db_name=DbName,
        idx_name=IdxName,
        root_dir=RootDir
    } = NewState,
    ?LOG_INFO("Spatial index compaction complete for ~s ~s", [DbName, IdxName]),

    unlink(OldState#gcst.fd),
    link(NewState#gcst.fd),

    IndexFName = geocouch_util:index_file(RootDir, DbName, Sig),
    CompactFName = geocouch_util:compaction_file(RootDir, DbName, Sig),

    io:format("~p~n~p~n", [IndexFName, CompactFName]),
    couch_file:close(OldState#gcst.fd),
    ok = couch_file:delete(RootDir, IndexFName),
    ok = file:rename(CompactFName, IndexFName),

    {ok, NewState}.
