%%% -*- erlang -*-
%%%
%%% This file is part of geocouch released under the Apache license 2. 
%%% See the NOTICE for more information.

-module(geocouch).

-export([spatial_query/3, spatial_query/4, spatial_query/6]).
-export([count/4, get_info/2, compact/2, cleanup/1]).

-include("geocouch.hrl").


-record(gcacc, {
    db,
    idx,
    meta_sent=false,
    callback,
    user_acc,
    last_go=ok,
    args
}).


spatial_query(Db, DDoc, SName) ->
    spatial_query(Db, DDoc, SName, #gcargs{}).


spatial_query(Db, DDoc, SName, Args) when is_list(Args) ->
    spatial_query(Db, DDoc, SName, to_gcargs(Args), fun default_cb/2, []);
spatial_query(Db, DDoc, SName, Args) ->
    spatial_query(Db, DDoc, SName, Args, fun default_cb/2, []).


spatial_query(Db, DDoc, SName, Args, Callback, Acc) when is_list(Args) ->
    spatial_query(Db, DDoc, SName, to_gcargs(Args), Callback, Acc);
spatial_query(Db, DDoc, SName, Args0, Callback, Acc0) ->
    {ok, Idx, Sig, Args} = geocouch_util:get_index(Db, DDoc, SName, Args0),
    {ok, Acc1} = case Args#gcargs.preflight_fun of
        PFFun when is_function(PFFun, 2) -> PFFun(Sig, Acc0);
        _ -> {ok, Acc0}
    end,
    spatial_fold(Db, Idx, Args, Callback, Acc1).


default_cb(complete, Acc) ->
    {ok, lists:reverse(Acc)};
default_cb(Row, Acc) ->
    {ok, [Row | Acc]}.


count(Db, DDoc, SName, Args0) ->
    {ok, Idx, _, Args} = geocouch_util:get_index(Db, DDoc, SName, Args0),
    geocouch_util:count(Idx, Args).


get_info(Db, DDoc) ->
    {ok, Pid} = geocouch_util:get_indexer_pid(Db, DDoc),
    couch_index:get_info(Pid).


compact(Db, DDoc) ->
    {ok, Pid} = geocouch_util:get_indexer_pid(Db, DDoc),
    couch_index:compact(Pid).


cleanup(Db) ->
    geocouch_cleanup:run(Db).


spatial_fold(Db, Idx, Args, Callback, Acc) ->
    Acc0 = #gcacc{
        db=Db,
        idx=Idx,
        callback=Callback,
        user_acc=Acc,
        args=Args
    },
    #gcargs{
        bbox=BBox,
        bounds=Bounds
    } = Args,
    {ok, Acc1} = geocouch_util:fold(Idx, fun spatial_fold/2, Acc0, BBox,Bounds),
    finish_fold(Acc1, Idx).


spatial_fold(Row, #gcacc{meta_sent=false}=Acc) ->
    #gcacc{
        idx=Idx,
        callback=Callback,
        user_acc=UAcc0
    } = Acc,
    Meta = make_meta(Idx),
    {Go, UAcc1} = Callback(Meta, UAcc0),
    Acc1 = Acc#gcacc{meta_sent=true, user_acc=UAcc1, last_go=Go},
    case Go of
        ok -> spatial_fold(Row, Acc1);
        stop -> {stop, Acc1}
    end;
spatial_fold({{BBox, DocId}, {Geom, Val}}, Acc) ->
    #gcacc{
        callback=Callback,
        user_acc=UAcc0
    } = Acc,
    Row = [{id, DocId}, {bbox, BBox}, {geometry, Geom}, {val, Val}],
    {Go, UAcc1} = Callback({row, Row}, UAcc0),
    {Go, Acc#gcacc{user_acc=UAcc1, last_go=Go}}.


finish_fold(#gcacc{last_go=ok}=Acc, Idx) ->
    #gcacc{callback=Callback, user_acc=UAcc}=Acc,
    Meta = make_meta(Idx),
    {Go, UAcc1} = case Acc#gcacc.meta_sent of
        true -> {ok, UAcc};
        false -> Callback(Meta, UAcc)
    end,
    % Notify callback that the fold is complete.
    {_, UAcc2} = case Go of
        ok -> Callback(complete, UAcc1);
        _ -> {ok, UAcc1}
    end,
    {ok, UAcc2};
finish_fold(#gcacc{user_acc=UAcc}, _Idx) ->
    {ok, UAcc}.


make_meta(Idx) ->
    {ok, Total} = geocouch_util:get_row_count(Idx),
    {meta, [
        {total, Total},
        {update_seq, Idx#gcidx.update_seq}
    ]}.


to_gcargs(KeyList) ->
    lists:foldl(fun({Key, Value}, Acc) ->
        Index = lookup_index(couch_util:to_existing_atom(Key)),
        setelement(Index, Acc, Value)
    end, #gcargs{}, KeyList).


lookup_index(Key) ->
    Index = lists:zip(
        record_info(fields, gcargs), lists:seq(2, record_info(size, gcargs))
    ),
    couch_util:get_value(Key, Index).
