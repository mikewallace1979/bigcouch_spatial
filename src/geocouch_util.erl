%%% -*- erlang -*-
%%%
%%% This file is part of geocouch released under the Apache license 2. 
%%% See the NOTICE for more information.

-module(geocouch_util).


-include_lib("couch/include/couch_db.hrl").
-include("geocouch.hrl").


-export([get_indexer_pid/2, get_index/4]).
-export([init_state/4, make_header/1]).
-export([count/2]).
-export([index_file/3, compaction_file/3, open_file/1]).
-export([delete_files/3, delete_index_file/3, delete_compaction_file/3]).
-export([reset_index/3]).
-export([get_row_count/1, fold/5]).
-export([get_req_handler/1, hexsig/1]).
-export([from_geojson/1, to_geojson/1, make_bbox/2, make_bbox/3]).
-export([ddoc_to_gcst/2]).

get_indexer_pid(DbName, DDoc) when is_binary(DbName) ->
    couch_util:with_db(DbName, fun(Db) -> get_indexer_pid(Db, DDoc) end);
get_indexer_pid(Db, DDoc) when is_binary(DDoc) ->
    case couch_db:open_doc(Db, DDoc, [ejson_body]) of
        {ok, Doc} -> get_indexer_pid(Db, Doc);
        Error -> Error
    end;
get_indexer_pid(Db, DDoc) ->
    InitState = ddoc_to_gcst(couch_db:name(Db), DDoc),
    couch_index_server:get_index(geocouch_index, InitState).


get_index(Db, DDoc, SpatialName, Args0) ->
    Args1 = validate_args(Args0),
    MinSeq = case Args1#gcargs.stale of
        ok -> 0;
        update_after -> 0;
        _ -> couch_db:get_update_seq(Db)
    end,
    {ok, Pid} = get_indexer_pid(Db, DDoc),
    {ok, State} = case couch_index:get_state(Pid, MinSeq) of
        {ok, _} = Resp -> Resp;
        Error -> throw(Error)
    end,
    UpdateAfterFun = fun() ->
        LastSeq = couch_db:get_update_seq(Db),
        catch couch_index:get_state(Pid, LastSeq)
    end,
    case Args1#gcargs.stale of
        update_after -> spawn(UpdateAfterFun);
        _ -> ok
    end,
    Idx = extract_index(SpatialName, State#gcst.indexes),
    Sig = index_sig(Db, State, Idx, Args1),
    {ok, Idx, Sig, Args1}.


count(Idx, Args) ->
    #gcidx{fd=Fd, vtree=Vtree} = Idx,
    #gcargs{bbox=BBox} = Args,
    vtree:count_lookup(Fd, Vtree, BBox).


ddoc_to_gcst(DbName, #doc{id=Id, body={Fields}}) ->
    MakeDict = fun
        ({Name, IdxSrc}, DictBySrcAcc) when is_binary(IdxSrc) ->
            Idx = case dict:find(IdxSrc, DictBySrcAcc) of
                {ok, Idx0} -> Idx0;
                error -> #gcidx{def=IdxSrc}
            end,
            Idx2 = Idx#gcidx{idx_names=[Name | Idx#gcidx.idx_names]},
            dict:store(IdxSrc, Idx2, DictBySrcAcc);
        (_, DictBySrcAcc) ->
            DictBySrcAcc
    end,

    RootDir = couch_config:get("couchdb", "index_dir"),
    Language = couch_util:get_value(<<"language">>, Fields, <<"javascript">>),
    {DesignOpts} = couch_util:get_value(<<"options">>, Fields, {[]}),
    {RawIndexes} = couch_util:get_value(<<"spatial">>, Fields, {[]}),
    {RawViews} = couch_util:get_value(<<"views">>, Fields, {[]}),
    Lib = couch_util:get_value(<<"lib">>, RawViews, {[]}),

    BySrc = lists:foldl(MakeDict, dict:new(), RawIndexes),
    NumIdxs = fun({_, Idx}, N) -> {Idx#gcidx{id_num=N}, N+1} end,
    {Idxs, _} = lists:mapfoldl(NumIdxs, 0, lists:sort(dict:to_list(BySrc))),

    IdxState = #gcst{
        db_name=DbName,
        idx_name=Id,
        language=Language,
        lib=Lib,
        indexes=Idxs,
        design_opts=DesignOpts,
        root_dir=RootDir
    },

    set_index_sig(IdxState).


set_index_sig(State) ->
    #gcst{
        indexes=Idxs,
        lib=Lib,
        language=Language,
        design_opts=DesignOpts
    } = State,
    SigInfo = {Idxs, Language, DesignOpts, sort_lib(Lib), ?GEOCOUCH_DISK_VSN},
    State#gcst{sig=couch_util:md5(term_to_binary(SigInfo))}.


init_state(Db, Fd, #gcst{indexes=Idxs}=State, nil) ->
    Header = #gcheader{
        update_seq=0,
        purge_seq=couch_db:get_purge_seq(Db),
        id_btree_state=nil,
        idx_states=[#gcidx{} || _ <- Idxs]
    },
    init_state(Db, Fd, State, Header);
init_state(_Db, Fd, State, Header) ->
    #gcst{indexes=Idxs} = State,
    #gcheader{
        update_seq=Seq,
        purge_seq=PurgeSeq,
        id_btree_state=IdBtreeState,
        idx_states=IdxStates
    } = Header,
    {ok, IdBtree} = couch_btree:open(IdBtreeState, Fd),

    Idxs2 = lists:zipwith(fun(St, Idx) ->
        Idx#gcidx{
            fd=Fd,
            vtree=St#gcidx.vtree,
            vtree_height=St#gcidx.vtree_height,
            update_seq=St#gcidx.update_seq,
            purge_seq=St#gcidx.purge_seq
        }
    end, IdxStates, Idxs),

    State#gcst{
        fd=Fd,
        update_seq=Seq,
        purge_seq=PurgeSeq,
        id_btree=IdBtree,
        indexes=Idxs2
    }.


make_header(State) ->
    #gcst{
        update_seq=UpdateSeq,
        purge_seq=PurgeSeq,
        id_btree=IdBtree,
        indexes=Idxs
    } = State,
    IdxStates = [
        #gcidx{
            vtree=I#gcidx.vtree,
            vtree_height=I#gcidx.vtree_height,
            update_seq=I#gcidx.update_seq,
            purge_seq=I#gcidx.purge_seq
        }
        || I <- Idxs
    ],
    #gcheader{
        update_seq=UpdateSeq,
        purge_seq=PurgeSeq,
        id_btree_state=couch_btree:get_state(IdBtree),
        idx_states=IdxStates
    }.


extract_index(_SpatialName, []) ->
    throw({not_found, missing_named_index});
extract_index(SpatialName, [Idx | Rest]) ->
    case lists:member(SpatialName, Idx#gcidx.idx_names) of
        true -> Idx;
        _ -> extract_index(SpatialName, Rest)
    end.


index_sig(_Db, State, Idx, Args0) ->
    Sig = State#gcst.sig,
    UpdateSeq = Idx#gcidx.update_seq,
    PurgeSeq = Idx#gcidx.purge_seq,
    Args = Args0#gcargs{
        preflight_fun=undefined,
        extra=[]
    },
    Bin = term_to_binary({Sig, UpdateSeq, PurgeSeq, Args}),
    hexsig(couch_util:md5(Bin)).



index_file(RootDir, DbName, Sig) ->
    design_root(RootDir, DbName) ++ hexsig(Sig) ++ ".view".


compaction_file(RootDir, DbName, Sig) ->
    design_root(RootDir, DbName) ++ hexsig(Sig) ++ ".compact.view".


design_root(RootDir, DbName) ->
    RootDir ++ "/." ++ binary_to_list(DbName) ++ "_design/".


open_file(FName) ->
    case couch_file:open(FName) of
        {ok, Fd} -> {ok, Fd};
        {error, enoent} -> couch_file:open(FName, [create]);
        Error -> Error
    end.


delete_files(RootDir, DbName, Sig) ->
    delete_index_file(RootDir, DbName, Sig),
    delete_compaction_file(RootDir, DbName, Sig).


delete_index_file(RootDir, DbName, Sig) ->
    delete_file(index_file(RootDir, DbName, Sig)).


delete_compaction_file(RootDir, DbName, Sig) ->
    delete_file(compaction_file(RootDir, DbName, Sig)).


delete_file(FName) ->
    case filelib:is_file(FName) of
        true ->
            RootDir = couch_index_util:root_dir(),
            couch_file:delete(RootDir, FName);
        _ ->
            ok
    end.

reset_index(Db, Fd, #gcst{sig=Sig}=State) ->
    ok = couch_file:truncate(Fd, 0),
    ok = couch_file:write_header(Fd, {Sig, nil}),
    init_state(Db, Fd, reset_state(State), nil).


reset_state(State) ->
    Idxs = [
        I#gcidx{
            fd=nil,
            vtree=nil,
            vtree_height=0}
        || I <- State#gcst.indexes
    ],
    State#gcst{
        fd=nil,
        query_server=nil,
        update_seq=0,
        id_btree=nil,
        indexes=Idxs
    }.


get_row_count(Idx) ->
    Count = vtree:count_total(Idx#gcidx.fd, Idx#gcidx.vtree),
    {ok, Count}.


fold(Idx, Fun, InitAcc, BBox, Bounds) ->
    WrapperFun = fun(Node, Acc) ->
        fold_fun(Fun, expand_dups([Node], []), Acc)
    end,
    {_, Acc2} = vtree:lookup(
        Idx#gcidx.fd, Idx#gcidx.vtree, BBox, {WrapperFun, InitAcc}, Bounds
    ),
    {ok, Acc2}.


fold_fun(_Fun, [], Acc) ->
    {ok, Acc};
fold_fun(Fun, [Item | Rest], Acc0) ->
    case Fun(Item, Acc0) of
        {ok, Acc1} -> fold_fun(Fun, Rest, Acc1);
        {stop, Acc1} -> {stop, Acc1}
    end.


validate_args(Args) ->
    case Args#gcargs.bbox of
       BBox when is_tuple(BBox), size(BBox) == 4 -> ok;
       _ -> gcerror(<<"Invalid value for `bbox`">>)
    end,

    case Args#gcargs.stale of
        undefined -> ok;
        ok -> ok;
        update_after -> ok;
        _ -> gcerror(<<"Invalid value for `stale`">>)
    end,

    case is_boolean(Args#gcargs.count) of
        true -> ok;
        _ -> gcerror(<<"Invalid value for `count`">>)
    end,

    case {Args#gcargs.bbox, Args#gcargs.bounds} of
        {{W, S, E, N}, nil} when E < W; N < S ->
            Msg = <<
                "Coordinates of the bounding box are flipped, but no bounds "
                "for the cartesian plane were specified "
                "(use the `plane_bounds` parameter)"
            >>,
            throw({query_parse_error, Msg});
        _ ->
            ok
    end,

    Args.


get_req_handler(SubReqHandler) ->
    SpecStr = couch_config:get("httpd_design_handlers", SubReqHandler),
    {ok, {Mod, Fun}} = couch_util:parse_term(SpecStr),
    fun(A1, A2, A3) -> Mod:Fun(A1, A2, A3) end.


from_geojson({Geom}) ->
    Type = couch_util:get_value(<<"type">>, Geom),
    Coords = case Type of
        <<"GeometryCollection">> ->
            Geoms = couch_util:get_value(<<"geometries">>, Geom),
            lists:map(fun from_geojson/1, Geoms);
        _ ->
            couch_util:get_value(<<"coordinates">>, Geom)
    end,
    {couch_util:to_existing_atom(Type), Coords}.


to_geojson({Type, Coords}) ->
    Coords2 = case Type of
        'GeometryCollection' ->
            Geoms = [to_geojson(C) || C <- Coords],
            {<<"geometries">>, Geoms};
        _ ->
            {<<"coordinates">>, Coords}
    end,
    {[{<<"type">>, Type}, Coords2]}.


make_bbox(Type, Coords) ->
    make_bbox(Type, Coords, nil).


make_bbox(Type, Coords, InitBbox) ->
    case Type of
        <<"Point">> ->
            bbox([Coords], InitBbox);
        <<"LineString">> ->
            bbox(Coords, InitBbox);
        <<"Polygon">> ->
            bbox(hd(Coords), InitBbox);
        <<"MultiPoint">> ->
            bbox(Coords, InitBbox);
        <<"MultiLineString">> ->
            lists:foldl(fun(Linestring, CurBbox) ->
                bbox(Linestring, CurBbox)
            end, InitBbox, Coords);
        <<"MultiPolygon">> ->
            lists:foldl(fun(Polygon, CurBbox) ->
                bbox(hd(Polygon), CurBbox)
            end, InitBbox, Coords)
    end.


bbox([], {Min, Max}) ->
    Min ++ Max;
bbox([Coords | Rest], nil) ->
    bbox(Rest, {Coords, Coords});
bbox(Coords, Bbox) when is_list(Bbox)->
    MinMax = lists:split(length(Bbox) div 2, Bbox),
    bbox(Coords, MinMax);
bbox([Coords | Rest], {Min, Max}) ->
    Min2 = lists:zipwith(fun(X, Y) -> min(X, Y) end, Coords, Min),
    Max2 = lists:zipwith(fun(X, Y) -> max(X, Y) end, Coords, Max),
    bbox(Rest, {Min2, Max2}).


sort_lib({Lib}) ->
    sort_lib(Lib, []).
sort_lib([], LAcc) ->
    lists:keysort(1, LAcc);
sort_lib([{LName, {LObj}}|Rest], LAcc) ->
    LSorted = sort_lib(LObj, []), % descend into nested object
    sort_lib(Rest, [{LName, LSorted}|LAcc]);
sort_lib([{LName, LCode}|Rest], LAcc) ->
    sort_lib(Rest, [{LName, LCode}|LAcc]).


expand_dups([], Acc) ->
    lists:reverse(Acc);
expand_dups([{Key, {dups, Vals}} | Rest], Acc) ->
    Expanded = [{Key, Val} || Val <- Vals],
    expand_dups(Rest, Expanded ++ Acc);
expand_dups([KV | Rest], Acc) ->
    expand_dups(Rest, [KV | Acc]).


hexsig(Sig) ->
    couch_util:to_hex(binary_to_list(Sig)).


gcerror(Mesg) ->
    throw({query_parse_error, Mesg}).
