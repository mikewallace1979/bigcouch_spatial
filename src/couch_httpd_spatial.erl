% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_httpd_spatial).
-include_lib("couch/include/couch_db.hrl").
-include("couch_spatial.hrl").

-export([handle_spatial_req/3, spatial_etag/3, spatial_etag/4,
         load_index/3, handle_compact_req/3, handle_design_info_req/3,
         handle_spatial_cleanup_req/2,
         parse_spatial_params/1]).

-import(couch_httpd,
        [send_json/2, send_json/3, send_method_not_allowed/2, send_chunk/2,
         start_json_response/2, start_json_response/3, end_json_response/1]).

% Either answer a normal spatial query, or keep dispatching if the path part
% after _spatial starts with an underscore.
handle_spatial_req(#httpd{
        path_parts=[_, _, _, _, SpatialName|_]}=Req, Db, DDoc) ->

    case SpatialName of
    % the path after _spatial starts with an underscore => dispatch
    <<$_,_/binary>> ->
        dispatch_sub_spatial_req(Req, Db, DDoc);
    _ ->
        handle_spatial(Req, Db, DDoc)
    end.

% the dispatching of endpoints below _spatial needs to be done manually
dispatch_sub_spatial_req(#httpd{
        path_parts=[_, _, _DName, Spatial, SpatialDisp|_]}=Req,
        Db, DDoc) ->
    Conf = couch_config:get("httpd_design_handlers",
        ?b2l(<<Spatial/binary, "/", SpatialDisp/binary>>)),
    Fun = geocouch_duplicates:make_arity_3_fun(Conf),
    apply(Fun, [Req, Db, DDoc]).

handle_spatial(#httpd{method='GET',
        path_parts=[_, _, DName, _, SpatialName]}=Req, Db, DDoc) ->
    ?LOG_DEBUG("Spatial query (~p): ~n~p", [DName, DDoc#doc.id]),
    #spatial_query_args{
        stale = Stale
    } = QueryArgs = parse_spatial_params(Req),
    {ok, Index, Group} = couch_spatial:get_spatial_index(
                           Db, DDoc#doc.id, SpatialName, Stale),
    output_spatial_index(Req, Index, Group, Db, QueryArgs);
handle_spatial(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET,HEAD").

% pendant is in couch_httpd_db
handle_compact_req(#httpd{method='POST',
        path_parts=[DbName, _ , DName|_]}=Req, Db, _DDoc) ->
    ok = couch_db:check_is_admin(Db),
    couch_httpd:validate_ctype(Req, "application/json"),
    ok = couch_spatial_compactor:start_compact(DbName, DName),
    send_json(Req, 202, {[{ok, true}]});
handle_compact_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "POST").

% pendant is in couch_httpd_db
handle_spatial_cleanup_req(#httpd{method='POST'}=Req, Db) ->
    % delete unreferenced index files
    ok = couch_db:check_is_admin(Db),
    couch_httpd:validate_ctype(Req, "application/json"),
    ok = couch_spatial:cleanup_index_files(Db),
    send_json(Req, 202, {[{ok, true}]});

handle_spatial_cleanup_req(Req, _Db) ->
    send_method_not_allowed(Req, "POST").

% pendant is in couch_httpd_db
handle_design_info_req(#httpd{
            method='GET',
            path_parts=[_DbName, _Design, DesignName, _, _]
        }=Req, Db, _DDoc) ->
    DesignId = <<"_design/", DesignName/binary>>,
    {ok, GroupInfoList} = couch_spatial:get_group_info(Db, DesignId),
    send_json(Req, 200, {[
        {name, DesignName},
        {spatial_index, {GroupInfoList}}
    ]});
handle_design_info_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET").


load_index(Req, Db, {DesignId, SpatialName}) ->
    QueryArgs = parse_spatial_params(Req),
    Stale = QueryArgs#spatial_query_args.stale,
    case couch_spatial:get_spatial_index(Db, DesignId, SpatialName, Stale) of
    {ok, Index, Group} ->
          {ok, Index, Group, QueryArgs};
    {not_found, Reason} ->
        throw({not_found, Reason})
    end.

%output_spatial_index(Req, Index, Group, Db,
%                     QueryArgs#spatial_query_args{count=true}) ->
output_spatial_index(Req, Index, Group, _Db, QueryArgs) when
        QueryArgs#spatial_query_args.count == true ->

    #spatial_query_args{
        bbox = Bbox,
        bounds = Bounds
    } = QueryArgs,
    Count = case Bounds of
        nil ->
            vtree:count_lookup(Group#spatial_group.fd,
                               Index#spatial.treepos,
                               Bbox);
        _ ->
            vtree:count_lookup(Group#spatial_group.fd,
                               Index#spatial.treepos,
                               Bbox, Bounds)
    end,
    send_json(Req, {[{"count",Count}]});

% counterpart in couch_httpd_view is output_map_view/6
output_spatial_index(Req, Index, Group, Db, QueryArgs) ->
    #spatial_query_args{
        bbox = Bbox,
        bounds = Bounds
    } = QueryArgs,
    CurrentEtag = spatial_etag(Db, Group, Index),

    Total = vtree:count_lookup(Group#spatial_group.fd,
                               Index#spatial.treepos,
                               Bbox),

    HelperFuns = #spatial_fold_helper_funs{
        start_response = fun json_spatial_start_resp/3,
        send_row = fun send_json_spatial_row/3,
        total_rows = Total
    },
    couch_httpd:etag_respond(Req, CurrentEtag, fun() ->
        FoldFun = make_spatial_fold_funs(
                    Req, QueryArgs, CurrentEtag, Db, HelperFuns),
        FoldAccInit = {undefined, ""},
        % In this case the accumulator consists of the response (which
        % might be undefined) and the actual accumulator we only care
        % about in spatiallist functions)
        {ok, {Resp, _Acc}} = couch_spatial:fold(
            Group, Index, FoldFun, FoldAccInit, Bbox, Bounds),
        finish_spatial_fold(Req, Resp)
    end).

% counterpart in couch_httpd_view is make_view_fold/7
make_spatial_fold_funs(Req, QueryArgs, Etag, Db, HelperFuns) ->
    #spatial_fold_helper_funs{
        start_response = StartRespFun,
        send_row = SendRowFun,
        total_rows = TotalRows
    } = HelperFuns,
    % The Acc is there to output characters that belong to the previous line,
    % but only if one line follows (think of a comma separated list which
    % doesn't have a comma at the last item)
    fun({{_Bbox, _DocId}, {_Geom, _Value}}=Row, {Resp, Acc}) ->
        Row0 = possibly_embed_doc(Db, QueryArgs, Row),
        case Resp of
        undefined ->
            {ok, NewResp, BeginBody} = StartRespFun(Req, Etag, TotalRows),
            {ok, Acc2} = SendRowFun(NewResp, Row0, BeginBody),
            {ok, {NewResp, Acc2}};
        Resp ->
            {ok, Acc2} = SendRowFun(Resp, Row0, Acc),
            {ok, {Resp, Acc2}}
        end
    end.



% counterpart in couch_httpd_view is finish_view_fold/5
finish_spatial_fold(Req, Resp) ->
    case Resp of
    % no response was sent yet
    undefined ->
        send_json(Req, 200, {[{"rows", []}]});
    Resp ->
        % end the index
        send_chunk(Resp, "\r\n]}"),
        end_json_response(Resp)
    end.


possibly_embed_doc(Db, #spatial_query_args{include_docs=true}, {{Bbox,
            DocId}, {Geom, Value}}) ->
    {Pid, Ref} = spawn_monitor(fun() -> 
                exit(couch_db:open_doc(Db, DocId, [])) end),
    {ok, NewDoc} =
    receive {'DOWN',Ref,process,Pid, Resp} ->
            Resp
    end,

    {{Bbox, DocId}, {Geom, Value, NewDoc}};
possibly_embed_doc(_Db, _QueryArgs, Row) ->
    Row.

% counterpart in couch_httpd_view is json_view_start_resp/6
json_spatial_start_resp(Req, Etag, TotalRows) ->
    {ok, Resp} = start_json_response(Req, 200, [{"Etag", Etag}]),
    BeginBody = io_lib:format(
            "{\"total_rows\":~w,\"rows\":[\r\n", [TotalRows]),
    {ok, Resp, BeginBody}.


% counterpart in couch_httpd_view is send_json_view_row/5
send_json_spatial_row(Resp, Row, RowFront) ->
    JsonObj = spatial_json_row_obj(Row),
    
    send_chunk(Resp, RowFront ++  ?JSON_ENCODE(JsonObj)),
    {ok, ",\r\n"}.


spatial_json_row_obj({{Bbox, DocId}, {Geom, Value, {error,Reason}}}) ->
    {[
        {<<"id">>, DocId},
        {<<"bbox">>, erlang:tuple_to_list(Bbox)},
        {<<"geometry">>, couch_spatial_updater:geocouch_to_geojsongeom(Geom)},
        {<<"value">>, Value},
        {<<"error">>, Reason}]};
spatial_json_row_obj({{Bbox, DocId}, {Geom, Value, Doc}}) ->
    JsonDoc = couch_doc:to_json_obj(Doc, []),
    {[
        {<<"id">>, DocId},
        {<<"bbox">>, erlang:tuple_to_list(Bbox)},
        {<<"geometry">>, couch_spatial_updater:geocouch_to_geojsongeom(Geom)},
        {<<"value">>, Value},
        {<<"doc">>, JsonDoc}]};
spatial_json_row_obj({{Bbox, DocId}, {Geom, Value}}) ->
    {[
        {<<"id">>, DocId},
        {<<"bbox">>, erlang:tuple_to_list(Bbox)},
        {<<"geometry">>, couch_spatial_updater:geocouch_to_geojsongeom(Geom)},
        {<<"value">>, Value}]}.


% counterpart in couch_httpd_view is view_group_etag/3 resp. /4
spatial_etag(Db, Group, Index) ->
    spatial_etag(Db, Group, Index, nil).
spatial_etag(_Db, #spatial_group{sig=Sig},
        #spatial{update_seq=UpdateSeq, purge_seq=PurgeSeq}, Extra) ->
    couch_httpd:make_etag({Sig, UpdateSeq, PurgeSeq, Extra}).

parse_spatial_params(Req) ->
    QueryList = couch_httpd:qs(Req),
    QueryParams = lists:foldl(fun({K, V}, Acc) ->
        parse_spatial_param(K, V) ++ Acc
    end, [], QueryList),
    QueryArgs = lists:foldl(fun({K, V}, Args2) ->
        validate_spatial_query(K, V, Args2)
    end, #spatial_query_args{}, lists:reverse(QueryParams)),

    #spatial_query_args{
        bbox = Bbox,
        bounds = Bounds
    } = QueryArgs,
    case {Bbox, Bounds} of
    % Coordinates of the bounding box are flipped and no bounds for the
    % cartesian plane were set
    {{W, S, E, N}, nil} when E < W; N < S ->
        Msg = <<"Coordinates of the bounding box are flipped, but no bounds "
                "for the cartesian plane were specified "
                "(use the `plane_bounds` parameter)">>,
        throw({query_parse_error, Msg});
    _ ->
        QueryArgs
    end.

parse_spatial_param("bbox", Bbox) ->
    [{bbox, list_to_tuple(?JSON_DECODE("[" ++ Bbox ++ "]"))}];
parse_spatial_param("stale", "ok") ->
    [{stale, ok}];
parse_spatial_param("stale", "update_after") ->
    [{stale, update_after}];
parse_spatial_param("stale", _Value) ->
    throw({query_parse_error,
            <<"stale only available as stale=ok or as stale=update_after">>});
parse_spatial_param("count", "true") ->
    [{count, true}];
parse_spatial_param("count", _Value) ->
    throw({query_parse_error, <<"count only available as count=true">>});
parse_spatial_param("plane_bounds", Bounds) ->
    [{bounds, list_to_tuple(?JSON_DECODE("[" ++ Bounds ++ "]"))}];
parse_spatial_param("limit", Limit) ->
    [{bounds, parse_positive_int_param(Limit)}];
parse_spatial_param("include_docs", Value) ->
    [{include_docs, parse_bool_param(Value)}];
parse_spatial_param(Key, Value) ->
    [{extra, {Key, Value}}].

validate_spatial_query(bbox, Value, Args) ->
    Args#spatial_query_args{bbox=Value};
validate_spatial_query(stale, ok, Args) ->
    Args#spatial_query_args{stale=ok};
validate_spatial_query(stale, update_after, Args) ->
    Args#spatial_query_args{stale=update_after};
validate_spatial_query(stale, _, Args) ->
    Args;
validate_spatial_query(count, true, Args) ->
    Args#spatial_query_args{count=true};
validate_spatial_query(bounds, Value, Args) ->
    Args#spatial_query_args{bounds=Value};
validate_spatial_query(limit, Value, Args) ->
    Args#spatial_query_args{limit=Value};
validate_spatial_query(include_docs, true, Args) ->
    Args#spatial_query_args{include_docs=true};
validate_spatial_query(extra, _Value, Args) ->
    Args.

parse_bool_param(Val) ->
    case string:to_lower(Val) of
    "true" -> true;
    "false" -> false;
    _ ->
        Msg = io_lib:format("Invalid boolean parameter: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

parse_int_param(Val) ->
    case (catch list_to_integer(Val)) of
    IntVal when is_integer(IntVal) ->
        IntVal;
    _ ->
        Msg = io_lib:format("Invalid value for integer parameter: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

parse_positive_int_param(Val) ->
    case parse_int_param(Val) of
    IntVal when IntVal >= 0 ->
        IntVal;
    _ ->
        Fmt = "Invalid value for positive integer parameter: ~p",
        Msg = io_lib:format(Fmt, [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.
