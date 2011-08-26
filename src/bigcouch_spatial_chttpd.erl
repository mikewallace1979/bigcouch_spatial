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

-module(bigcouch_spatial_chttpd).

-export([handle_spatial_req/3, handle_spatial_list_req/3]).

-include("couch_spatial.hrl").
-include_lib("chttpd/include/chttpd.hrl").
-include_lib("couch/include/couch_db.hrl").

-import(chttpd,
    [send_json/2,send_json/3,send_json/4,send_method_not_allowed/2,send_chunk/2,
    start_json_response/2, start_json_response/3, end_json_response/1,
    send_chunked_error/2]).


%% _spatial/_list handler (for compatibility with geocouch)
handle_spatial_req(#httpd{method='GET', path_parts=[_, _,
            _, _, <<"_list">>, ListName, SpatialName]}=Req,
            Db, DDoc) ->
    handle_spatial_list(Req, ListName, SpatialName, Db, DDoc);

%% _spatial handler
handle_spatial_req(#httpd{method='GET', 
        path_parts=[_, _, _, _, SpatialName]}=Req, Db, DDoc) ->
    QueryArgs = parse_spatial_params(Req),
    Etag = couch_uuids:new(),
    chttpd:etag_respond(Req, Etag, fun() ->
        {ok, Resp} = start_json_response(Req, 200, [{"Etag",Etag}]),
        CB = fun spatial_callback/2,
        bigcouch_spatial:spatial(Db, DDoc, SpatialName, CB, {nil, Resp}, QueryArgs),
        chttpd:end_json_response(Resp)
    end);
handle_spatial_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET,HEAD").


%% _spatial_list handler
handle_spatial_list_req(#httpd{method='GET', path_parts=[_, _,
            _, _, ListName, SpatialName]}=Req, Db, DDoc) ->
    handle_spatial_list(Req, ListName, SpatialName, Db, DDoc);

handle_spatial_list_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET,HEAD").

handle_spatial_list(Req, ListName, SpatialName, Db, DDoc) ->
    QueryArgs = parse_spatial_params(Req),
    Etag = couch_uuids:new(),
    CB = fun chttpd_show:list_callback/2,
    chttpd:etag_respond(Req, Etag, fun() ->    
        couch_query_servers:with_ddoc_proc(DDoc, fun(QServer) ->
            Acc0 = #lacc{
                lname = ListName,
                req = Req,
                qserver = QServer,
                db = Db,
                etag = Etag
            },
            bigcouch_spatial:spatial(Db, DDoc, SpatialName, CB, Acc0, QueryArgs)
        end)
    end).



spatial_callback({total, Total}, {nil, Resp}) ->
    Chunk = "{\"total_rows\":~p,\"rows\":[\r\n",
    send_chunk(Resp, io_lib:format(Chunk, [Total])),
    {ok, {"", Resp}};
spatial_callback({total, _}, Acc) ->
    % a sorted=false view where the message came in late.  Ignore.
    {ok, Acc};
spatial_callback({row, Row}, {nil, Resp}) ->
    % first row 
    send_chunk(Resp, ["{\"rows\":[\r\n", ?JSON_ENCODE(Row)]),
    {ok, {",\r\n", Resp}};
spatial_callback({row, Row}, {Prepend, Resp}) ->
    send_chunk(Resp, [Prepend, ?JSON_ENCODE(Row)]),
    {ok, {",\r\n", Resp}};
spatial_callback(complete, {nil, Resp}) ->
    send_chunk(Resp, "{\"rows\":[]}");
spatial_callback(complete, {_, Resp}) ->
    send_chunk(Resp, "\r\n]}");
spatial_callback({error, Reason}, {_, Resp}) ->
    {Code, ErrorStr, ReasonStr} = chttpd:error_info(Reason),
    Json = {[{code,Code}, {error,ErrorStr}, {reason,ReasonStr}]},
    send_chunk(Resp, [$\n, ?JSON_ENCODE(Json), $\n]).



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
    [{limit, parse_positive_int_param(Limit)}];
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

