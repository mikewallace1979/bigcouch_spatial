%%% -*- erlang -*-
%%%
%%% This file is part of geocouch released under the Apache license 2. 
%%% See the NOTICE for more information.

-module(geocouch_show).

-export([handle_spatial_list_req/3]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("geocouch/include/geocouch.hrl").

-record(sacc, {
    db,
    req,
    resp,
    qserver,
    lname,
    etag
}).


handle_spatial_list_req(#httpd{method='GET'}=Req, Db, DDoc) ->
    case Req#httpd.path_parts of
        [_, _, _DName, _, _, LName, SName] ->
            handle_spatial_list(Req, Db, DDoc, LName, DDoc, SName);
        [_, _, _, _, _, LName, DName, SName] ->
            SDocId = <<"_design/", DName/binary>>,
            {ok, SDDoc} = couch_db:open_doc(Db, SDocId, [ejson_body]),
            handle_spatial_list(Req, Db, DDoc, LName, SDDoc, SName);
        _ ->
            couch_httpd:send_error(Req, 404, <<"list_error">>, <<"Bad path.">>)
    end;
handle_spatial_list_req(Req, _Db, _DDoc) ->
    couch_httpd:send_method_not_allowed(Req, "GET,HEAD").


handle_spatial_list(Req, Db, DDoc, LName, SDDoc, SName) ->
    Args0 = geocouch_http:parse_qs(Req),
    ETagFun = fun(BaseSig, Acc0) ->
        UserCtx = Req#httpd.user_ctx,
        Roles = UserCtx#user_ctx.roles,
        Accept = couch_httpd:header_value(Req, "Accept"),
        Parts = {couch_httpd:doc_etag(DDoc), Accept, Roles},
        ETag = couch_httpd:make_etag({BaseSig, Parts}),
        case couch_httpd:etag_match(Req, ETag) of
            true -> throw({etag_match, ETag});
            false -> {ok, Acc0#sacc{etag=ETag}}
        end
    end,
    Args = Args0#gcargs{preflight_fun=ETagFun},
    couch_httpd:etag_maybe(Req, fun() ->
        couch_query_servers:with_ddoc_proc(DDoc, fun(QServer) ->
            Acc = #sacc{db=Db, req=Req, qserver=QServer, lname=LName},
            geocouch:spatial_query(Db, SDDoc, SName, Args, fun list_cb/2, Acc)
        end)
    end).


list_cb({meta, Meta}, #sacc{resp=undefined}=Acc) ->
    MetaProps = case couch_util:get_value(total, Meta) of
        undefined -> [];
        Total -> [{total_rows, Total}]
    end ++ case couch_util:get_value(update_seq, Meta) of
        undefined -> [];
        UpdateSeq -> [{update_seq, UpdateSeq}]
    end,
    start_list_resp({MetaProps}, Acc);
list_cb({row, Row}, #sacc{resp=undefined}=Acc) ->
    {ok, NewAcc} = start_list_resp({[]}, Acc),
    send_list_row(Row, NewAcc);
list_cb({row, Row}, Acc) ->
    send_list_row(Row, Acc);
list_cb(complete, Acc) ->
    #sacc{qserver={Proc, _}, resp=Resp0} = Acc,
    case Resp0 of
        nil -> {ok, #sacc{resp=Resp}} = start_list_resp({[]}, Acc);
        Resp -> Resp
    end,
    [<<"end">>, Data] = couch_query_servers:proc_prompt(Proc, [<<"list_end">>]),
    send_non_empty_chunk(Resp, Data),
    couch_httpd:last_chunk(Resp),
    {ok, Resp}.

start_list_resp(Head, Acc) ->
    #sacc{db=Db, req=Req, qserver=QServer, lname=LName, etag=ETag}=Acc,
    JsonReq = couch_httpd_external:json_req_obj(Req, Db),

    [<<"start">>, Chunk, JsonResp] = couch_query_servers:ddoc_proc_prompt(
        QServer, [<<"lists">>, LName], [Head, JsonReq]
    ),
    JsonResp2 = apply_etag(JsonResp, ETag),
    #extern_resp_args{
        code = Code,
        ctype = CType,
        headers = ExtHeaders
    } = couch_httpd_external:parse_external_response(JsonResp2),
    JsonHeaders = couch_httpd_external:default_or_content_type(
        CType, ExtHeaders
    ),
    {ok, Resp} = couch_httpd:start_chunked_response(Req, Code, JsonHeaders),
    send_non_empty_chunk(Resp, Chunk),
    {ok, Acc#sacc{resp=Resp}}.

send_list_row(Row, #sacc{qserver={Proc, _}, resp=Resp}=Acc) ->
    RowObj = case couch_util:get_value(id, Row) of
        undefined -> [];
        Id -> [{id, Id}]
    end ++ case couch_util:get_value(bbox, Row) of
        undefined -> [];
        BBox -> [{bbox, tuple_to_list(BBox)}]
    end ++ case couch_util:get_value(geometry, Row) of
        undefined -> [];
        Geom -> [{geometry, geocouch_util:to_geojson(Geom)}]
    end ++ case couch_util:get_value(val, Row) of
        undefined -> [];
        Val -> [{value, Val}]
    end,
    try couch_query_servers:proc_prompt(Proc, [<<"list_row">>, {RowObj}]) of
        [<<"chunks">>, Chunk] ->
            send_non_empty_chunk(Resp, Chunk),
            {ok, Acc};
        [<<"end">>, Chunk] ->
            send_non_empty_chunk(Resp, Chunk),
            couch_httpd:last_chunk(Resp),
            {stop, Acc}
    catch Error ->
        couch_httpd:send_chunked_error(Resp, Error),
        {stop, Acc}
    end.


send_non_empty_chunk(_, []) ->
    ok;
send_non_empty_chunk(Resp, Chunk) ->
    couch_httpd:send_chunk(Resp, Chunk).


apply_etag({ExternalResponse}, CurrentEtag) ->
    case couch_util:get_value(<<"headers">>, ExternalResponse, nil) of
        nil ->
            {[
                {<<"headers">>, {[
                    {<<"Etag">>, CurrentEtag},
                    {<<"Vary">>, <<"Accept">>}
                ]}}
                | ExternalResponse
            ]};
        JsonHeaders ->
            {[
                case Field of
                    {<<"headers">>, JsonHeaders} ->
                        JsonHeadersEtagged = json_apply_field(
                            {<<"Etag">>, CurrentEtag}, JsonHeaders
                        ),
                        JsonHeadersVaried = json_apply_field(
                            {<<"Vary">>, <<"Accept">>}, JsonHeadersEtagged
                        ),
                        {<<"headers">>, JsonHeadersVaried};
                    _ ->
                        Field
                end || Field <- ExternalResponse
            ]}
    end.


% Maybe this is in the proplists API
% todo move to couch_util
json_apply_field(H, {L}) ->
    json_apply_field(H, L, []).


json_apply_field({Key, NewValue}, [{Key, _OldVal} | Headers], Acc) ->
    % drop matching keys
    json_apply_field({Key, NewValue}, Headers, Acc);
json_apply_field({Key, NewValue}, [{OtherKey, OtherVal} | Headers], Acc) ->
    % something else is next, leave it alone.
    json_apply_field({Key, NewValue}, Headers, [{OtherKey, OtherVal} | Acc]);
json_apply_field({Key, NewValue}, [], Acc) ->
    % end of list, add ours
    {[{Key, NewValue}|Acc]}.
