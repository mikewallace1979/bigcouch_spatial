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


-module(bigcouch_spatial_rpc).

-export([spatial/4]).

-include("geocouch.hrl").
-include_lib("fabric/include/fabric.hrl").
-include_lib("couch/include/couch_db.hrl").

-record(spatial_acc, {
    db,
    include_docs,
    limit,
    total_rows}).

spatial(DbName, DDoc, SpatialName, QueryArgs) ->
    {ok, Db} = couch_db:open_int(DbName, []),
    #gcargs{
        bbox=Bbox,
        stale=_Stale,
        bounds=Bounds,
        limit = Limit,
        include_docs = IncludeDocs,
        extra = Extra
    } = QueryArgs,
    set_io_priority(DbName, Extra),

    {ok, Index, Sig, Args} = geocouch_util:get_index(Db, DDoc, SpatialName,
            QueryArgs),
    Acc0 = #spatial_acc{
        db = Db,
        include_docs = IncludeDocs,
        limit = Limit
    },
    {ok, Acc1} = case Args#gcargs.preflight_fun of
        PFFun when is_function(PFFun, 2) -> PFFun(Sig, Acc0);
        _ -> {ok, Acc0}
    end,
    {ok, {_, _Acc}} = geocouch_util:fold(Index, fun
            spatial_fold/2, {nil, Acc1}, Bbox, Bounds),
    rexi:reply(complete).


%% internal

spatial_fold({{_Bbox, _DocId}, {_Geom, _Value}}, 
        {_Resp, #spatial_acc{limit=0}}=Acc) ->
    {stop, Acc};
spatial_fold({{Bbox, DocId}, {Geom, Value}}, {Resp, Acc}) ->
    #spatial_acc{
        db = Db,
        limit = Limit,
        include_docs = IncludeDocs
    } = Acc,

    case Value of {Props} ->
        LinkedDocs = (couch_util:get_value(<<"_id">>, Props) =/= undefined);
    _ ->
        LinkedDocs = false
    end,

    if LinkedDocs ->
        % we'll embed this at a higher level b/c the doc may be non-local
        Doc = undefined;
    IncludeDocs ->
        case couch_db:open_doc(Db, DocId, []) of
        {not_found, deleted} ->
            Doc = null;
        {not_found, missing} ->
            Doc = undefined;
        {ok, Doc0} ->
            Doc = couch_doc:to_json_obj(Doc0, [])
        end;
    true ->
        Doc = undefined
    end,
    
    case rexi:sync_reply(#spatial_row{bbox=Bbox, id=DocId, geometry=Geom, 
                value=Value, doc=Doc}) of
        ok ->
            {ok, {Resp, Acc#spatial_acc{limit=Limit-1}}};
        stop ->
            exit(normal);
        timeout ->
            exit(timeout)
    end.

set_io_priority(DbName, Options) ->
    case lists:keyfind(io_priority, 1, Options) of
    {io_priority, Pri} ->
        erlang:put(io_priority, Pri);
    false ->
        erlang:put(io_priority, {interactive, DbName})
    end.
