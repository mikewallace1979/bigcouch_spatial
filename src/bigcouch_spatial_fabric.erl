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


-module(bigcouch_spatial_fabric).

-include("couch_spatial.hrl").

-include_lib("fabric/include/fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").


-export([go/6]).

go(DbName, DocId, Spatial, QueryArgs, Callback, Acc0) when is_binary(DocId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", DocId/binary>>, []),
    go(DbName, DDoc, Spatial, QueryArgs, Callback, Acc0);

go(DbName, DDoc, Spatial, QueryArgs, Callback, Acc0) ->
    Shards = get_shards(DbName, QueryArgs),
    Workers = submit_jobs(Shards, spatial, [DDoc, Spatial, QueryArgs]),
    BufferSize = couch_config:get("fabric", "map_buffer_size", "2"),
    #spatial_query_args{limit = Limit} = QueryArgs, 
    State = #spatial_collector{
        db_name=DbName,
        query_args = QueryArgs,
        callback = Callback,
        buffer_size = list_to_integer(BufferSize),
        counters = fabric_dict:init(Workers, 0),
        limit = Limit,
        user_acc = Acc0
    },
    try rexi_utils:recv(Workers, #shard.ref, fun handle_message/3,
        State, infinity, 1000 * 60 * 60) of
    {ok, NewState} ->
        {ok, NewState#spatial_collector.user_acc};
    {timeout, NewState} ->
        Callback({error, timeout}, NewState#spatial_collector.user_acc);
    {error, Resp} ->
        {ok, Resp}
    after
        fabric_util:cleanup(Workers)
    end.

handle_message({rexi_DOWN, _, _, _}, nil, State) ->
    % TODO see if progress can be made here, possibly by removing all shards
    % from that node and checking is_progress_possible
    {ok, State};

handle_message({rexi_EXIT, Reason}, Worker, State) ->
    #spatial_collector{callback=Callback, counters=Counters0, user_acc=Acc} = State,
    Counters = fabric_dict:erase(Worker, Counters0),
    case fabric_view:is_progress_possible(Counters) of
    true ->
        {ok, State#spatial_collector{counters = Counters}};
    false ->
        {ok, Resp} = Callback({error, fabric_util:error_info(Reason)}, Acc),
        {error, Resp}
    end;
    
handle_message(#spatial_row{} = Row, {Worker, From}, State) ->
    #spatial_collector{counters = Counters0, rows = Rows0} = State,
    case fabric_dict:lookup_element(Worker, Counters0) of
    undefined ->
        gen_server:reply(From, stop),
        {ok, State};
    _ ->
        Rows = merge_row(Row#spatial_row{worker=Worker}, Rows0),
        Counters1 = fabric_dict:update_counter(Worker, 1, Counters0),
        Counters2 = fabric_view:remove_overlapping_shards(Worker, Counters1),
        State1 = State#spatial_collector{rows=Rows, counters=Counters2},
        State2 = maybe_pause_worker(Worker, From, State1),
        maybe_send_row(State2)
    end;

handle_message(complete, Worker, State) ->
    Counters = fabric_dict:update_counter(Worker, 1, 
        State#spatial_collector.counters),
    maybe_send_row(State#spatial_collector{counters = Counters}).

maybe_pause_worker(Worker, From, State) ->
    #spatial_collector{buffer_size = BufferSize, counters = Counters} = State,
    case fabric_dict:lookup_element(Worker, Counters) of
    BufferSize ->
        State#spatial_collector{blocked = [{Worker,From} | State#spatial_collector.blocked]};
    _Count ->
        gen_server:reply(From, ok),
        State
    end.

maybe_resume_worker(Worker, State) ->
    #spatial_collector{buffer_size = Buffer, counters = C, blocked = B} = State,
    case fabric_dict:lookup_element(Worker, C) of
    Count when Count < Buffer/2 ->
        case couch_util:get_value(Worker, B) of
        undefined ->
            State;
        From ->
            gen_server:reply(From, ok),
            State#spatial_collector{blocked = lists:keydelete(Worker, 1, B)}
        end;
    _Other ->
        State
    end.

maybe_send_row(#spatial_collector{limit=0} = State) ->
    #spatial_collector{counters=Counters, user_acc=AccIn, callback=Callback} = State,
    case fabric_dict:any(0, Counters) of
    true ->
        % we still need to send the total/offset header
        {ok, State};
    false ->
        {_, Acc} = Callback(complete, AccIn),
        {stop, State#spatial_collector{user_acc=Acc}}
    end;
maybe_send_row(State) ->
    #spatial_collector{
        callback = Callback,
        counters = Counters,
        limit = Limit,
        user_acc = AccIn
    } = State,
    case fabric_dict:any(0, Counters) of
    true ->
        {ok, State};
    false ->
        try get_next_row(State) of
        {Row, NewState} ->
            case Callback(transform_row(possibly_embed_doc(NewState,Row)), AccIn) of
            {stop, Acc} ->
                {stop, NewState#spatial_collector{user_acc=Acc}};
            {ok, Acc} -> 
                maybe_send_row(NewState#spatial_collector{user_acc=Acc, limit=Limit-1})
            end
        catch complete ->
            {_, Acc} = Callback(complete, AccIn),
            {stop, State#spatial_collector{user_acc=Acc}}
        end
    end.

%% if include_docs=true is used when keys and
%% the values contain "_id" then use the "_id"s
%% to retrieve documents and embed in result
possibly_embed_doc(_State, #spatial_row{value=undefined}=Row) ->
    Row;
possibly_embed_doc(#spatial_collector{db_name=DbName, query_args=Args},
              #spatial_row{value=Value}=Row) ->
    #spatial_query_args{include_docs=IncludeDocs} = Args,
    case IncludeDocs andalso is_tuple(Value) of
    true ->
        {Props} = Value,
        case couch_util:get_value(<<"_id">>,Props) of
        undefined -> Row;
        IncId ->
            % use separate process to call fabric:open_doc
            % to not interfere with current call
            {Pid, Ref} = spawn_monitor(fun() ->
                                  exit(fabric:open_doc(DbName, IncId, [])) end),
            {ok, NewDoc} =
                receive {'DOWN',Ref,process,Pid, Resp} ->
                        Resp
                end,
            Row#spatial_row{doc=couch_doc:to_json_obj(NewDoc,[])}
        end;
        _ -> Row
    end.

%% internal %%
get_shards(DbName, #spatial_query_args{stale=ok}) ->
    mem3:ushards(DbName);
get_shards(DbName, _) ->
    mem3:shards(DbName).

merge_row(Row, Rows) ->
    lists:ukeysort(#spatial_row.id, [Row|Rows]).

get_next_row(#spatial_collector{rows = []}) ->
    throw(complete);
get_next_row(State) ->
    #spatial_collector{rows = [Row|Rest], counters = Counters0} = State,
    Worker = Row#spatial_row.worker,
    Counters1 = fabric_dict:update_counter(Worker, -1, Counters0),
    NewState = maybe_resume_worker(Worker, 
        State#spatial_collector{counters=Counters1}),
    {Row, NewState#spatial_collector{rows = Rest}}.

transform_row(#spatial_row{bbox=Bbox, id=undefined}) ->
    {row, {[{bbox, erlang:tuple_to_list(Bbox)}, {error,not_found}]}};
transform_row(#spatial_row{bbox=Bbox, id=Id, geometry=Geom, value=Value, 
        doc=undefined}) ->
    {row, {[{id,Id}, {bbox, erlang:tuple_to_list(Bbox)}, 
                {geometry, couch_spatial_updater:geocouch_to_geojsongeom(Geom)}, 
                {value,Value}]}};
transform_row(#spatial_row{bbox=Bbox, id=Id, geometry=Geom, value=Value, 
        doc={error,Reason}}) ->
    {row, {[{id,Id}, {bbox, erlang:tuple_to_list(Bbox)}, 
                {geometry, couch_spatial_updater:geocouch_to_geojsongeom(Geom)}, 
                {value,Value}, {error,Reason}]}};
transform_row(#spatial_row{bbox=Bbox, id=Id, geometry=Geom, value=Value, 
        doc=Doc}) ->
    {row, {[{id,Id}, {bbox,erlang:tuple_to_list(Bbox)}, 
                {geometry, couch_spatial_updater:geocouch_to_geojsongeom(Geom)}, 
                {value,Value}, {doc,Doc}]}}.


submit_jobs(Shards, EndPoint, ExtraArgs) ->
    lists:map(fun(#shard{node=Node, name=ShardName} = Shard) ->
        Ref = rexi:cast(Node, {bigcouch_spatial_rpc, EndPoint, [ShardName | ExtraArgs]}),
        Shard#shard{ref = Ref}
    end, Shards).


