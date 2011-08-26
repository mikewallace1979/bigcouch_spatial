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

-module(bigcouch_spatial).

-export([spatial/3, spatial/4, spatial/6]).

-include("couch_spatial.hrl").
-include_lib("fabric/include/fabric.hrl").
-include_lib("couch/include/couch_db.hrl").

-type dbname() :: (iodata() | #db{}).
-type callback() :: fun((any(), any()) -> {ok | stop, any()}).

%% @equiv spatial(DbName, DesignName, SpatialName, #spatial_query_args{})
spatial(DbName, DesignName, SpatialName) ->
    spatial(DbName, DesignName, SpatialName, #spatial_query_args{}).

%% @equiv spatial(DbName, DesignName,
%%                     SpatialName, fun default_callback/2, [], QueryArgs)
spatial(DbName, DesignName, SpatialName, QueryArgs) ->
    Callback = fun default_callback/2,
    spatial(DbName, DesignName, SpatialName, Callback, [], QueryArgs).

%% @doc retrieves docs in a bbox. Additional query parameters, such as `limit',
%%      `bbox`, `bounds', `stale', and `include_docs', can
%%      also be passed to further constrain the query. 
-spec spatial(dbname(), #doc{}|binary(), iodata(), callback(), any(),
        #view_query_args{}) -> any().
spatial(DbName, Design, SpatialName, Callback, Acc0, QueryArgs) ->
    bigcouch_spatial_fabric:go(dbname(DbName), Design, SpatialName, QueryArgs, 
        Callback, Acc0).


%% internal stuff

dbname(DbName) when is_list(DbName) ->
    list_to_binary(DbName);
dbname(DbName) when is_binary(DbName) ->
    DbName;
dbname(#db{name=Name}) ->
    Name;
dbname(DbName) ->
    erlang:error({illegal_database_name, DbName}).



default_callback(complete, Acc) ->
    {ok, lists:reverse(Acc)};
default_callback(Row, Acc) ->
    {ok, [Row | Acc]}.
