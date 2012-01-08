%%% -*- erlang -*-
%%%
%%% This file is part of geocouch released under the Apache license 2. 
%%% See the NOTICE for more information.

-module(geocouch_cleanup).

-export([run/1]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("geocouch/include/geocouch.hrl").


run(Db) ->
    IdxDir = couch_config:get("couchdb", "index_dir"),
    DbName = couch_db:name(Db),
    DbNameL = binary_to_list(DbName),

    {ok, DesignDocs} = couch_db:get_design_docs(Db),
    SigFiles = lists:foldl(fun(DDoc, SFAcc) ->
        InitState = geocouch_util:ddoc_to_gcst(DbName, DDoc),
        Sig = InitState#gcst.sig,
        IFName = geocouch_util:index_file(IdxDir, DbName, Sig),
        CFName = geocouch_util:compaction_file(IdxDir, DbName, Sig),
        [IFName, CFName | SFAcc]
    end, [], [DD || DD <- DesignDocs, DD#doc.deleted == false]),

    DiskFiles = filelib:wildcard(IdxDir ++ "/." ++ DbNameL ++ "_design/*"),

    ToDelete = DiskFiles -- SigFiles,

    lists:foreach(fun(FN) ->
        ?LOG_DEBUG("Deleting stale spatial file: ~s", [FN]),
        couch_file:delete(IdxDir, FN, false)
    end, ToDelete),

    ok.
