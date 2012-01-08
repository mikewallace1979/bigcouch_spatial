%%% -*- erlang -*-
%%%
%%% This file is part of geocouch released under the Apache license 2. 
%%% See the NOTICE for more information.


-module(geocouch_index).

-behaviour(couch_index_api).

-export([get/2]).
-export([init/2, open/2, close/1, reset/1, delete/1]).
-export([start_update/3, purge/4, process_doc/3, finish_update/1, commit/1]).
-export([compact/3, swap_compacted/2]).

-include_lib("geocouch/include/geocouch.hrl").

get(Property, State) ->
    case Property of
        db_name ->
            State#gcst.db_name;
        idx_name ->
            State#gcst.idx_name;
        signature ->
            State#gcst.sig;
        update_seq ->
            State#gcst.update_seq;
        purge_seq ->
            State#gcst.purge_seq;
        update_options ->
            Opts = State#gcst.design_opts,
            Opts1 = case couch_util:get_value(<<"include_design">>, Opts, false) of
                true -> [include_design];
                _ -> []
            end,
            Opts2 = case couch_util:get_value(<<"local_seq">>, Opts, false) of
                true -> [local_seq];
                _ -> []
            end,
            Opts1 ++ Opts2;
        info ->
            #gcst{
                fd = Fd,
                sig = Sig,
                language = Lang,
                update_seq = UpdateSeq,
                purge_seq = PurgeSeq
            } = State,
            {ok, Size} = couch_file:bytes(Fd),
            {ok, [
                {signature, list_to_binary(geocouch_util:hexsig(Sig))},
                {language, Lang},
                {disk_size, Size},
                {update_seq, UpdateSeq},
                {purge_seq, PurgeSeq}
            ]};
        Other ->
            throw({unknown_index_property, Other})
    end.

init(Db, DDoc) ->
    geocouch_util:ddoc_to_gcst(couch_db:name(Db), DDoc).

open(Db, State) ->
    #gcst{
        db_name=DbName,
        sig=Sig,
        root_dir=RootDir
    } = State,
    IndexFName = geocouch_util:index_file(RootDir, DbName, Sig),
    case geocouch_util:open_file(IndexFName) of
        {ok, Fd} ->
            case (catch couch_file:read_header(Fd)) of
                {ok, {Sig, Header}} ->
                    % Matching view signatures.
                    {ok, geocouch_util:init_state(Db, Fd, State, Header)};
                _ ->
                    {ok, geocouch_util:reset_index(Db, Fd, State)}
            end;
        Error ->
            (catch geocouch_util:delete_index_file(RootDir, DbName, Sig)),
            Error
    end.

close(State) ->
    couch_file:close(State#gcst.fd).


delete(#gcst{db_name=DbName, sig=Sig, root_dir=RootDir}=State) ->
    couch_file:close(State#gcst.fd),
    catch geocouch_util:delete_index_file(RootDir, DbName, Sig).


purge(Db, PurgeSeq, PurgedIdRevs, State) ->
    geoocouch_updater:purge_index(Db, PurgeSeq, PurgedIdRevs, State).


start_update(Partial, State, NumChanges) ->
    geocouch_updater:start_update(Partial, State, NumChanges).


process_doc(Doc, Seq, State) ->
    geocouch_updater:process_doc(Doc, Seq, State).


finish_update(State) ->
    geocouch_updater:finish_update(State).


commit(State) ->
    Header = {State#gcst.sig, geocouch_util:make_header(State)},
    couch_file:write_header(State#gcst.fd, Header),
    couch_file:sync(State#gcst.fd).


compact(Db, State, Opts) ->
    geocouch_compactor:compact(Db, State, Opts).


swap_compacted(OldState, NewState) ->
    geocouch_compactor:swap_compacted(OldState, NewState).


reset(State) ->
    couch_util:with_db(State#gcst.db_name, fun(Db) ->
        NewState = geocouch_util:reset_index(Db, State#gcst.fd, State),
        {ok, NewState}
    end).
