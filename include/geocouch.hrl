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

-define(GEOCOUCH_DISK_VSN, 2).


-record(gcst, {
    sig=nil,
    fd=nil,
    db_name,
    idx_name,
    language,
    design_opts=[],
    lib,
    indexes,
    id_btree=nil,
    update_seq=0,
    purge_seq=0,
    root_dir=[],

    first_build,
    partial_resp_pid,
    doc_acc,
    doc_queue,
    write_queue,
    query_server=nil
}).


-record(gcidx, {
    id_num,
    update_seq=0,
    purge_seq=0,
    idx_names=[],
    def=nil,
    fd,
    vtree=nil,
    vtree_height=0
}).


-record(gcheader, {
    update_seq=0,
    purge_seq=0,
    id_btree_state=nil,
    idx_states=[],
    disk_version = ?GEOCOUCH_DISK_VSN
}).


-record(gcargs, {
    bbox,
    stale,
    count=false,
    bounds=nil,
    limit=10000000000,
    include_docs=false,
    preflight_fun,
    extra=[]
}).

-record(spatial_collector, {
    db_name=nil,
    query_args,
    callback,
    counters,
    buffer_size,
    blocked = [],
    total_rows = 0,
    rows = [],
    etag,
    limit,
    os_proc,
    lang,
    user_acc
}).

-record(spatial_row, {id, bbox, geometry, value, doc, worker}).