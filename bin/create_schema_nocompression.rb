#!./hraven org.jruby.Main

#
# Copyright 2013 Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Create all hRaven tables in HBase
#
# Run this script using the HBase "shell" command:
#
#     hbase [--config /path/to/hbase/conf] shell bin/create_table.rb
#
create 'hraven.job_history', {NAME => 'i'}

create 'hraven.job_history_task', {NAME => 'i'}

# job_history  (indexed) by jobId table contains 1 column family:
#   i:  job-level information specifically the rowkey into the 
create 'hraven.job_history-by_jobId', {NAME => 'i'}

# job_history_app_version - stores all version numbers seen for a single app ID
#   i:  "info" -- version information
create 'hraven.job_history_app_version', {NAME => 'i'}

create 'hraven.job_history_raw', {NAME => 'i', BLOOMFILTER => 'ROWCOL'},
                                {NAME => 'r', VERSIONS => 1, BLOCKCACHE => false}

# job_history_process - stores metadata about job history data loading process
#   i:  "info" -- process information
create 'hraven.job_history_process', {NAME => 'i', VERSIONS => 10}

# flow_queue - stores reference to each flow ID running on a cluster, reverse timestamp ordered
create 'hraven.flow_queue', {NAME => 'i', VERSIONS => 3, BLOOMFILTER => 'ROW'}

# flow_event - stores events fired during pig job execution
create 'hraven.flow_event', {NAME => 'i', VERSIONS => 3, BLOOMFILTER => 'ROW'}

# graphite key mapping tables
create 'hraven.graphite_key_mapping', {NAME => 'i'}
create 'hraven.graphite_key_mapping_r', {NAME => 'i'}

exit
