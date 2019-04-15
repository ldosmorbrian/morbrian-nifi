/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package morbrian.nifi.reporting.elasticsearch.metrics;

/**
 * The Metric names to send to Elasticsearch.
 */
public interface MetricNames {

    public static final char NAME_SEP = '_';

    // NiFi Metrics
    String FLOW_FILES_RECEIVED = "FlowFilesReceivedLast5Minutes";
    String BYTES_RECEIVED = "BytesReceivedLast5Minutes";
    String FLOW_FILES_SENT = "FlowFilesSentLast5Minutes";
    String BYTES_SENT = "BytesSentLast5Minutes";
    String FLOW_FILES_QUEUED = "FlowFilesQueued";
    String BYTES_QUEUED = "BytesQueued";
    String BYTES_READ = "BytesReadLast5Minutes";
    String BYTES_WRITTEN = "BytesWrittenLast5Minutes";
    String ACTIVE_THREADS = "ActiveThreads";
    String TOTAL_TASK_DURATION = "TotalTaskDurationSeconds";

    // JVM Metrics
    String JVM_UPTIME = "jvm_uptime";
    String JVM_HEAP_USED = "jvm_heap_used";
    String JVM_HEAP_USAGE = "jvm_heap_usage";
    String JVM_NON_HEAP_USAGE = "jvm_non_heap_usage";
    String JVM_THREAD_STATES_RUNNABLE = "jvm_thread_states_runnable";
    String JVM_THREAD_STATES_BLOCKED = "jvm_thread_states_blocked";
    String JVM_THREAD_STATES_TIMED_WAITING = "jvm_thread_states_timed_waiting";
    String JVM_THREAD_STATES_TERMINATED = "jvm_thread_states_terminated";
    String JVM_THREAD_COUNT = "jvm_thread_count";
    String JVM_DAEMON_THREAD_COUNT = "jvm_daemon_thread_count";
    String JVM_FILE_DESCRIPTOR_USAGE = "jvm_file_descriptor_usage";
    String JVM_GC_RUNS = "jvm_gc_runs";
    String JVM_GC_TIME = "jvm_gc_time";

    // Port status metrics
    String INPUT_COUNT = "InputCount";
    String INPUT_BYTES = "InputBytes";
    String OUTPUT_COUNT = "OutputCount";
    String OUTPUT_BYTES = "OutputBytes";

    //Connection status metrics
    String QUEUED_COUNT = "QueuedCount";
    String QUEUED_BYTES = "QueuedBytes";

    //Port status tags
    String PORT_ID = "port-id";
    String PORT_GROUP_ID = "port-group-id";
    String PORT_NAME = "port-name";

    //Connection status tags
    String CONNECTION_ID = "connection-id";
    String CONNECTION_GROUP_ID = "connection-group-id";
    String CONNECTION_NAME = "connection-name";
    String CONNECTION_SOURCE_ID = "connection-source-id";
    String CONNECTION_SOURCE_NAME = "connection-source-name";
    String CONNECTION_DESTINATION_ID = "connection-destination-id";
    String CONNECTTION_DESTINATION_NAME = "connection-destination-name";
}
