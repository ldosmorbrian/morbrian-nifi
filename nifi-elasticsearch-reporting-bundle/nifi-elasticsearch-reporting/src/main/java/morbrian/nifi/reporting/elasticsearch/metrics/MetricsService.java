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

import com.yammer.metrics.core.VirtualMachineMetrics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;

/**
 * A service used to produce key/value metrics based on a given input.
 */
public class MetricsService {

    //processor - specific metrics
    public Map<String, Double> getProcessorMetrics(ProcessorStatus status) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, (double)status.getInputCount());
        metrics.put(MetricNames.FLOW_FILES_SENT, (double)status.getOutputCount());
        metrics.put(MetricNames.BYTES_READ, (double)status.getInputBytes());
        metrics.put(MetricNames.BYTES_WRITTEN, (double)status.getOutputBytes());
        metrics.put(MetricNames.ACTIVE_THREADS, (double)status.getActiveThreadCount());
        metrics.put(MetricNames.TOTAL_TASK_DURATION, (double)status.getProcessingNanos());
        return metrics;
    }

    public Map<String, Double> getPortStatusMetrics(PortStatus status){
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.ACTIVE_THREADS, (double)status.getActiveThreadCount());
        metrics.put(MetricNames.INPUT_COUNT, (double)status.getInputCount());
        metrics.put(MetricNames.OUTPUT_COUNT, (double)status.getOutputCount());
        metrics.put(MetricNames.INPUT_BYTES, (double)status.getInputBytes());
        metrics.put(MetricNames.OUTPUT_BYTES, (double)status.getOutputBytes());
        metrics.put(MetricNames.FLOW_FILES_RECEIVED,(double)status.getFlowFilesReceived());
        metrics.put(MetricNames.FLOW_FILES_SENT, (double)status.getFlowFilesSent());
        metrics.put(MetricNames.BYTES_RECEIVED, (double)status.getBytesReceived());
        metrics.put(MetricNames.BYTES_SENT, (double)status.getBytesSent());
        return metrics;
    }

    public Map<String,String> getPortStatusTags(PortStatus status) {
        final Map<String, String> portTags = new HashMap<>();
        portTags.put(MetricNames.PORT_ID, status.getId());
        portTags.put(MetricNames.PORT_GROUP_ID, status.getGroupId());
        portTags.put(MetricNames.PORT_NAME, status.getName());
        return portTags;
    }

    public Map<String,String> getConnectionStatusTags(ConnectionStatus status) {
        final Map<String, String> connectionTags = new HashMap<>();
        connectionTags.put(MetricNames.CONNECTION_ID, status.getId());
        connectionTags.put(MetricNames.CONNECTION_NAME, status.getName());
        connectionTags.put(MetricNames.CONNECTION_GROUP_ID, status.getGroupId());
        connectionTags.put(MetricNames.CONNECTION_DESTINATION_ID, status.getDestinationId());
        connectionTags.put(MetricNames.CONNECTTION_DESTINATION_NAME, status.getDestinationName());
        connectionTags.put(MetricNames.CONNECTION_SOURCE_ID, status.getSourceId());
        connectionTags.put(MetricNames.CONNECTION_SOURCE_NAME, status.getSourceName());
        return connectionTags;
    }

    public Map<String, Double> getConnectionStatusMetrics(ConnectionStatus status) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.INPUT_COUNT, (double)status.getInputCount());
        metrics.put(MetricNames.INPUT_BYTES, (double)status.getInputBytes());
        metrics.put(MetricNames.QUEUED_COUNT, (double)status.getQueuedCount());
        metrics.put(MetricNames.QUEUED_BYTES, (double)status.getQueuedBytes());
        metrics.put(MetricNames.OUTPUT_COUNT, (double)status.getOutputCount());
        metrics.put(MetricNames.OUTPUT_BYTES, (double)status.getOutputBytes());
        return metrics;
    }


    //general metrics for whole dataflow
    public Map<String, Double> getDataFlowMetrics(ProcessGroupStatus status) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, (double)status.getFlowFilesReceived());
        metrics.put(MetricNames.BYTES_RECEIVED, (double)status.getBytesReceived());
        metrics.put(MetricNames.FLOW_FILES_SENT, (double)status.getFlowFilesSent());
        metrics.put(MetricNames.BYTES_SENT, (double)status.getBytesSent());
        metrics.put(MetricNames.FLOW_FILES_QUEUED, (double)status.getQueuedCount());
        metrics.put(MetricNames.BYTES_QUEUED, (double)status.getQueuedContentSize());
        metrics.put(MetricNames.BYTES_READ, (double)status.getBytesRead());
        metrics.put(MetricNames.BYTES_WRITTEN, (double)status.getBytesWritten());
        metrics.put(MetricNames.ACTIVE_THREADS, (double)status.getActiveThreadCount());
        metrics.put(MetricNames.TOTAL_TASK_DURATION, (double)calculateProcessingNanos(status));
        status.getOutputPortStatus();
        return metrics;
    }

    public List<String> getAllTagsList() {
        List<String> tagsList = new ArrayList<>();
        tagsList.add("env");
        tagsList.add("dataflow_id");
        tagsList.add(MetricNames.PORT_ID);
        tagsList.add(MetricNames.PORT_NAME);
        tagsList.add(MetricNames.PORT_GROUP_ID);
        tagsList.add(MetricNames.CONNECTION_ID);
        tagsList.add(MetricNames.CONNECTION_NAME);
        tagsList.add(MetricNames.CONNECTION_GROUP_ID);
        tagsList.add(MetricNames.CONNECTION_SOURCE_ID);
        tagsList.add(MetricNames.CONNECTION_SOURCE_NAME);
        tagsList.add(MetricNames.CONNECTION_DESTINATION_ID);
        tagsList.add(MetricNames.CONNECTTION_DESTINATION_NAME);
        return tagsList;
    }

    //virtual machine metrics
    public Map<String, Double> getJVMMetrics(VirtualMachineMetrics virtualMachineMetrics) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.JVM_UPTIME, (double)virtualMachineMetrics.uptime());
        metrics.put(MetricNames.JVM_HEAP_USED, virtualMachineMetrics.heapUsed());
        metrics.put(MetricNames.JVM_HEAP_USAGE, virtualMachineMetrics.heapUsage());
        metrics.put(MetricNames.JVM_NON_HEAP_USAGE, virtualMachineMetrics.nonHeapUsage());
        metrics.put(MetricNames.JVM_THREAD_COUNT, (double)virtualMachineMetrics.threadCount());
        metrics.put(MetricNames.JVM_DAEMON_THREAD_COUNT, (double)virtualMachineMetrics.daemonThreadCount());
        metrics.put(MetricNames.JVM_FILE_DESCRIPTOR_USAGE, virtualMachineMetrics.fileDescriptorUsage());

        for (Map.Entry<Thread.State, Double> entry : virtualMachineMetrics.threadStatePercentages().entrySet()) {
            final int normalizedValue = (int) (100 * (entry.getValue() == null ? 0 : entry.getValue()));
            switch (entry.getKey()) {
                case BLOCKED:
                    metrics.put(MetricNames.JVM_THREAD_STATES_BLOCKED, (double)normalizedValue);
                    break;
                case RUNNABLE:
                    metrics.put(MetricNames.JVM_THREAD_STATES_RUNNABLE, (double)normalizedValue);
                    break;
                case TERMINATED:
                    metrics.put(MetricNames.JVM_THREAD_STATES_TERMINATED, (double)normalizedValue);
                    break;
                case TIMED_WAITING:
                    metrics.put(MetricNames.JVM_THREAD_STATES_TIMED_WAITING, (double)normalizedValue);
                    break;
                default:
                    break;
            }
        }

        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : virtualMachineMetrics.garbageCollectors().entrySet()) {
            final String gcName = entry.getKey().replace(" ", "");
            final long runs = entry.getValue().getRuns();
            final long timeMS = entry.getValue().getTime(TimeUnit.MILLISECONDS);
            metrics.put(MetricNames.JVM_GC_RUNS + MetricNames.NAME_SEP + gcName, (double)runs);
            metrics.put(MetricNames.JVM_GC_TIME + MetricNames.NAME_SEP + gcName, (double)timeMS);
        }

        return metrics;
    }


    // calculates the total processing time of all processors in nanos
    protected long calculateProcessingNanos(final ProcessGroupStatus status) {
        long nanos = 0L;

        for (final ProcessorStatus procStats : status.getProcessorStatus()) {
            nanos += procStats.getProcessingNanos();
        }

        for (final ProcessGroupStatus childGroupStatus : status.getProcessGroupStatus()) {
            nanos += calculateProcessingNanos(childGroupStatus);
        }

        return nanos;
    }


}
