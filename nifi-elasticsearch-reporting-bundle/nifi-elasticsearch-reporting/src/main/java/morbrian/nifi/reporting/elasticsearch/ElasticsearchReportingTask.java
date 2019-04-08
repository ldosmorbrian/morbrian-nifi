package morbrian.nifi.reporting.elasticsearch;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;
import com.yammer.metrics.core.VirtualMachineMetrics;
import java.net.MalformedURLException;
import java.net.URL;
import morbrian.nifi.reporting.elasticsearch.metrics.MetricsService;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.elasticsearch.script.ScriptContext.Standard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Tags({"reporting", "elasticsearch", "metrics"})
@CapabilityDescription("Publishes metrics from NiFi to elasticsearch. For accurate and informative reporting, components should have unique names.")
public class ElasticsearchReportingTask extends AbstractReportingTask {

    private static final String METRIC_NAME_SEPARATOR = "-";

    static final PropertyDescriptor ENVIRONMENT = new PropertyDescriptor.Builder()
            .name("Environment")
            .description("Environment, dataflow is running in. " +
                    "This property will be included as metrics tag.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("dev")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ES_URL = new PropertyDescriptor.Builder()
        .name("Elasticsearch Index Url")
        .description("Elasticsearch URL including path with index and type.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("http://localhost:9200/nifi/reporting")
        .addValidator(StandardValidators.URI_VALIDATOR)
        .build();


    private MetricsService metricsService;
    private ESMetricRegistryBuilder esMetricRegistryBuilder;
    private MetricRegistry metricRegistry;
    private String environment;
    private String statusId;
    private URL esUrl;
    private ConcurrentHashMap<String, AtomicDouble> metricsMap;
    private Map<String, String> defaultTags;
    private volatile VirtualMachineMetrics virtualMachineMetrics;
    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    @OnScheduled
    public void setup(final ConfigurationContext context) {
        metricsService = getMetricsService();
        esMetricRegistryBuilder = getMetricRegistryBuilder();
        metricRegistry = getMetricRegistry();
        metricsMap = getMetricsMap();
        environment = ENVIRONMENT.getDefaultValue();
        virtualMachineMetrics = VirtualMachineMetrics.getInstance();
        esMetricRegistryBuilder.setMetricRegistry(metricRegistry).setTags(metricsService.getAllTagsList());
        esUrl = getValidUrlOrNull(ES_URL.getDefaultValue());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ES_URL);
        properties.add(ENVIRONMENT);
        return properties;
    }

    @Override
    public void onTrigger(ReportingContext context) {
        final ProcessGroupStatus status = context.getEventAccess().getControllerStatus();

        esUrl = getValidUrlOrNull(context.getProperty(ES_URL).evaluateAttributeExpressions().getValue());
        environment = context.getProperty(ENVIRONMENT).evaluateAttributeExpressions().getValue();
        statusId = status.getId();
        defaultTags = ImmutableMap.of("env", environment, "dataflow_id", statusId);

        esMetricRegistryBuilder.setEsUrl(esUrl);

        esMetricRegistryBuilder.build();
        updateAllMetricGroups(status);
        esMetricRegistryBuilder.getElasticsearchReporter().report();
    }

    protected void updateMetrics(Map<String, Double> metrics, Optional<String> processorName, Map<String, String> tags) {
        for (Map.Entry<String, Double> entry : metrics.entrySet()) {
            final String metricName = buildMetricName(processorName, entry.getKey());
            logger.debug(metricName + ": " + entry.getValue());
            //if metric is not registered yet - register it
            if (!metricsMap.containsKey(metricName)) {
                metricsMap.put(metricName, new AtomicDouble(entry.getValue()));
                metricRegistry.register(metricName, new MetricGauge(metricName, tags));
            }
            //set real time value to metrics map
            metricsMap.get(metricName).set(entry.getValue());
        }
    }

    private void updateAllMetricGroups(ProcessGroupStatus processGroupStatus) {
        final List<ProcessorStatus> processorStatuses = new ArrayList<>();
        populateProcessorStatuses(processGroupStatus, processorStatuses);
        for (final ProcessorStatus processorStatus : processorStatuses) {
            updateMetrics(metricsService.getProcessorMetrics(processorStatus),
                    Optional.of(processorStatus.getName()), defaultTags);
        }

        final List<ConnectionStatus> connectionStatuses = new ArrayList<>();
        populateConnectionStatuses(processGroupStatus, connectionStatuses);
        for (ConnectionStatus connectionStatus: connectionStatuses) {
            Map<String, String> connectionStatusTags = new HashMap<>(defaultTags);
            connectionStatusTags.putAll(metricsService.getConnectionStatusTags(connectionStatus));
            updateMetrics(metricsService.getConnectionStatusMetrics(connectionStatus), Optional.<String>absent(), connectionStatusTags);
        }

        final List<PortStatus> inputPortStatuses = new ArrayList<>();
        populateInputPortStatuses(processGroupStatus, inputPortStatuses);
        for (PortStatus portStatus: inputPortStatuses) {
            Map<String, String> portTags = new HashMap<>(defaultTags);
            portTags.putAll(metricsService.getPortStatusTags(portStatus));
            updateMetrics(metricsService.getPortStatusMetrics(portStatus), Optional.<String>absent(), portTags);
        }

        final List<PortStatus> outputPortStatuses = new ArrayList<>();
        populateOutputPortStatuses(processGroupStatus, outputPortStatuses);
        for (PortStatus portStatus: outputPortStatuses) {
            Map<String, String> portTags = new HashMap<>(defaultTags);
            portTags.putAll(metricsService.getPortStatusTags(portStatus));
            updateMetrics(metricsService.getPortStatusMetrics(portStatus), Optional.<String>absent(), portTags);
        }

        updateMetrics(metricsService.getJVMMetrics(virtualMachineMetrics),
                Optional.<String>absent(), defaultTags);
        updateMetrics(metricsService.getDataFlowMetrics(processGroupStatus), Optional.<String>absent(), defaultTags);
    }

    private class MetricGauge implements Gauge {
        private Map<String, String> tags;
        private String metricName;

        public MetricGauge(String metricName, Map<String, String> tagsMap) {
            this.tags = tagsMap;
            this.metricName = metricName;
        }

        @Override
        public Object getValue() {
            return metricsMap.get(metricName).get();
        }

        //@Override
        public List<String> getTags() {
            List<String> tagsList = Lists.newArrayList();
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                tagsList.add(entry.getKey() + ":" + entry.getValue());
            }
            return tagsList;
        }
    }

    private void populateProcessorStatuses(final ProcessGroupStatus groupStatus, final List<ProcessorStatus> statuses) {
        statuses.addAll(groupStatus.getProcessorStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateProcessorStatuses(childGroupStatus, statuses);
        }
    }

    private void populateConnectionStatuses(final ProcessGroupStatus groupStatus, final List<ConnectionStatus> statuses) {
        statuses.addAll(groupStatus.getConnectionStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateConnectionStatuses(childGroupStatus, statuses);
        }
    }

    private void populateInputPortStatuses(final ProcessGroupStatus groupStatus, final List<PortStatus> statuses) {
        statuses.addAll(groupStatus.getInputPortStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateInputPortStatuses(childGroupStatus, statuses);
        }
    }

    private void populateOutputPortStatuses(final ProcessGroupStatus groupStatus, final List<PortStatus> statuses) {
        statuses.addAll(groupStatus.getOutputPortStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateOutputPortStatuses(childGroupStatus, statuses);
        }
    }

    private String buildMetricName(Optional<String> processorName, String metricName) {
        return makeNameSafeForEs(processorName.or("flow")) + METRIC_NAME_SEPARATOR + metricName;
    }

    protected MetricsService getMetricsService() {
        return new MetricsService();
    }

    protected ESMetricRegistryBuilder getMetricRegistryBuilder() {
        return new ESMetricRegistryBuilder();
    }

    protected MetricRegistry getMetricRegistry() {
        return new MetricRegistry();
    }

    protected ConcurrentHashMap<String, AtomicDouble> getMetricsMap() {
        return new ConcurrentHashMap<>();
    }

    private URL getValidUrlOrNull(String urlString) {
        try {
            return new URL(urlString);
        } catch (MalformedURLException exc) {
            // since we have an input validator this would be highly unexpected.
            getLogger().error("URL is invalid: " + urlString, exc);
            return null;
        }
    }

    private String makeNameSafeForEs(String name) {
        return name.replace('.', '-');
    }
}
