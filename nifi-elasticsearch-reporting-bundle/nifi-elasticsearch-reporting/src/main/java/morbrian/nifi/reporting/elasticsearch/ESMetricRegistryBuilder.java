package morbrian.nifi.reporting.elasticsearch;

import com.codahale.metrics.MetricRegistry;

import java.util.Arrays;
import java.util.List;

public class ESMetricRegistryBuilder {


    private MetricRegistry metricRegistry = null;
    private List<String> tags = Arrays.asList();
    private ElasticsearchReporter elasticsearchReporter;

    public ESMetricRegistryBuilder setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public ESMetricRegistryBuilder setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public ElasticsearchReporter getElasticsearchReporter() {
        return elasticsearchReporter;
    }

    public MetricRegistry build() {
        if (metricRegistry == null)
            metricRegistry = new MetricRegistry();

        if (elasticsearchReporter == null)
            elasticsearchReporter = new ElasticsearchReporter(metricRegistry);

        return this.metricRegistry;
    }

}
