package morbrian.nifi.reporting.elasticsearch;

import com.codahale.metrics.MetricRegistry;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import okhttp3.OkHttpClient;

public class ESMetricRegistryBuilder {


    private MetricRegistry metricRegistry = null;
    private List<String> tags = Arrays.asList();
    private ElasticsearchReporter elasticsearchReporter;
    private URL esUrl;
    private OkHttpClient client;

    public ESMetricRegistryBuilder setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public ESMetricRegistryBuilder setEsUrl(URL esUrl) {
        this.esUrl = esUrl;
        return this;
    }

    public ESMetricRegistryBuilder setClient(OkHttpClient client) {
        this.client = client;
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
            elasticsearchReporter = new ElasticsearchReporter(client, metricRegistry, esUrl);

        return this.metricRegistry;
    }

}
