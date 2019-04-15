package morbrian.nifi.reporting.elasticsearch;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import javax.json.Json;
import javax.json.JsonObjectBuilder;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchReporter extends ScheduledReporter {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchReporter.class);
    private static final int MAX_RESOURCE_SIZE = 10000;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static final MediaType JSON
        = MediaType.get("application/json; charset=utf-8");
    private static final int SC_HTTP_OK = 200;
    private static final int SC_HTTP_CREATED = 201;
    private static final int SC_HTTP_NOT_FOUND = 404;
    private final OkHttpClient client;



    private URL esUrl;

    public ElasticsearchReporter(OkHttpClient client, MetricRegistry metricRegistry, URL esUrl) {
        super(metricRegistry, "elasticsearch-reporter", MetricFilter.ALL,
            TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
        this.esUrl = esUrl;
        this.client = client;
    }

    public void setEsUrl(URL esUrl) {
        this.esUrl = esUrl;
    }

    public URL getEsUrl() {
        return esUrl;
    }

    public OkHttpClient getClient() {
        return client;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
        SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        JsonObjectBuilder reportBuilder = Json.createObjectBuilder();

        // stamp
        reportBuilder.add("date", DATE_FORMAT.format(new Date(System.currentTimeMillis())));

        // build gauges
        JsonObjectBuilder gaugesBuilder = Json.createObjectBuilder();
        gauges.forEach((key, value) -> gaugesBuilder.add(key, Double.valueOf(value.getValue().toString())));
        reportBuilder.add("gauges", gaugesBuilder.build());

        // build counters
        JsonObjectBuilder countersBuilder = Json.createObjectBuilder();
        gauges.forEach((key, value) -> countersBuilder.add(key, Double.valueOf(value.getValue().toString())));
        reportBuilder.add("counters", countersBuilder.build());

        String report = reportBuilder.build().toString();

        esPostMetrics(report);
    }

    private void esPostMetrics(String report) {
        logger.debug(report);

        // Create request for remote resource.
        RequestBody body = RequestBody.create(JSON, report);
        Request request = new Request.Builder()
            .url(getEsUrl())
            .post(body)
            .build();
        try {
            prepareEsEndpoint();
            Response response = getClient().newCall(request).execute();
            int statusCode = response.code();
            if (statusCode != SC_HTTP_OK && statusCode != SC_HTTP_CREATED) {
                logger.warn(String.format("Unexpected status code %d when checking ES Index at %s, response body: %s", statusCode, esUrl, response.body()));
            }
        } catch(IOException exc) {
            logger
                .error("Failed to post metrics to Elasticsearch: " + esUrl, exc);
        }
    }

    /**
     * Check if index and mappings exist, create if not already present.
     */
    private void prepareEsEndpoint() throws IOException {
        if (!checkIndexExists(getEsUrl())) {
            // errors on this would indicate a configuration problem
            // create index
            configureEndpoint("/_index", "nifi-reporting-es-index.json");
            // associate mapping
            configureEndpoint("/_mapping", "nifi-reporting-es-mappings.json");
        }
    }

    private void configureEndpoint(String modifier, String resourcePath) throws IOException {
        RequestBody body = RequestBody.create(JSON, readResourceContent(resourcePath));
        Request request = new Request.Builder()
            .url(new URL(getEsUrl().toString() + modifier))
            .put(body)
            .build();

        try {
            Response response = getClient().newCall(request).execute();
            int statusCode = response.code();
            // expect OK if we put successfully or if it already exists.
            if (statusCode != SC_HTTP_OK) {
                logger.warn("Attempt to configure ES endpoint responded with unexpected code %d and body %s", statusCode, response.body().string());
            }
        } catch(IOException exc) {
            logger.error(String.format("Failed prepare Elasticsearch endpoint (%s): %s", modifier, esUrl), exc);
        }
    }

    private String readResourceContent(String resourcePath) throws IOException {
        InputStream resourceStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);
        if (resourceStream == null) {
            throw new IOException(String.format("ES Config resource %s not found in classpath", resourcePath));
        }
        try (final Reader reader = new InputStreamReader(resourceStream)) {
            return CharStreams.toString(reader);
        }
    }

    private boolean checkIndexExists(URL esUrl) {
        Request request = new Request.Builder()
            .url(esUrl)
            .head()
            .build();

        try {
            int statusCode = getClient().newCall(request).execute().code();
            switch(statusCode) {
                case SC_HTTP_OK:
                    return true;
                case SC_HTTP_NOT_FOUND:
                    return false;
                default:
                    logger.warn(String.format("Unexpected status code %d when checking ES Index at %s", statusCode, esUrl));
                    return false;
            }
        } catch(IOException exc) {
            logger.warn("Failed to verify index in Elasticsearch: " + esUrl, exc);
            return false;
        }
    }


}

