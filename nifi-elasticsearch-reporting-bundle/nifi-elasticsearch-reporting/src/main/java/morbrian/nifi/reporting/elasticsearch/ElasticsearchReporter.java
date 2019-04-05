package morbrian.nifi.reporting.elasticsearch;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.json.Json;
import javax.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class ElasticsearchReporter extends ScheduledReporter {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchReporter.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    public ElasticsearchReporter(MetricRegistry metricRegistry) {
        super(metricRegistry, "elasticsearch-reporter", MetricFilter.ALL,
            TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
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

        // log report
        String report = reportBuilder.build().toString();
        LOG.info(report);
    }

}

