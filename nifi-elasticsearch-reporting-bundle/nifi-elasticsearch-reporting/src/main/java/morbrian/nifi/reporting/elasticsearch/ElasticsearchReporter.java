package morbrian.nifi.reporting.elasticsearch;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class ElasticsearchReporter extends ScheduledReporter {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchReporter.class);

    public ElasticsearchReporter(MetricRegistry metricRegistry) {
        super(metricRegistry, "elasticsearch-reporter", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        System.out.println("BCM Report...");
        LOG.info("BCM2 Report...");


    }

}

