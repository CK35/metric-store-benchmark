package de.ck35.metricstore.benchmark;

import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ck35.metriccache.api.MetricCacheRepository;
import de.ck35.metriccache.api.MetricCacheRequest;
import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.api.StoredMetricCallable;

public class ReadBenchmark implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ReadBenchmark.class);
    
    private final Interval interval;
    private final MetricCacheRepository cacheRepository;

    private final boolean skip;
    
    public ReadBenchmark(Interval interval, MetricCacheRepository cacheRepository, boolean skip) {
        this.interval = interval;
        this.cacheRepository = cacheRepository;
        this.skip = skip;
    }

    @Override
    public void run() {
        if(skip) {
            LOG.info("Skipping read benchmark.");
        }
        LOG.info("Starting read benchmark.");
        final AtomicLong readCalls = new AtomicLong();
        for(MetricBucket metricBucket : cacheRepository.listBuckets()) {
            LOG.info("Reading from: '{}'", metricBucket.getName());
            MetricCacheRequest request = cacheRepository.request();
            request.builder(new StoredMetricCallable() {
                @Override
                public void call(StoredMetric node) {
                    readCalls.incrementAndGet();
                }
            }).build();
            request.read(metricBucket.getName(), interval);
        }
        LOG.info("Read: '{}' nodes.", readCalls.get());
        LOG.info("Readbenchmark done.");
    }
}