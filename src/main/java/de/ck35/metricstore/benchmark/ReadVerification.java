package de.ck35.metricstore.benchmark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Supplier;

import de.ck35.metriccache.api.MetricCache;
import de.ck35.metriccache.api.MetricCacheRequest;
import de.ck35.metricstore.api.MetricRepository;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.api.StoredMetricCallable;

public class ReadVerification implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ReadVerification.class);
    
    private final MetricRepository metricRepository;
    private final MetricCache cacheRepository;
    private final Supplier<List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>>> dataSupplier;
    private final Interval interval;
    private final boolean skip;
    
    public ReadVerification(MetricRepository metricRepository,
                            MetricCache cacheRepository,
                            Interval interval,
                            Supplier<List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>>> dataSupplier,
                            boolean skip) {
        this.metricRepository = metricRepository;
        this.cacheRepository = cacheRepository;
        this.interval = interval;
        this.dataSupplier = dataSupplier;
        this.skip = skip;
    }

    @Override
    public void run() {
        if(skip) {
            LOG.info("Skipping read verification.");
            return;
        }
        LOG.info("Starting read verification.");
        List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>> data = dataSupplier.get();
        if(data.isEmpty()) {
            LOG.warn("No data provided. Skipping read verification.");
            return;
        }
        for(Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>> entry : data) {
            String bucketName = entry.getKey().getBucketName();
            String bucketType = entry.getKey().getBucketType();
            LOG.info("Verifying metric repository with Bucket: '{}'.", bucketName);
            metricRepository.read(bucketName, interval, new VerificationCallable(bucketName, bucketType, entry.getValue().iterator()));
            
            LOG.info("Verifying metric cache with Bucket: '{}'.", bucketName);
            MetricCacheRequest request = cacheRepository.request();
            VerificationCallable callable = new VerificationCallable(bucketName, bucketType, entry.getValue().iterator());
            request.builder(callable).build();
            request.read(bucketName, interval);
        }
        LOG.info("Read verification done.");
    }
    
    public static class VerificationCallable implements StoredMetricCallable {
        
        private final Iterator<Entry<DateTime, ObjectNode>> expectedNodes;
        private final String bucketName;
        private final String bucketType;
        
        private Entry<DateTime, ObjectNode> next;
        private DateTime currentTimestamp;
        private List<ObjectNode> currentNodes;

        
        public VerificationCallable(String bucketName, String bucketType, Iterator<Entry<DateTime, ObjectNode>> expectedNodes) {
            this.bucketName = bucketName;
            this.bucketType = bucketType;
            this.expectedNodes = expectedNodes;
            this.currentTimestamp = null;
            this.currentNodes = new ArrayList<>();
        }
        @Override
        public void call(StoredMetric node) {
            if(currentNodes.isEmpty() && !expectedNodes.hasNext() && next == null) {
                throw new IllegalArgumentException(toIllegalArgumentMessage(node));
            } else if(currentNodes.isEmpty()) {
                if(next == null) {
                    next = expectedNodes.next();
                }
                currentTimestamp = next.getKey();
                currentNodes.add(next.getValue());
                while(expectedNodes.hasNext()) {
                    next = expectedNodes.next();
                    if(currentTimestamp.equals(next.getKey())) {
                        currentNodes.add(next.getValue());
                    } else {
                        break;
                    }
                }
            }
            if(!bucketName.equals(node.getMetricBucket().getName())) {
                throw new IllegalArgumentException("Wrong bucket name! Expected: '" + bucketName + "' but read: '" + node.getMetricBucket().getName() + "'!");
            }
            if(!bucketType.equals(node.getMetricBucket().getType())) {
                throw new IllegalArgumentException("Wrong bucket type! Expected: '" + bucketType + "' but read: '" + node.getMetricBucket().getType() + "'!");
            }
            if(!currentTimestamp.withZone(DateTimeZone.UTC).withSecondOfMinute(0).withMillisOfSecond(0).equals(node.getTimestamp().withZone(DateTimeZone.UTC))) {
                throw new IllegalArgumentException(toIllegalArgumentMessage(node));
            }
            if(!currentNodes.remove(node.getObjectNode())) {
                throw new IllegalArgumentException(toIllegalArgumentMessage(node));
            }
        }
        private static String toIllegalArgumentMessage(StoredMetric node) {
            return "Unexpected node read with Bucket: '" + node.getMetricBucket().getName() + "' " +
                   "timestamp: '" + node.getTimestamp() + "' " +
                   "and object: '" + node.getObjectNode() + "'!";
        }
    }
}