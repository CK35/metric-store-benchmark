package de.ck35.metricstore.benchmark;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import de.ck35.metriccache.api.MetricCacheRepository;
import de.ck35.metriccache.api.MetricCacheRequest;
import de.ck35.metriccache.api.MetricCacheRequest.FieldFilterBuilder;
import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.StoredMetricCallable;

@RunWith(MockitoJUnitRunner.class)
public class BenchmarkTest {

    private List<Entry<BucketInfo, ObjectNode>> testData;
    
    @Mock MetricCacheRepository repository;
    @Mock MetricCacheRequest request;
    @Mock FieldFilterBuilder fieldFilterBuilder;
    @Mock ObjectNode node;
    
    private Iterable<Entry<BucketInfo, ObjectNode>> testDataIterator;
    private Supplier<ExecutorService> executorServiceSupplier;
    private int threadCount;
    private int timeout;
    private TimeUnit unit;
    private boolean skip;

    @Mock MetricBucket metricBucket;


    @Before
    public void before() {
        BucketInfo bucketInfo = new BucketInfo("a", "a-type");
        this.testData = ImmutableList.of(Maps.immutableEntry(bucketInfo, node));
        this.testDataIterator = testData;
        this.executorServiceSupplier = Suppliers.ofInstance(Executors.newFixedThreadPool(1));
        this.threadCount = 1;
        this.timeout = 1;
        this.unit = TimeUnit.MINUTES;
        
        when(repository.listBuckets()).thenReturn(Collections.singleton(metricBucket));
        when(repository.request()).thenReturn(request);
        when(request.builder(any(StoredMetricCallable.class))).thenReturn(fieldFilterBuilder);
    }
    
    public WriteBenchmark benchmark() {
        return new WriteBenchmark(repository, testDataIterator, executorServiceSupplier, threadCount, timeout, unit, skip);
    }
    
    @Test
    public void test() {
        WriteBenchmark benchmark = benchmark();
        benchmark.run();
        verify(repository).wirte("a", "a-type", node);
    }
    
    @Test(expected=RuntimeException.class)
    public void testInterrupt() throws InterruptedException {
        ExecutorService executorService = mock(ExecutorService.class);
        this.executorServiceSupplier = Suppliers.ofInstance(executorService);
        when(executorService.awaitTermination(timeout, unit)).thenThrow(new InterruptedException());
        
        WriteBenchmark benchmark = benchmark();
        benchmark.run();
    }

}