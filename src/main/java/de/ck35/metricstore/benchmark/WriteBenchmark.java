package de.ck35.metricstore.benchmark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Supplier;

import de.ck35.metriccache.api.MetricCacheRepository;

public class WriteBenchmark implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(WriteBenchmark.class);
    
	private final MetricCacheRepository repository;
	private final Iterable<Entry<BucketInfo, ObjectNode>> dataIterable;

	private final Supplier<ExecutorService> executorServiceSupplier;
    private final int threadCount;
    private final int timeout;
    private final TimeUnit unit;

    private final boolean skip;

	public WriteBenchmark(MetricCacheRepository repository, 
	                 Iterable<Entry<BucketInfo, ObjectNode>> testDataIterator,
	                 Supplier<ExecutorService> executorServiceSupplier,
	                 int threadCount,
	                 int timeout,
	                 TimeUnit unit, 
	                 boolean skip) {
        this.repository = repository;
        this.dataIterable = testDataIterator;
        this.executorServiceSupplier = executorServiceSupplier;
        this.threadCount = threadCount;
        this.timeout = timeout;
        this.unit = unit;
        this.skip = skip;
    }

    @Override
	public void run() {
        if(skip) {
            LOG.info("Skipping write benchmark.");
            return;
        }
	    LOG.info("Starting write benchmark.");
	    DataSupplier dataSupplier = new DataSupplier(dataIterable.iterator());
	    ExecutorService threadPool = executorServiceSupplier.get();
	    try {
	        List<Future<?>> futures = new ArrayList<Future<?>>(threadCount);
	        for(int thread = 0 ; thread < threadCount ; thread++) {
	            futures.add(threadPool.submit(new MetricRepositoryWriter(repository, dataSupplier)));
	        }
	        for(Future<?> future : futures) {
	            future.get(timeout, unit);
	        }
	        threadPool.shutdownNow();
	    } catch (InterruptedException | ExecutionException | TimeoutException e) {
	        throw new RuntimeException("Benchmark failed!", e);
        } finally {
	        threadPool.shutdownNow();
	    }
	    LOG.info("Writebenchmark done.");
	}
    
    public static class DataSupplier implements Supplier<Entry<BucketInfo, ObjectNode>> {
        
        private final Iterator<Entry<BucketInfo, ObjectNode>> iter;
        
        public DataSupplier(Iterator<Entry<BucketInfo, ObjectNode>> iter) {
            this.iter = iter;
        }
        @Override
        public synchronized Entry<BucketInfo, ObjectNode> get() {
            if(iter.hasNext()) {
                return iter.next();
            } else {                
                return null;
            }
        }
    }

    public static class MetricRepositoryWriter implements Runnable {
        
        private final MetricCacheRepository repository;
        private final Supplier<Entry<BucketInfo, ObjectNode>> dataSupplier;

        public MetricRepositoryWriter(MetricCacheRepository repository, Supplier<Entry<BucketInfo, ObjectNode>> dataSupplier) {
            this.repository = repository;
            this.dataSupplier = dataSupplier;
        }
        @Override
        public void run() {
            for(Entry<BucketInfo, ObjectNode> next = dataSupplier.get() ; next != null ; next = dataSupplier.get()) {
                repository.wirte(next.getKey().getBucketName(), next.getKey().getBucketType(), next.getValue());
            }
        }
    }
}