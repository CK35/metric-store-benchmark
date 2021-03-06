package de.ck35.metricstore.benchmark;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;

public class Monitor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Monitor.class);
    
	private final Supplier<Optional<Long>> totalProcessedCommandsSupplier;
	private final Supplier<Optional<Long>> totalReadsSupplier;
	private final Supplier<Optional<Double>> cpuUsageSupplier;
	private final Supplier<Optional<Double>> heapUsageSupplier;
	
	private final AtomicBoolean enabled;
	private final CountDownLatch runLatch;
	private final CountDownLatch resultLatch;
	private final AtomicReference<NavigableMap<DateTime, SystemState>> resultReference;

    private final int pollTimeout;
    private final TimeUnit unit;

	
    public Monitor(Supplier<Optional<Long>> totalProcessedCommandsSupplier,
                   Supplier<Optional<Long>> totalReadsSupplier,
                   Supplier<Optional<Double>> cpuUsageSupplier,
                   Supplier<Optional<Double>> heapUsageSupplier,
                   int pollTimeout, 
                   TimeUnit unit) {
        this.totalProcessedCommandsSupplier = totalProcessedCommandsSupplier;
        this.totalReadsSupplier = totalReadsSupplier;
		this.cpuUsageSupplier = cpuUsageSupplier;
		this.heapUsageSupplier = heapUsageSupplier;
        this.pollTimeout = pollTimeout;
        this.unit = unit;
        this.enabled = new AtomicBoolean(true);
        this.runLatch = new CountDownLatch(1);
        this.resultLatch = new CountDownLatch(1);
        this.resultReference = new AtomicReference<>();
	}
	
	@Override
	public void run() {
	    this.runLatch.countDown();
	    ImmutableSortedMap.Builder<DateTime, SystemState> stateMap = ImmutableSortedMap.naturalOrder();
	    try {
	        try {
	            Optional<Entry<DateTime, SystemState>> lastState = Optional.absent();
	            while(!Thread.interrupted()) {
	                DateTime now = now();
	                lastState = Optional.of(Maps.immutableEntry(now, systemState(now, lastState)));
	                stateMap.put(now, lastState.get().getValue());
	                if(enabled.get()) {	                	
	                	Thread.sleep(TimeUnit.MILLISECONDS.convert(pollTimeout, unit));
	                } else {
	                	return;
	                }
	            }
	        } catch (InterruptedException e) {
	            LOG.warn("Monitor Thread interrupted.");
	        }
	    } finally {
	        resultReference.set(stateMap.build());
	        resultLatch.countDown();
	    }
	}
	
	public void awaitRun() throws InterruptedException {
	    this.runLatch.await();
	}
	
	public NavigableMap<DateTime, SystemState> awaitResult() throws InterruptedException {
	    this.enabled.set(false);
	    this.resultLatch.await();
        return resultReference.get();
    }

    protected SystemState systemState(DateTime now, Optional<Entry<DateTime, SystemState>> lastState) {
    	Optional<Entry<Long, SystemState>> last = optionalDurationMillis(now, lastState);
    	Optional<Long> totalProcessedCommands = totalProcessedCommandsSupplier.get();
    	Optional<Long> totalReadCalls = totalReadsSupplier.get();
    	Optional<Double> processedCommandsPerSecond;
    	Optional<Double> readCallsPerSecond;
    	if(last.isPresent()) {
    	    processedCommandsPerSecond = elementsPerSecond(totalProcessedCommands, last.get().getValue().getTotalProcessedCommands(), last.get().getKey());
    	    readCallsPerSecond = elementsPerSecond(totalReadCalls, last.get().getValue().getTotalReadCalls(), last.get().getKey());
    	} else {
    		processedCommandsPerSecond = Optional.absent();
    		readCallsPerSecond = Optional.absent();
    	}
        return new SystemState(cpuUsageSupplier.get(), 
                               heapUsageSupplier.get(),
                               totalProcessedCommands, 
                               processedCommandsPerSecond,
                               totalReadCalls,
                               readCallsPerSecond);
    }
    
    public static Optional<Double> elementsPerSecond(Optional<Long> totalElements, Optional<Long> lastTotalElements, long durationMillis) {
        long elements = totalElements.or(0l) - lastTotalElements.or(0l);
        if(elements < 0) {
            return Optional.absent();
        } else {
            return Optional.of((elements/(double) durationMillis)*1000.0);
        }
    }
    
    public static Optional<Entry<Long, SystemState>> optionalDurationMillis(DateTime now, Optional<Entry<DateTime, SystemState>> lastState) {
    	if(lastState.isPresent()) {
    		return Optional.of(Maps.immutableEntry(new Interval(lastState.get().getKey(), now).toDurationMillis(), lastState.get().getValue()));
    	} else {
    		return Optional.absent();
    	}
    }
    
	public DateTime now() {
	    return DateTime.now();
	}
	
	public static class SystemState {
	    
	    private final Optional<Double> cpuUsage;
	    private final Optional<Double> heapUsage;
	    private final Optional<Long> totalProcessedCommands;
	    private final Optional<Double> processedCommandsPerSecond;
	    private final Optional<Long> totalReadCalls;
	    private final Optional<Double> totalReadCallsPerSecond;
	    
		public SystemState(Optional<Double> cpuUsage, 
		                   Optional<Double> heapUsage, 
		                   Optional<Long> totalProcessedCommands, 
		                   Optional<Double> processedCommandsPerSecond,
		                   Optional<Long> totalReadCalls,
		                   Optional<Double> totalReadCallsPerSecond) {
			this.cpuUsage = cpuUsage;
			this.heapUsage = heapUsage;
			this.totalProcessedCommands = totalProcessedCommands;
			this.processedCommandsPerSecond = processedCommandsPerSecond;
            this.totalReadCalls = totalReadCalls;
            this.totalReadCallsPerSecond = totalReadCallsPerSecond;
		}

		public Optional<Double> getCpuUsage() {
			return cpuUsage;
		}
		public Optional<Double> getHeapUsage() {
			return heapUsage;
		}
		public Optional<Long> getTotalProcessedCommands() {
			return totalProcessedCommands;
		}
		public Optional<Double> getProcessedCommandsPerSecond() {
			return processedCommandsPerSecond;
		}
		public Optional<Long> getTotalReadCalls() {
            return totalReadCalls;
        }
		public Optional<Double> getTotalReadCallsPerSecond() {
            return totalReadCallsPerSecond;
        }
	}
}