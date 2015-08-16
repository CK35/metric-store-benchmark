package de.ck35.metricstore.benchmark.configuration;

import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Suppliers;

import de.ck35.metriccache.api.MetricCacheRepository;
import de.ck35.metricstore.api.MetricRepository;
import de.ck35.metricstore.benchmark.BucketInfo;
import de.ck35.metricstore.benchmark.DataGenerator;
import de.ck35.metricstore.benchmark.DataIterable;
import de.ck35.metricstore.benchmark.Monitor;
import de.ck35.metricstore.benchmark.ReadBenchmark;
import de.ck35.metricstore.benchmark.ReadVerification;
import de.ck35.metricstore.benchmark.Reporter;
import de.ck35.metricstore.benchmark.Reporter.FileWriterSupplier;
import de.ck35.metricstore.benchmark.WriteBenchmark;

@Configuration
public class BenchmarkConfiguration {

    @Autowired MetricRepository metricRepository;
	@Autowired MetricCacheRepository cacheRepository;
	@Autowired ObjectMapper mapper;
	@Autowired Environment env;
	@Autowired Monitor monitor;
	
	@Bean
	public WriteBenchmark writeBenchmark() {
	    int threadCount = env.getProperty("metricstore.benchmark.threadcount", Integer.class, 10);
		return new WriteBenchmark(cacheRepository, 
		                          dataIterable(),
		                          Suppliers.ofInstance(Executors.newFixedThreadPool(threadCount)),
		                          threadCount,
		                          env.getProperty("metricstore.benchmark.timeout", Integer.class, 60),
		                          env.getProperty("metricstore.benchmark.timeout.unit", TimeUnit.class, TimeUnit.MINUTES),
		                          env.getProperty("metricstore.benchmark.write.skip", Boolean.class, false));
	}
	
	@Bean
	public ReadBenchmark readBenchmark() {
	    return new ReadBenchmark(dataInterval(), 
	                             cacheRepository,
	                             env.getProperty("metricstore.benchmark.read.skip", Boolean.class, false));
	}

	@Bean
	public Reporter reporter() {
	    FileWriterSupplier supplier = new FileWriterSupplier(Paths.get(env.getProperty("metricstore.benchmark.report.file", "benchmark-result.csv")), 
	                                                         Charset.forName(env.getProperty("metricstore.benchmark.report.charset", String.class, "UTF-8")));
	    return new Reporter(monitor,
	                        env.getProperty("metricstore.benchmark.report.separator", ";"),
	                        DateTimeFormat.forPattern(env.getProperty("metricstore.benchmark.report.dateTimeFormat", "HH:mm:ss")),
	                        supplier);
	}
	
	@Bean
	public Random random() {
	    return new Random();
	}
	
	@Bean
	public Iterable<Entry<BucketInfo, ObjectNode>> dataIterable() {
	    return new DataIterable(random(), dataGenerator());
	}
	
	@Bean
	public Interval dataInterval() {
	    Period testPeriod = Period.parse(env.getProperty("metricstore.benchmark.period", "PT1h"));
        return new Interval(testPeriod, DateTime.now());
	}
	
	@Bean
	public DataGenerator dataGenerator() {
	    return new DataGenerator(mapper.getNodeFactory(),
	                             env.getProperty("metricstore.benchmark.data.buckets", Integer.class, 4),
	                             dataInterval(),
	                             env.getProperty("metricstore.benchmark.data.nodesPerMinute", Integer.class, 10_000),
	                             env.getProperty("metricstore.benchmark.data.fieldsPerNode", Integer.class, 5),
	                             env.getProperty("metricstore.benchmark.data.fieldValueLength", Integer.class, 20),
	                             env.getProperty("metricstore.benchmark.data.numberOfRandomFieldValues", Integer.class, 10_000));
	}
	
	@Bean
	public ReadVerification readVerification() {
	    return new ReadVerification(metricRepository,
	                                cacheRepository, 
	                                dataInterval(), 
	                                dataGenerator(),
	                                env.getProperty("metricstore.benchmark.readverification.skip", Boolean.class, false));
	}
}