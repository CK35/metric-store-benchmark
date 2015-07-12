package de.ck35.metricstore.benchmark.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({de.ck35.metricstore.fs.configuration.RootConfiguration.class,
         de.ck35.metriccache.core.configuration.RootConfiguration.class,
         JMXConfiguration.class,
         MonitorConfiguration.class,
         BenchmarkConfiguration.class})
public class RootConfiguration {}