package com.appdynamics.extensions.metricloader;

import com.appdynamics.extensions.AMonitorTaskRunnable;
import com.appdynamics.extensions.MetricWriteHelper;
import com.appdynamics.extensions.conf.MonitorContextConfiguration;
import com.appdynamics.extensions.logging.ExtensionsLoggerFactory;
import com.appdynamics.extensions.metrics.Metric;
import com.google.common.collect.Lists;
import com.opencsv.CSVReader;
import com.appdynamics.extensions.metricloader.config.PathToProcess;
import org.slf4j.Logger;

import java.io.FileReader;
import java.util.List;
import java.util.Map;

public class MetricLoaderTask implements AMonitorTaskRunnable {

    private static final Logger LOGGER = ExtensionsLoggerFactory.getLogger(MetricLoaderTask.class);
    private final PathToProcess pathToProcess;
    private final MetricWriteHelper metricWriteHelper;
    private final MonitorContextConfiguration contextConfiguration;
    private final String metricPathPrefix;

    public MetricLoaderTask(MonitorContextConfiguration contextConfiguration, MetricWriteHelper metricWriteHelper, PathToProcess pathToProcess) {
        this.pathToProcess = pathToProcess;
        this.contextConfiguration = contextConfiguration;
        this.metricWriteHelper = metricWriteHelper;
        this.metricPathPrefix = contextConfiguration.getMetricPrefix() + "|" + pathToProcess.getDisplayName() + "|";
        //do not cache
        this.metricWriteHelper.setCacheMetrics(false);
    }

    @Override
    public void onTaskComplete() {
        LOGGER.info("Completed task for name "+pathToProcess.getDisplayName());
    }

    @Override
    public void run() {
        try {
            List<Metric> metrics = Lists.newArrayList();
            //open csv, get header, read vals, make metric
            CSVReader reader = new CSVReader(new FileReader(pathToProcess.getPath()));
            List<String[]> allRows = reader.readAll();
            //we only push metrics with header as name and the first row after header
            String[] h = allRows.get(0);
            String[] r = allRows.get(1);
            for(int i=0;i<h.length;i++)
            {
                LOGGER.debug("key: {}, value: {}",h[i], r[i]);
                metrics.add(new Metric(h[i], r[i], metricPathPrefix+h[i]));
            }
            metricWriteHelper.transformAndPrintMetrics(metrics);
        } catch (Exception ex) {
            LOGGER.error("Metrics in file {} failed to process!", pathToProcess.getPath(), ex);
        }
    }
}
