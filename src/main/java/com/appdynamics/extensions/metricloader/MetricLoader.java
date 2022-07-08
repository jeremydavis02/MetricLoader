package com.appdynamics.extensions.metricloader;

import com.appdynamics.extensions.ABaseMonitor;
import com.appdynamics.extensions.TasksExecutionServiceProvider;
import com.appdynamics.extensions.conf.MonitorContextConfiguration;
import com.appdynamics.extensions.logging.ExtensionsLoggerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.appdynamics.extensions.metricloader.config.PathToProcess;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;


import static com.appdynamics.extensions.util.AssertUtils.assertNotNull;
import static util.Constants.CONFIGURED_PATHS;

/**
 * Created with IntelliJ IDEA.
 *  * User: jeremy.davis, Jeremy Davis
 *  * Date: 02/03/22
 */
public class MetricLoader extends ABaseMonitor {

    private static final String METRIC_PREFIX = "Custom Metrics|Metric Loader";
    private static final Logger LOGGER = ExtensionsLoggerFactory.getLogger(MetricLoader.class);
    private List<PathToProcess> pathsToProcess;
    private MonitorContextConfiguration monitorContextConfiguration;
    private Map<String, ?> configYml = Maps.newHashMap();

    @Override
    protected String getDefaultMetricPrefix() {
        return METRIC_PREFIX;
    }

    @Override
    public String getMonitorName() {
        return "Metric Loader";
    }

    @Override
    protected void doRun(TasksExecutionServiceProvider tasksExecutionServiceProvider) {
        initMonitor();
        assertNotNull(pathsToProcess, "Please configure the paths to be processed in your config.yml");
        for (PathToProcess pathToProcess : pathsToProcess) {
            MetricLoaderTask task = new MetricLoaderTask(monitorContextConfiguration,
                    tasksExecutionServiceProvider.getMetricWriteHelper(), pathToProcess);
            tasksExecutionServiceProvider.submit(pathToProcess.getDisplayName(), task);
        }
    }

    @Override
    protected List<Map<String, ?>> getServers() {
        try{
            LOGGER.debug(CONFIGURED_PATHS);
            LOGGER.debug(configYml.toString());
            Object p = getContextConfiguration().getConfigYml().get(CONFIGURED_PATHS);
            LOGGER.debug(p.toString());
            return (List<Map<String, ?>>) p;
        }
        catch(Exception ex){
            LOGGER.error("Could not get paths in config.yml", ex);
            return null;
        }

    }

    private void initMonitor() {
        try {
            monitorContextConfiguration = getContextConfiguration();
            configYml = monitorContextConfiguration.getConfigYml();
            pathsToProcess = getPathsToProcess(getServers());
            LOGGER.info("The Paths to process: "+pathsToProcess.toString());

        } catch (Exception ex) {
            LOGGER.error("Error encountered while getting paths to process from config", ex);
        }
    }

    private List<PathToProcess> getPathsToProcess(List<Map<String, ?>> configuredPaths) {
        List<PathToProcess> pathsToProcess = Lists.newArrayList();
        for (Map<String, ?> path : configuredPaths) {
            pathsToProcess.add(new PathToProcess() {{
                setDisplayName((String) path.get("displayName"));
                setPath((String) path.get("path"));
                /*setIgnoreHiddenFiles(Boolean.valueOf(path.get("ignoreHiddenFiles").toString()));
                setEnableRecursiveFileCounts(Boolean.valueOf(path.get("recursiveFileCounts").toString()));
                setExcludeSubdirectoryCount(Boolean.valueOf(path.get("excludeSubdirectoriesFromFileCount").toString()));
                setEnableRecursiveFileSizes(Boolean.valueOf(path.get("recursiveFileSizes").toString()));*/
            }});
        }
        return pathsToProcess;
    }
}
