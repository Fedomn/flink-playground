package org.playground.flink.datastream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;

/** <a href="https://github.com/rmetzger/tiny-flink-talk/tree/main">reference</a> */
public class MiniClusterDemo {
    public static void main(String[] args) throws Exception {
        var flinkConfig = new Configuration();
        flinkConfig.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("8m"));
        flinkConfig.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("8m"));

        MiniClusterConfiguration clusterConfig =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(1)
                        .setConfiguration(flinkConfig)
                        .build();
        try (var cluster = new MiniCluster(clusterConfig)) {
            cluster.start();
            Thread.sleep(1000);
            // cluster.submitJob(...);
        }
    }
}
