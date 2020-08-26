package org.example;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterInformationRetriever;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

import java.util.Collections;

/**
 * @author <p>
 * 通过api的方式以application的模式来提交flink任务到yarn集群
 */

public class SubmitJobApplicationMode {

    static {
        BasicConfigurator.configure();
    }

    public static void main(String[] args) {
        //hadoop 运行的用户名，根据实际调整
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        //flink的本地配置目录，为了得到flink的配置
        String configurationDirectory = "/usr/local/flink/conf/";
        //存放flink集群相关的jar包目录,此实验通过运行参数配置，根据实际调整
        String flinkLibs = "hdfs://10.211.55.21:9820/data/flink/lib";
        //用户jar，根据实际调整
        String userJarPath = "hdfs://10.211.55.21:9820/data/flink/userlib/WordCount.jar";
        //Flink Dist Jar,此实验通过运行参数配置，根据实际调整
        String flinkDistJar = "hdfs://10.211.55.21:9820/data/flink/libs/flink-dist_2.11-1.11.1.jar";

        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        //各配置文件，根据实际调整
        configuration.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        configuration.addResource(new Path("/usr/local/hadoop/etc/hadoop/yarn-site.xml"));
        configuration.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        YarnConfiguration yarnConfiguration = new YarnConfiguration(configuration);

        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        YarnClusterInformationRetriever clusterInformationRetriever = YarnClientYarnClusterInformationRetriever.create(yarnClient);

        //获取flink的配置
        Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration(configurationDirectory);
        flinkConfiguration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        flinkConfiguration.set(PipelineOptions.JARS, Collections.singletonList(userJarPath));

        Path remoteLib = new Path(flinkLibs);
        flinkConfiguration.set(YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList(remoteLib.toString()));

        flinkConfiguration.set(YarnConfigOptions.FLINK_DIST_JAR, flinkDistJar);
        //设置为application模式
        flinkConfiguration.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
        //yarn application name
        flinkConfiguration.set(YarnConfigOptions.APPLICATION_NAME, "jobName");


        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();

        //设置用户jar的参数和主类 (String[]参数形式 --x y)
        ApplicationConfiguration appConfig = new ApplicationConfiguration(args, null);


        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(flinkConfiguration, yarnConfiguration, yarnClient, clusterInformationRetriever, true);
        ClusterClientProvider<ApplicationId> clusterClientProvider = null;
        try {
            clusterClientProvider = yarnClusterDescriptor.deployApplicationCluster(clusterSpecification, appConfig);

            //get response information
            ClusterClient<ApplicationId> clusterClient = clusterClientProvider.getClusterClient();
            ApplicationId applicationId = clusterClient.getClusterId();
            System.out.println(applicationId);
        } catch (ClusterDeploymentException e) {
            e.printStackTrace();
        }
    }
}
