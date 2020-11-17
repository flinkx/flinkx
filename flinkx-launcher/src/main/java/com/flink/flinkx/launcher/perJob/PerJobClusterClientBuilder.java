/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flink.flinkx.launcher.perJob;

import com.flink.flinkx.launcher.YarnConfLoader;
import com.flink.flinkx.options.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 * @author tudou
 */
public class PerJobClusterClientBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(com.flink.flinkx.launcher.perJob.PerJobClusterClientBuilder.class);

    public static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";

	public static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";

    private YarnClient yarnClient;

    private YarnConfiguration yarnConf;

    private Configuration flinkConfig;

    /**
     * init yarnClient
     * @param launcherOptions flinkx args
     * @param conProp flink args
     */
    public void init(Options launcherOptions, Properties conProp) throws Exception {
        String yarnConfDir = launcherOptions.getYarnconf();
        if(StringUtils.isBlank(yarnConfDir)) {
            throw new RuntimeException("parameters of yarn is required");
        }
        flinkConfig = launcherOptions.loadFlinkConfiguration();
        conProp.forEach((key, val) -> flinkConfig.setString(key.toString(), val.toString()));
        SecurityUtils.install(new SecurityConfiguration(flinkConfig));

        yarnConf = YarnConfLoader.getYarnConf(yarnConfDir);
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();

        LOG.info("----init yarn success ----");
    }

    /**
     * create a yarn cluster descriptor which is used to start the application master
     * @param launcherOptions LauncherOptions
     * @return
     * @throws MalformedURLException
     */
    public YarnClusterDescriptor createPerJobClusterDescriptor(Options launcherOptions) throws MalformedURLException {
        String flinkJarPath = launcherOptions.getFlinkLibJar();
        if (StringUtils.isNotBlank(flinkJarPath)) {
            if (!new File(flinkJarPath).exists()) {
                throw new IllegalArgumentException("The Flink jar path is not exist");
            }
        } else {
            throw new IllegalArgumentException("The Flink jar path is null");
        }
        File logback = new File(launcherOptions.getFlinkconf()+ File.separator + CONFIG_FILE_LOGBACK_NAME);
        if(logback.exists()){
            flinkConfig.setString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, launcherOptions.getFlinkconf()+ File.separator + CONFIG_FILE_LOGBACK_NAME);
        }else{
            File log4j = new File(launcherOptions.getFlinkconf()+ File.separator + CONFIG_FILE_LOG4J_NAME);
            if(log4j.exists()){
                flinkConfig.setString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, launcherOptions.getFlinkconf()+ File.separator + CONFIG_FILE_LOG4J_NAME);
            }

        }

        YarnClusterDescriptor descriptor = new YarnClusterDescriptor(
                flinkConfig,
                yarnConf,
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);

        List<File> shipFiles = new ArrayList<>();
        File[] jars = new File(flinkJarPath).listFiles();
        if (jars != null) {
            for (File jar : jars) {
                if (jar.toURI().toURL().toString().contains("flink-dist")) {
                    descriptor.setLocalJarPath(new Path(jar.toURI().toURL().toString()));
                } else {
                    shipFiles.add(jar);
                }
            }
        }

        descriptor.addShipFiles(shipFiles);
        return descriptor;
    }
}