/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.flinkx.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.flink.flink.api.java.MyLocalStreamEnvironment;
import com.flink.flinkx.config.DataTransferConfig;
import com.flink.flinkx.config.SpeedConfig;
import com.flink.flinkx.constants.ConfigConstant;
import com.flink.flinkx.file.reader.FileReader;
import com.flink.flinkx.file.writer.FileWriter;
import com.flink.flinkx.mysql.reader.MysqlReader;
import com.flink.flinkx.mysql.writer.MysqlWriter;
import com.flink.flinkx.mysqld.reader.MysqldReader;
import com.flink.flinkx.reader.BaseDataReader;
import com.flink.flinkx.stream.reader.StreamReader;
import com.flink.flinkx.stream.writer.StreamWriter;
import com.flink.flinkx.util.ResultPrintUtil;
import com.flink.flinkx.writer.BaseDataWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author jiangbo
 */
public class LocalTest {

    private static final int FAILURE_RATE = 3;
    private static final int FAILURE_INTERVAL = 6;
    private static final int DELAY_INTERVAL = 10;
    public static Logger LOG = LoggerFactory.getLogger(LocalTest.class);
    public static Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {
        setLogLevel(Level.INFO.toString());

        Properties confProperties = new Properties();
//        confProperties.put("flink.checkpoint.interval", "10000");
//        confProperties.put("flink.checkpoint.stateBackend", "file:///tmp/flinkx_checkpoint");
//
        conf.setString("akka.ask.timeout", "180 s");
        conf.setString("web.timeout", "100000");
//        conf.setString("metrics.reporter.promgateway.class","org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter");
//        conf.setString("metrics.reporter.promgateway.host","172.16.8.178");
//        conf.setString("metrics.reporter.promgateway.port","9091");
//        conf.setString("metrics.reporter.promgateway.jobName","kanata");
//        conf.setString("metrics.reporter.promgateway.randomJobNameSuffix","true");
//        conf.setString("metrics.reporter.promgateway.deleteOnShutdown","false");

//        String jobPath = "D:\\dtstack\\flinkx-all\\flinkx-examples\\examples\\clickhouse_stream.json";
        String jobPath = "/Users/gerry/Desktop/www/java/flink/flinkx/jobs/file_stream.json";
        String savePointPath = "";
        JobExecutionResult result = LocalTest.runJob(new File(jobPath), confProperties, savePointPath);
        ResultPrintUtil.printResult(result);
    }

    public static JobExecutionResult runJob(File jobFile, Properties confProperties, String savePointPath) throws Exception {
        String jobContent = readJob(jobFile);
        return runJob(jobContent, confProperties, savePointPath);
    }

    public static JobExecutionResult runJob(String job, Properties confProperties, String savePointPath) throws Exception {
        DataTransferConfig config = DataTransferConfig.parse(job);
        MyLocalStreamEnvironment env = new MyLocalStreamEnvironment(conf);
        openCheckpointConf(env, confProperties);
        env.setParallelism(config.getJob().getSetting().getSpeed().getChannel());
        env.setRestartStrategy(RestartStrategies.noRestart());

        BaseDataReader reader = buildDataReader(config, env);
        DataStream<Row> dataStream = reader.readData();
        SpeedConfig speedConfig = config.getJob().getSetting().getSpeed();
        if (speedConfig.getReaderChannel() > 0) {
            dataStream = ((DataStreamSource<Row>) dataStream).setParallelism(speedConfig.getReaderChannel());
        }

        if (speedConfig.isRebalance()) {
            dataStream = dataStream.rebalance();
        }

        BaseDataWriter dataWriter = buildDataWriter(config);
        DataStreamSink<?> dataStreamSink = dataWriter.writeData(dataStream);
        if (speedConfig.getWriterChannel() > 0) {
            dataStreamSink.setParallelism(speedConfig.getWriterChannel());
        }

        if (StringUtils.isNotEmpty(savePointPath)) {
            env.setSettings(SavepointRestoreSettings.forPath(savePointPath));
        }

        return env.execute();
    }

    private static String readJob(File file) {
        try(FileInputStream in = new FileInputStream(file);) {
            byte[] fileContent = new byte[(int) file.length()];
            in.read(fileContent);
            return new String(fileContent, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static BaseDataReader buildDataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        String readerName = config.getJob().getContent().get(0).getReader().getName();
        BaseDataReader reader;
        switch (readerName) {
            case PluginNameConstants.STREAM_READER:
                reader = new StreamReader(config, env);
                break;
            case PluginNameConstants.MYSQLD_READER:
                reader = new MysqldReader(config, env);
                break;
            case PluginNameConstants.MYSQL_READER:
                reader = new MysqlReader(config, env);
                break;
            case PluginNameConstants.FILE_READER:
                reader = new FileReader(config, env);
                break;
            default:
                throw new IllegalArgumentException("Can not find reader by name:" + readerName);
        }

        return reader;
    }

    private static BaseDataWriter buildDataWriter(DataTransferConfig config) {
        String writerName = config.getJob().getContent().get(0).getWriter().getName();
        BaseDataWriter writer;
        switch (writerName) {
            case PluginNameConstants.STREAM_WRITER:
                writer = new StreamWriter(config);
                break;
            case PluginNameConstants.MYSQL_WRITER:
                writer = new MysqlWriter(config);
                break;
            case PluginNameConstants.FILE_WRITER:
                writer = new FileWriter(config);
                break;
            default:
                throw new IllegalArgumentException("Can not find writer by name:" + writerName);
        }

        return writer;
    }

    private static void openCheckpointConf(StreamExecutionEnvironment env, Properties properties) {
        if (properties == null) {
            return;
        }

        if (properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY) == null) {
            return;
        } else {
            long interval = Long.parseLong(properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY).trim());

            //start checkpoint every ${interval}
            env.enableCheckpointing(interval);

            LOG.info("Open checkpoint with interval:" + interval);
        }

        String checkpointTimeoutStr = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_TIMEOUT_KEY);
        if (checkpointTimeoutStr != null) {
            long checkpointTimeout = Long.parseLong(checkpointTimeoutStr);
            //checkpoints have to complete within one min,or are discard
            env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);

            LOG.info("Set checkpoint timeout:" + checkpointTimeout);
        }

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                FAILURE_RATE,
                Time.of(FAILURE_INTERVAL, TimeUnit.MINUTES),
                Time.of(DELAY_INTERVAL, TimeUnit.SECONDS)
        ));
    }

    private static void setLogLevel(String level) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.toLevel(level));
    }
}
