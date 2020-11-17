//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.flink.api.java;

import com.esotericsoftware.kryo.Serializer;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.io.IteratorInputFormat;
import org.apache.flink.api.java.io.ParallelIteratorInputFormat;
import org.apache.flink.api.java.io.PrimitiveInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.io.TextValueInputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.api.java.utils.PlanGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.NumberSequenceIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SplittableIterator;
import org.apache.flink.util.WrappingRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Public
public class ExecutionEnvironment {
    protected static final Logger LOG = LoggerFactory.getLogger(ExecutionEnvironment.class);
    private static ExecutionEnvironmentFactory contextEnvironmentFactory = null;
    private static final ThreadLocal<ExecutionEnvironmentFactory> threadLocalContextEnvironmentFactory = new ThreadLocal();
    private static int defaultLocalDop = Runtime.getRuntime().availableProcessors();
    private final List<DataSink<?>> sinks;
    private final List<Tuple2<String, DistributedCacheEntry>> cacheFile;
    private final ExecutionConfig config;
    protected JobExecutionResult lastJobExecutionResult;
    private boolean wasExecuted;
    private final PipelineExecutorServiceLoader executorServiceLoader;
    private final Configuration configuration;
    private final ClassLoader userClassloader;
    private final List<JobListener> jobListeners;

    @PublicEvolving
    public ExecutionEnvironment(Configuration configuration) {
        this(configuration, (ClassLoader)null);
    }

    @PublicEvolving
    public ExecutionEnvironment(Configuration configuration, ClassLoader userClassloader) {
        this(new DefaultExecutorServiceLoader(), configuration, userClassloader);
    }

    @PublicEvolving
    public ExecutionEnvironment(PipelineExecutorServiceLoader executorServiceLoader, Configuration configuration, ClassLoader userClassloader) {
        this.sinks = new ArrayList();
        this.cacheFile = new ArrayList();
        this.config = new ExecutionConfig();
        this.wasExecuted = false;
        this.jobListeners = new ArrayList();
        this.executorServiceLoader = (PipelineExecutorServiceLoader)Preconditions.checkNotNull(executorServiceLoader);
        this.configuration = (Configuration)Preconditions.checkNotNull(configuration);
        this.userClassloader = userClassloader == null ? this.getClass().getClassLoader() : userClassloader;
        this.configure(this.configuration, this.userClassloader);
    }

    protected ExecutionEnvironment() {
        this(new Configuration());
    }

    @Internal
    public ClassLoader getUserCodeClassLoader() {
        return this.userClassloader;
    }

    @Internal
    public PipelineExecutorServiceLoader getExecutorServiceLoader() {
        return this.executorServiceLoader;
    }

    @Internal
    public Configuration getConfiguration() {
        return this.configuration;
    }

    public ExecutionConfig getConfig() {
        return this.config;
    }

    protected List<JobListener> getJobListeners() {
        return this.jobListeners;
    }

    public int getParallelism() {
        return this.config.getParallelism();
    }

    public void setParallelism(int parallelism) {
        this.config.setParallelism(parallelism);
    }

    @PublicEvolving
    public void setRestartStrategy(RestartStrategyConfiguration restartStrategyConfiguration) {
        this.config.setRestartStrategy(restartStrategyConfiguration);
    }

    @PublicEvolving
    public RestartStrategyConfiguration getRestartStrategy() {
        return this.config.getRestartStrategy();
    }

    /** @deprecated */
    @Deprecated
    @PublicEvolving
    public void setNumberOfExecutionRetries(int numberOfExecutionRetries) {
        this.config.setNumberOfExecutionRetries(numberOfExecutionRetries);
    }

    /** @deprecated */
    @Deprecated
    @PublicEvolving
    public int getNumberOfExecutionRetries() {
        return this.config.getNumberOfExecutionRetries();
    }

    public JobExecutionResult getLastJobExecutionResult() {
        return this.lastJobExecutionResult;
    }

    public <T extends Serializer<?> & Serializable> void addDefaultKryoSerializer(Class<?> type, T serializer) {
        this.config.addDefaultKryoSerializer(type, serializer);
    }

    public void addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
        this.config.addDefaultKryoSerializer(type, serializerClass);
    }

    public <T extends Serializer<?> & Serializable> void registerTypeWithKryoSerializer(Class<?> type, T serializer) {
        this.config.registerTypeWithKryoSerializer(type, serializer);
    }

    public void registerTypeWithKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
        this.config.registerTypeWithKryoSerializer(type, serializerClass);
    }

    public void registerType(Class<?> type) {
        if (type == null) {
            throw new NullPointerException("Cannot register null type class.");
        } else {
            TypeInformation<?> typeInfo = TypeExtractor.createTypeInfo(type);
            if (typeInfo instanceof PojoTypeInfo) {
                this.config.registerPojoType(type);
            } else {
                this.config.registerKryoType(type);
            }

        }
    }

    @PublicEvolving
    public void configure(ReadableConfig configuration, ClassLoader classLoader) {
        configuration.getOptional(DeploymentOptions.JOB_LISTENERS).ifPresent((listeners) -> {
            this.registerCustomListeners(classLoader, listeners);
        });
        configuration.getOptional(PipelineOptions.CACHED_FILES).ifPresent((f) -> {
            this.cacheFile.clear();
            this.cacheFile.addAll(DistributedCache.parseCachedFilesFromString(f));
        });
        this.config.configure(configuration, classLoader);
    }

    private void registerCustomListeners(ClassLoader classLoader, List<String> listeners) {
        Iterator var3 = listeners.iterator();

        while(var3.hasNext()) {
            String listener = (String)var3.next();

            try {
                JobListener jobListener = (JobListener)InstantiationUtil.instantiate(listener, JobListener.class, classLoader);
                this.jobListeners.add(jobListener);
            } catch (FlinkException var6) {
                throw new WrappingRuntimeException("Could not load JobListener : " + listener, var6);
            }
        }

    }

    public DataSource<String> readTextFile(String filePath) {
        Preconditions.checkNotNull(filePath, "The file path may not be null.");
        return new DataSource(this, new TextInputFormat(new Path(filePath)), BasicTypeInfo.STRING_TYPE_INFO, Utils.getCallLocationName());
    }

    public DataSource<String> readTextFile(String filePath, String charsetName) {
        Preconditions.checkNotNull(filePath, "The file path may not be null.");
        TextInputFormat format = new TextInputFormat(new Path(filePath));
        format.setCharsetName(charsetName);
        return new DataSource(this, format, BasicTypeInfo.STRING_TYPE_INFO, Utils.getCallLocationName());
    }

    public DataSource<StringValue> readTextFileWithValue(String filePath) {
        Preconditions.checkNotNull(filePath, "The file path may not be null.");
        return new DataSource(this, new TextValueInputFormat(new Path(filePath)), new ValueTypeInfo(StringValue.class), Utils.getCallLocationName());
    }

    public DataSource<StringValue> readTextFileWithValue(String filePath, String charsetName, boolean skipInvalidLines) {
        Preconditions.checkNotNull(filePath, "The file path may not be null.");
        TextValueInputFormat format = new TextValueInputFormat(new Path(filePath));
        format.setCharsetName(charsetName);
        format.setSkipInvalidLines(skipInvalidLines);
        return new DataSource(this, format, new ValueTypeInfo(StringValue.class), Utils.getCallLocationName());
    }

    public <X> DataSource<X> readFileOfPrimitives(String filePath, Class<X> typeClass) {
        Preconditions.checkNotNull(filePath, "The file path may not be null.");
        return new DataSource(this, new PrimitiveInputFormat(new Path(filePath), typeClass), TypeExtractor.getForClass(typeClass), Utils.getCallLocationName());
    }

    public <X> DataSource<X> readFileOfPrimitives(String filePath, String delimiter, Class<X> typeClass) {
        Preconditions.checkNotNull(filePath, "The file path may not be null.");
        return new DataSource(this, new PrimitiveInputFormat(new Path(filePath), delimiter, typeClass), TypeExtractor.getForClass(typeClass), Utils.getCallLocationName());
    }

    public CsvReader readCsvFile(String filePath) {
        return new CsvReader(filePath, this);
    }

    public <X> DataSource<X> readFile(FileInputFormat<X> inputFormat, String filePath) {
        if (inputFormat == null) {
            throw new IllegalArgumentException("InputFormat must not be null.");
        } else if (filePath == null) {
            throw new IllegalArgumentException("The file path must not be null.");
        } else {
            inputFormat.setFilePath(new Path(filePath));

            try {
                return this.createInput(inputFormat, TypeExtractor.getInputFormatTypes(inputFormat));
            } catch (Exception var4) {
                throw new InvalidProgramException("The type returned by the input format could not be automatically determined. Please specify the TypeInformation of the produced type explicitly by using the 'createInput(InputFormat, TypeInformation)' method instead.");
            }
        }
    }

    public <X> DataSource<X> createInput(InputFormat<X, ?> inputFormat) {
        if (inputFormat == null) {
            throw new IllegalArgumentException("InputFormat must not be null.");
        } else {
            try {
                return this.createInput(inputFormat, TypeExtractor.getInputFormatTypes(inputFormat));
            } catch (Exception var3) {
                throw new InvalidProgramException("The type returned by the input format could not be automatically determined. Please specify the TypeInformation of the produced type explicitly by using the 'createInput(InputFormat, TypeInformation)' method instead.", var3);
            }
        }
    }

    public <X> DataSource<X> createInput(InputFormat<X, ?> inputFormat, TypeInformation<X> producedType) {
        if (inputFormat == null) {
            throw new IllegalArgumentException("InputFormat must not be null.");
        } else if (producedType == null) {
            throw new IllegalArgumentException("Produced type information must not be null.");
        } else {
            return new DataSource(this, inputFormat, producedType, Utils.getCallLocationName());
        }
    }

    public <X> DataSource<X> fromCollection(Collection<X> data) {
        if (data == null) {
            throw new IllegalArgumentException("The data must not be null.");
        } else if (data.size() == 0) {
            throw new IllegalArgumentException("The size of the collection must not be empty.");
        } else {
            X firstValue = data.iterator().next();
            TypeInformation<X> type = TypeExtractor.getForObject(firstValue);
            CollectionInputFormat.checkCollection(data, type.getTypeClass());
            return new DataSource(this, new CollectionInputFormat(data, type.createSerializer(this.config)), type, Utils.getCallLocationName());
        }
    }

    public <X> DataSource<X> fromCollection(Collection<X> data, TypeInformation<X> type) {
        return this.fromCollection(data, type, Utils.getCallLocationName());
    }

    private <X> DataSource<X> fromCollection(Collection<X> data, TypeInformation<X> type, String callLocationName) {
        CollectionInputFormat.checkCollection(data, type.getTypeClass());
        return new DataSource(this, new CollectionInputFormat(data, type.createSerializer(this.config)), type, callLocationName);
    }

    public <X> DataSource<X> fromCollection(Iterator<X> data, Class<X> type) {
        return this.fromCollection(data, TypeExtractor.getForClass(type));
    }

    public <X> DataSource<X> fromCollection(Iterator<X> data, TypeInformation<X> type) {
        return new DataSource(this, new IteratorInputFormat(data), type, Utils.getCallLocationName());
    }

    @SafeVarargs
    public final <X> DataSource<X> fromElements(X... data) {
        if (data == null) {
            throw new IllegalArgumentException("The data must not be null.");
        } else if (data.length == 0) {
            throw new IllegalArgumentException("The number of elements must not be zero.");
        } else {
            TypeInformation typeInfo;
            try {
                typeInfo = TypeExtractor.getForObject(data[0]);
            } catch (Exception var4) {
                throw new RuntimeException("Could not create TypeInformation for type " + data[0].getClass().getName() + "; please specify the TypeInformation manually via ExecutionEnvironment#fromElements(Collection, TypeInformation)", var4);
            }

            return this.fromCollection(Arrays.asList(data), typeInfo, Utils.getCallLocationName());
        }
    }

    @SafeVarargs
    public final <X> DataSource<X> fromElements(Class<X> type, X... data) {
        if (data == null) {
            throw new IllegalArgumentException("The data must not be null.");
        } else if (data.length == 0) {
            throw new IllegalArgumentException("The number of elements must not be zero.");
        } else {
            TypeInformation typeInfo;
            try {
                typeInfo = TypeExtractor.getForClass(type);
            } catch (Exception var5) {
                throw new RuntimeException("Could not create TypeInformation for type " + type.getName() + "; please specify the TypeInformation manually via ExecutionEnvironment#fromElements(Collection, TypeInformation)", var5);
            }

            return this.fromCollection(Arrays.asList(data), typeInfo, Utils.getCallLocationName());
        }
    }

    public <X> DataSource<X> fromParallelCollection(SplittableIterator<X> iterator, Class<X> type) {
        return this.fromParallelCollection(iterator, TypeExtractor.getForClass(type));
    }

    public <X> DataSource<X> fromParallelCollection(SplittableIterator<X> iterator, TypeInformation<X> type) {
        return this.fromParallelCollection(iterator, type, Utils.getCallLocationName());
    }

    private <X> DataSource<X> fromParallelCollection(SplittableIterator<X> iterator, TypeInformation<X> type, String callLocationName) {
        return new DataSource(this, new ParallelIteratorInputFormat(iterator), type, callLocationName);
    }

    public DataSource<Long> generateSequence(long from, long to) {
        return this.fromParallelCollection(new NumberSequenceIterator(from, to), BasicTypeInfo.LONG_TYPE_INFO, Utils.getCallLocationName());
    }

    public JobExecutionResult execute() throws Exception {
        return this.execute(getDefaultName());
    }

    public JobExecutionResult execute(String jobName) throws Exception {
        JobClient jobClient = this.executeAsync(jobName);

        try {
            if (this.configuration.getBoolean(DeploymentOptions.ATTACHED)) {
                this.lastJobExecutionResult = (JobExecutionResult)jobClient.getJobExecutionResult(this.userClassloader).get();
            } else {
                this.lastJobExecutionResult = new DetachedJobExecutionResult(jobClient.getJobID());
            }

            this.jobListeners.forEach((jobListener) -> {
                jobListener.onJobExecuted(this.lastJobExecutionResult, (Throwable)null);
            });
        } catch (Throwable var4) {
            this.jobListeners.forEach((jobListener) -> {
                jobListener.onJobExecuted((JobExecutionResult)null, ExceptionUtils.stripExecutionException(var4));
            });
            ExceptionUtils.rethrowException(var4);
        }

        return this.lastJobExecutionResult;
    }

    @PublicEvolving
    public void registerJobListener(JobListener jobListener) {
        Preconditions.checkNotNull(jobListener, "JobListener cannot be null");
        this.jobListeners.add(jobListener);
    }

    @PublicEvolving
    public void clearJobListeners() {
        this.jobListeners.clear();
    }

    @PublicEvolving
    public final JobClient executeAsync() throws Exception {
        return this.executeAsync(getDefaultName());
    }

    @PublicEvolving
    public JobClient executeAsync(String jobName) throws Exception {
        Preconditions.checkNotNull(this.configuration.get(DeploymentOptions.TARGET), "No execution.target specified in your configuration file.");
        Plan plan = this.createProgramPlan(jobName);
        PipelineExecutorFactory executorFactory = this.executorServiceLoader.getExecutorFactory(this.configuration);
        Preconditions.checkNotNull(executorFactory, "Cannot find compatible factory for specified execution.target (=%s)", new Object[]{this.configuration.get(DeploymentOptions.TARGET)});
        CompletableFuture jobClientFuture = executorFactory.getExecutor(this.configuration).execute(plan, this.configuration);

        try {
            JobClient jobClient = (JobClient)jobClientFuture.get();
            this.jobListeners.forEach((jobListener) -> {
                jobListener.onJobSubmitted(jobClient, (Throwable)null);
            });
            return jobClient;
        } catch (Throwable var6) {
            this.jobListeners.forEach((jobListener) -> {
                jobListener.onJobSubmitted((JobClient)null, var6);
            });
            ExceptionUtils.rethrow(var6);
            return null;
        }
    }

    public String getExecutionPlan() throws Exception {
        Plan p = this.createProgramPlan(getDefaultName(), false);
        return ExecutionPlanUtil.getExecutionPlanAsJSON(p);
    }

    public void registerCachedFile(String filePath, String name) {
        this.registerCachedFile(filePath, name, false);
    }

    public void registerCachedFile(String filePath, String name, boolean executable) {
        this.cacheFile.add(new Tuple2(name, new DistributedCacheEntry(filePath, executable)));
    }

    @Internal
    public Plan createProgramPlan() {
        return this.createProgramPlan(getDefaultName());
    }

    @Internal
    public Plan createProgramPlan(String jobName) {
        return this.createProgramPlan(jobName, true);
    }

    @Internal
    public Plan createProgramPlan(String jobName, boolean clearSinks) {
        Preconditions.checkNotNull(jobName);
        if (this.sinks.isEmpty()) {
            if (this.wasExecuted) {
                throw new RuntimeException("No new data sinks have been defined since the last execution. The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'.");
            } else {
                throw new RuntimeException("No data sinks have been created yet. A program needs at least one sink that consumes data. Examples are writing the data set or printing it.");
            }
        } else {
            PlanGenerator generator = new PlanGenerator(this.sinks, this.config, this.getParallelism(), this.cacheFile, jobName);
            Plan plan = generator.generate();
            if (clearSinks) {
                this.sinks.clear();
                this.wasExecuted = true;
            }

            return plan;
        }
    }

    @Internal
    void registerDataSink(DataSink<?> sink) {
        this.sinks.add(sink);
    }

    private static String getDefaultName() {
        return "Flink Java Job at " + Calendar.getInstance().getTime();
    }

    public static ExecutionEnvironment getExecutionEnvironment() {
        return (ExecutionEnvironment)Utils.resolveFactory(threadLocalContextEnvironmentFactory, contextEnvironmentFactory).map(ExecutionEnvironmentFactory::createExecutionEnvironment).orElseGet(ExecutionEnvironment::createLocalEnvironment);
    }

    @PublicEvolving
    public static CollectionEnvironment createCollectionsEnvironment() {
        CollectionEnvironment ce = new CollectionEnvironment();
        ce.setParallelism(1);
        return ce;
    }

    public static LocalEnvironment createLocalEnvironment() {
        return createLocalEnvironment(defaultLocalDop);
    }

    public static LocalEnvironment createLocalEnvironment(int parallelism) {
        return createLocalEnvironment(new Configuration(), parallelism);
    }

    public static LocalEnvironment createLocalEnvironment(Configuration customConfiguration) {
        return createLocalEnvironment(customConfiguration, -1);
    }

    @PublicEvolving
    public static ExecutionEnvironment createLocalEnvironmentWithWebUI(Configuration conf) {
        Preconditions.checkNotNull(conf, "conf");
        if (!conf.contains(RestOptions.PORT)) {
            conf.setInteger(RestOptions.PORT, (Integer)RestOptions.PORT.defaultValue());
        }

        return createLocalEnvironment(conf, -1);
    }

    private static LocalEnvironment createLocalEnvironment(Configuration configuration, int defaultParallelism) {
        LocalEnvironment localEnvironment = new LocalEnvironment(configuration);
        if (defaultParallelism > 0) {
            localEnvironment.setParallelism(defaultParallelism);
        }

        return localEnvironment;
    }

    public static ExecutionEnvironment createRemoteEnvironment(String host, int port, String... jarFiles) {
        return new RemoteEnvironment(host, port, jarFiles);
    }

    public static ExecutionEnvironment createRemoteEnvironment(String host, int port, Configuration clientConfiguration, String... jarFiles) {
        return new RemoteEnvironment(host, port, clientConfiguration, jarFiles, (URL[])null);
    }

    public static ExecutionEnvironment createRemoteEnvironment(String host, int port, int parallelism, String... jarFiles) {
        RemoteEnvironment rec = new RemoteEnvironment(host, port, jarFiles);
        rec.setParallelism(parallelism);
        return rec;
    }

    public static int getDefaultLocalParallelism() {
        return defaultLocalDop;
    }

    public static void setDefaultLocalParallelism(int parallelism) {
        defaultLocalDop = parallelism;
    }

    protected static void initializeContextEnvironment(ExecutionEnvironmentFactory ctx) {
        contextEnvironmentFactory = (ExecutionEnvironmentFactory)Preconditions.checkNotNull(ctx);
        threadLocalContextEnvironmentFactory.set(contextEnvironmentFactory);
    }

    protected static void resetContextEnvironment() {
        contextEnvironmentFactory = null;
        threadLocalContextEnvironmentFactory.remove();
    }

    @Internal
    public static boolean areExplicitEnvironmentsAllowed() {
        return contextEnvironmentFactory == null && threadLocalContextEnvironmentFactory.get() == null;
    }
}
