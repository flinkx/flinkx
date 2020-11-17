//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.flink.yarn;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.ClusterSpecification.ClusterSpecificationBuilder;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.plugin.PluginConfig;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint.ExecutionMode;
import org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmanager.JobManagerProcessSpec;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;
import org.apache.flink.yarn.configuration.YarnConfigOptions.UserJarInclusion;
import org.apache.flink.yarn.entrypoint.YarnApplicationClusterEntryPoint;
import org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint;
import org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnClusterDescriptor implements ClusterDescriptor<ApplicationId> {
	private static final Logger LOG = LoggerFactory.getLogger(YarnClusterDescriptor.class);
	private final YarnConfiguration yarnConfiguration;
	private final YarnClient yarnClient;
	private final YarnClusterInformationRetriever yarnClusterInformationRetriever;
	private final boolean sharedYarnClient;
	private final List<File> shipFiles = new LinkedList();
	private final String yarnQueue;
	private Path flinkJarPath;
	private final Configuration flinkConfiguration;
	private final String customName;
	private final String nodeLabel;
	private final String applicationType;
	private String zookeeperNamespace;
	private UserJarInclusion userJarInclusion;

	public YarnClusterDescriptor(Configuration flinkConfiguration, YarnConfiguration yarnConfiguration, YarnClient yarnClient, YarnClusterInformationRetriever yarnClusterInformationRetriever, boolean sharedYarnClient) {
		this.yarnConfiguration = (YarnConfiguration)Preconditions.checkNotNull(yarnConfiguration);
		this.yarnClient = (YarnClient)Preconditions.checkNotNull(yarnClient);
		this.yarnClusterInformationRetriever = (YarnClusterInformationRetriever)Preconditions.checkNotNull(yarnClusterInformationRetriever);
		this.sharedYarnClient = sharedYarnClient;
		this.flinkConfiguration = (Configuration)Preconditions.checkNotNull(flinkConfiguration);
		this.userJarInclusion = getUserJarInclusionMode(flinkConfiguration);
		this.getLocalFlinkDistPath(flinkConfiguration).ifPresent(this::setLocalJarPath);
		this.decodeDirsToShipToCluster(flinkConfiguration).ifPresent(this::addShipFiles);
		this.yarnQueue = flinkConfiguration.getString(YarnConfigOptions.APPLICATION_QUEUE);
		this.customName = flinkConfiguration.getString(YarnConfigOptions.APPLICATION_NAME);
		this.applicationType = flinkConfiguration.getString(YarnConfigOptions.APPLICATION_TYPE);
		this.nodeLabel = flinkConfiguration.getString(YarnConfigOptions.NODE_LABEL);
		this.zookeeperNamespace = flinkConfiguration.getString(HighAvailabilityOptions.HA_CLUSTER_ID, (String)null);
	}

	private Optional<List<File>> decodeDirsToShipToCluster(Configuration configuration) {
		Preconditions.checkNotNull(configuration);
		List<File> files = ConfigUtils.decodeListFromConfig(configuration, YarnConfigOptions.SHIP_DIRECTORIES, File::new);
		return files.isEmpty() ? Optional.empty() : Optional.of(files);
	}

	private Optional<Path> getLocalFlinkDistPath(Configuration configuration) {
		String localJarPath = configuration.getString(YarnConfigOptions.FLINK_DIST_JAR);
		if (localJarPath != null) {
			return Optional.of(new Path(localJarPath));
		} else {
			LOG.info("No path for the flink jar passed. Using the location of " + this.getClass() + " to locate the jar");
			String decodedPath = this.getDecodedJarPath();
			return decodedPath.endsWith(".jar") ? Optional.of(new Path((new File(decodedPath)).toURI())) : Optional.empty();
		}
	}

	private String getDecodedJarPath() {
		String encodedJarPath = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();

		try {
			return URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
		} catch (UnsupportedEncodingException var3) {
			throw new RuntimeException("Couldn't decode the encoded Flink dist jar path: " + encodedJarPath + " You can supply a path manually via the command line.");
		}
	}

	@VisibleForTesting
	List<File> getShipFiles() {
		return this.shipFiles;
	}

	public YarnClient getYarnClient() {
		return this.yarnClient;
	}

	protected String getYarnSessionClusterEntrypoint() {
		return YarnSessionClusterEntrypoint.class.getName();
	}

	protected String getYarnJobClusterEntrypoint() {
		return YarnJobClusterEntrypoint.class.getName();
	}

	public Configuration getFlinkConfiguration() {
		return this.flinkConfiguration;
	}

	public void setLocalJarPath(Path localJarPath) {
		if (!localJarPath.toString().endsWith("jar")) {
			throw new IllegalArgumentException("The passed jar path ('" + localJarPath + "') does not end with the 'jar' extension");
		} else {
			this.flinkJarPath = localJarPath;
		}
	}

	public void addShipFiles(List<File> shipFiles) {
		Preconditions.checkArgument(this.userJarInclusion != UserJarInclusion.DISABLED || isUsrLibDirIncludedInShipFiles(shipFiles), "This is an illegal ship directory : %s. When setting the %s to %s the name of ship directory can not be %s.", new Object[]{"usrlib", YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR.key(), UserJarInclusion.DISABLED, "usrlib"});
		this.shipFiles.addAll(shipFiles);
	}

	private void isReadyForDeployment(ClusterSpecification clusterSpecification) throws Exception {
		if (this.flinkJarPath == null) {
			throw new YarnClusterDescriptor.YarnDeploymentException("The Flink jar path is null");
		} else if (this.flinkConfiguration == null) {
			throw new YarnClusterDescriptor.YarnDeploymentException("Flink configuration object has not been set");
		} else {
			int numYarnMaxVcores = this.yarnClusterInformationRetriever.getMaxVcores();
			int configuredAmVcores = this.flinkConfiguration.getInteger(YarnConfigOptions.APP_MASTER_VCORES);
			if (configuredAmVcores > numYarnMaxVcores) {
				throw new IllegalConfigurationException(String.format("The number of requested virtual cores for application master %d exceeds the maximum number of virtual cores %d available in the Yarn Cluster.", configuredAmVcores, numYarnMaxVcores));
			} else {
				int configuredVcores = this.flinkConfiguration.getInteger(YarnConfigOptions.VCORES, clusterSpecification.getSlotsPerTaskManager());
				if (configuredVcores > numYarnMaxVcores) {
					throw new IllegalConfigurationException(String.format("The number of requested virtual cores per node %d exceeds the maximum number of virtual cores %d available in the Yarn Cluster. Please note that the number of virtual cores is set to the number of task slots by default unless configured in the Flink config with '%s.'", configuredVcores, numYarnMaxVcores, YarnConfigOptions.VCORES.key()));
				} else {
					if (System.getenv("HADOOP_CONF_DIR") == null && System.getenv("YARN_CONF_DIR") == null) {
						LOG.warn("Neither the HADOOP_CONF_DIR nor the YARN_CONF_DIR environment variable is set. The Flink YARN Client needs one of these to be set to properly load the Hadoop configuration for accessing YARN.");
					}

				}
			}
		}
	}

	public String getZookeeperNamespace() {
		return this.zookeeperNamespace;
	}

	private void setZookeeperNamespace(String zookeeperNamespace) {
		this.zookeeperNamespace = zookeeperNamespace;
	}

	public String getNodeLabel() {
		return this.nodeLabel;
	}

	public void close() {
		if (!this.sharedYarnClient) {
			this.yarnClient.stop();
		}

	}

	public ClusterClientProvider<ApplicationId> retrieve(ApplicationId applicationId) throws ClusterRetrieveException {
		try {
			if (System.getenv("HADOOP_CONF_DIR") == null && System.getenv("YARN_CONF_DIR") == null) {
				LOG.warn("Neither the HADOOP_CONF_DIR nor the YARN_CONF_DIR environment variable is set.The Flink YARN Client needs one of these to be set to properly load the Hadoop configuration for accessing YARN.");
			}

			ApplicationReport report = this.yarnClient.getApplicationReport(applicationId);
			if (report.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
				LOG.error("The application {} doesn't run anymore. It has previously completed with final status: {}", applicationId, report.getFinalApplicationStatus());
				throw new RuntimeException("The Yarn application " + applicationId + " doesn't run anymore.");
			} else {
				this.setClusterEntrypointInfoToConfig(report);
				return () -> {
					try {
						return new RestClusterClient(this.flinkConfiguration, report.getApplicationId());
					} catch (Exception var3) {
						throw new RuntimeException("Couldn't retrieve Yarn cluster", var3);
					}
				};
			}
		} catch (Exception var3) {
			throw new ClusterRetrieveException("Couldn't retrieve Yarn cluster", var3);
		}
	}

	public ClusterClientProvider<ApplicationId> deploySessionCluster(ClusterSpecification clusterSpecification) throws ClusterDeploymentException {
		try {
			return this.deployInternal(clusterSpecification, "Flink session cluster", this.getYarnSessionClusterEntrypoint(), (JobGraph)null, false);
		} catch (Exception var3) {
			throw new ClusterDeploymentException("Couldn't deploy Yarn session cluster", var3);
		}
	}

	public ClusterClientProvider<ApplicationId> deployApplicationCluster(ClusterSpecification clusterSpecification, ApplicationConfiguration applicationConfiguration) throws ClusterDeploymentException {
		Preconditions.checkNotNull(clusterSpecification);
		Preconditions.checkNotNull(applicationConfiguration);
		YarnDeploymentTarget deploymentTarget = YarnDeploymentTarget.fromConfig(this.flinkConfiguration);
		if (YarnDeploymentTarget.APPLICATION != deploymentTarget) {
			throw new ClusterDeploymentException("Couldn't deploy Yarn Application Cluster. Expected deployment.target=" + YarnDeploymentTarget.APPLICATION.getName() + " but actual one was \"" + deploymentTarget.getName() + "\"");
		} else {
			applicationConfiguration.applyToConfiguration(this.flinkConfiguration);
			List<String> pipelineJars = (List)this.flinkConfiguration.getOptional(PipelineOptions.JARS).orElse(Collections.emptyList());
			Preconditions.checkArgument(pipelineJars.size() == 1, "Should only have one jar");

			try {
				return this.deployInternal(clusterSpecification, "Flink Application Cluster", YarnApplicationClusterEntryPoint.class.getName(), (JobGraph)null, false);
			} catch (Exception var6) {
				throw new ClusterDeploymentException("Couldn't deploy Yarn Application Cluster", var6);
			}
		}
	}

	public ClusterClientProvider<ApplicationId> deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached) throws ClusterDeploymentException {
		try {
			return this.deployInternal(clusterSpecification, "Flink per-job cluster", this.getYarnJobClusterEntrypoint(), jobGraph, detached);
		} catch (Exception var5) {
			throw new ClusterDeploymentException("Could not deploy Yarn job cluster.", var5);
		}
	}

	public void killCluster(ApplicationId applicationId) throws FlinkException {
		try {
			this.yarnClient.killApplication(applicationId);
			FileSystem fs = FileSystem.get(this.yarnConfiguration);
			Throwable var3 = null;

			try {
				Path applicationDir = YarnApplicationFileUploader.getApplicationDirPath(fs.getHomeDirectory(), applicationId);
				Utils.deleteApplicationFiles(Collections.singletonMap("_FLINK_YARN_FILES", applicationDir.toUri().toString()));
			} catch (Throwable var13) {
				var3 = var13;
				throw var13;
			} finally {
				if (fs != null) {
					if (var3 != null) {
						try {
							fs.close();
						} catch (Throwable var12) {
							var3.addSuppressed(var12);
						}
					} else {
						fs.close();
					}
				}

			}

		} catch (IOException | YarnException var15) {
			throw new FlinkException("Could not kill the Yarn Flink cluster with id " + applicationId + '.', var15);
		}
	}

	private ClusterClientProvider<ApplicationId> deployInternal(ClusterSpecification clusterSpecification, String applicationName, String yarnClusterEntrypoint, @Nullable JobGraph jobGraph, boolean detached) throws Exception {
		UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
		if (HadoopUtils.isKerberosSecurityEnabled(currentUser)) {
			boolean useTicketCache = this.flinkConfiguration.getBoolean(SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE);
			if (!HadoopUtils.areKerberosCredentialsValid(currentUser, useTicketCache)) {
				throw new RuntimeException("Hadoop security with Kerberos is enabled but the login user does not have Kerberos credentials or delegation tokens!");
			}
		}

		this.isReadyForDeployment(clusterSpecification);
		this.checkYarnQueues(this.yarnClient);
		YarnClientApplication yarnApplication = this.yarnClient.createApplication();
		GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();
		Resource maxRes = appResponse.getMaximumResourceCapability();

		YarnClusterDescriptor.ClusterResourceDescription freeClusterMem;
		try {
			freeClusterMem = this.getCurrentFreeClusterResources(this.yarnClient);
		} catch (IOException | YarnException var17) {
			this.failSessionDuringDeployment(this.yarnClient, yarnApplication);
			throw new YarnClusterDescriptor.YarnDeploymentException("Could not retrieve information about free cluster resources.", var17);
		}

		int yarnMinAllocationMB = this.yarnConfiguration.getInt("yarn.scheduler.minimum-allocation-mb", 0);

		ClusterSpecification validClusterSpecification;
		try {
			validClusterSpecification = this.validateClusterResources(clusterSpecification, yarnMinAllocationMB, maxRes, freeClusterMem);
		} catch (YarnClusterDescriptor.YarnDeploymentException var16) {
			this.failSessionDuringDeployment(this.yarnClient, yarnApplication);
			throw var16;
		}

		LOG.info("Cluster specification: {}", validClusterSpecification);
		ExecutionMode executionMode = detached ? ExecutionMode.DETACHED : ExecutionMode.NORMAL;
		this.flinkConfiguration.setString(ClusterEntrypoint.EXECUTION_MODE, executionMode.toString());
		ApplicationReport report = this.startAppMaster(this.flinkConfiguration, applicationName, yarnClusterEntrypoint, jobGraph, this.yarnClient, yarnApplication, validClusterSpecification);
		if (detached) {
			ApplicationId yarnApplicationId = report.getApplicationId();
			logDetachedClusterInformation(yarnApplicationId, LOG);
		}

		this.setClusterEntrypointInfoToConfig(report);
		return () -> {
			try {
				return new RestClusterClient(this.flinkConfiguration, report.getApplicationId());
			} catch (Exception var3) {
				throw new RuntimeException("Error while creating RestClusterClient.", var3);
			}
		};
	}

	private ClusterSpecification validateClusterResources(ClusterSpecification clusterSpecification, int yarnMinAllocationMB, Resource maximumResourceCapability, YarnClusterDescriptor.ClusterResourceDescription freeClusterResources) throws YarnClusterDescriptor.YarnDeploymentException {
		int jobManagerMemoryMb = clusterSpecification.getMasterMemoryMB();
		int taskManagerMemoryMb = clusterSpecification.getTaskManagerMemoryMB();
		this.logIfComponentMemNotIntegerMultipleOfYarnMinAllocation("JobManager", jobManagerMemoryMb, yarnMinAllocationMB);
		this.logIfComponentMemNotIntegerMultipleOfYarnMinAllocation("TaskManager", taskManagerMemoryMb, yarnMinAllocationMB);
		if (jobManagerMemoryMb < yarnMinAllocationMB) {
			jobManagerMemoryMb = yarnMinAllocationMB;
		}

		String note = "Please check the 'yarn.scheduler.maximum-allocation-mb' and the 'yarn.nodemanager.resource.memory-mb' configuration values\n";
		if (jobManagerMemoryMb > maximumResourceCapability.getMemory()) {
			throw new YarnClusterDescriptor.YarnDeploymentException("The cluster does not have the requested resources for the JobManager available!\nMaximum Memory: " + maximumResourceCapability.getMemory() + "MB Requested: " + jobManagerMemoryMb + "MB. " + "Please check the 'yarn.scheduler.maximum-allocation-mb' and the 'yarn.nodemanager.resource.memory-mb' configuration values\n");
		} else if (taskManagerMemoryMb > maximumResourceCapability.getMemory()) {
			throw new YarnClusterDescriptor.YarnDeploymentException("The cluster does not have the requested resources for the TaskManagers available!\nMaximum Memory: " + maximumResourceCapability.getMemory() + " Requested: " + taskManagerMemoryMb + "MB. " + "Please check the 'yarn.scheduler.maximum-allocation-mb' and the 'yarn.nodemanager.resource.memory-mb' configuration values\n");
		} else {
			String noteRsc = "\nThe Flink YARN client will try to allocate the YARN session, but maybe not all TaskManagers are connecting from the beginning because the resources are currently not available in the cluster. The allocation might take more time than usual because the Flink YARN client needs to wait until the resources become available.";
			if (taskManagerMemoryMb > freeClusterResources.containerLimit) {
				LOG.warn("The requested amount of memory for the TaskManagers (" + taskManagerMemoryMb + "MB) is more than the largest possible YARN container: " + freeClusterResources.containerLimit + "\nThe Flink YARN client will try to allocate the YARN session, but maybe not all TaskManagers are connecting from the beginning because the resources are currently not available in the cluster. The allocation might take more time than usual because the Flink YARN client needs to wait until the resources become available.");
			}

			if (jobManagerMemoryMb > freeClusterResources.containerLimit) {
				LOG.warn("The requested amount of memory for the JobManager (" + jobManagerMemoryMb + "MB) is more than the largest possible YARN container: " + freeClusterResources.containerLimit + "\nThe Flink YARN client will try to allocate the YARN session, but maybe not all TaskManagers are connecting from the beginning because the resources are currently not available in the cluster. The allocation might take more time than usual because the Flink YARN client needs to wait until the resources become available.");
			}

			return (new ClusterSpecificationBuilder()).setMasterMemoryMB(jobManagerMemoryMb).setTaskManagerMemoryMB(taskManagerMemoryMb).setSlotsPerTaskManager(clusterSpecification.getSlotsPerTaskManager()).createClusterSpecification();
		}
	}

	private void logIfComponentMemNotIntegerMultipleOfYarnMinAllocation(String componentName, int componentMemoryMB, int yarnMinAllocationMB) {
		int normalizedMemMB = (componentMemoryMB + (yarnMinAllocationMB - 1)) / yarnMinAllocationMB * yarnMinAllocationMB;
		if (normalizedMemMB <= 0) {
			normalizedMemMB = yarnMinAllocationMB;
		}

		if (componentMemoryMB != normalizedMemMB) {
			LOG.info("The configured {} memory is {} MB. YARN will allocate {} MB to make up an integer multiple of its minimum allocation memory ({} MB, configured via 'yarn.scheduler.minimum-allocation-mb'). The extra {} MB may not be used by Flink.", new Object[]{componentName, componentMemoryMB, normalizedMemMB, yarnMinAllocationMB, normalizedMemMB - componentMemoryMB});
		}

	}

	private void checkYarnQueues(YarnClient yarnClient) {
		try {
			List<QueueInfo> queues = yarnClient.getAllQueues();
			if (queues.size() > 0 && this.yarnQueue != null) {
				boolean queueFound = false;
				Iterator var4 = queues.iterator();

				while(var4.hasNext()) {
					QueueInfo queue = (QueueInfo)var4.next();
					if (queue.getQueueName().equals(this.yarnQueue)) {
						queueFound = true;
						break;
					}
				}

				if (!queueFound) {
					String queueNames = "";

					QueueInfo queue;
					for(Iterator var9 = queues.iterator(); var9.hasNext(); queueNames = queueNames + queue.getQueueName() + ", ") {
						queue = (QueueInfo)var9.next();
					}

					LOG.warn("The specified queue '" + this.yarnQueue + "' does not exist. Available queues: " + queueNames);
				}
			} else {
				LOG.debug("The YARN cluster does not have any queues configured");
			}
		} catch (Throwable var7) {
			LOG.warn("Error while getting queue information from YARN: " + var7.getMessage());
			if (LOG.isDebugEnabled()) {
				LOG.debug("Error details", var7);
			}
		}

	}

	private ApplicationReport startAppMaster(Configuration configuration, String applicationName, String yarnClusterEntrypoint, JobGraph jobGraph, YarnClient yarnClient, YarnClientApplication yarnApplication, ClusterSpecification clusterSpecification) throws Exception {
		org.apache.flink.core.fs.FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));
		FileSystem fs = FileSystem.get(this.yarnConfiguration);
		if (!fs.getClass().getSimpleName().equals("GoogleHadoopFileSystem") && fs.getScheme().startsWith("file")) {
			LOG.warn("The file system scheme is '" + fs.getScheme() + "'. This indicates that the specified Hadoop configuration path is wrong and the system is using the default Hadoop configuration values.The Flink YARN client needs to store its files in a distributed file system");
		}

		ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
		List<Path> providedLibDirs = this.getRemoteSharedPaths(configuration);
		YarnApplicationFileUploader fileUploader = YarnApplicationFileUploader.from(fs, fs.getHomeDirectory(), providedLibDirs, appContext.getApplicationId(), this.getFileReplication());
		Set<File> systemShipFiles = new HashSet(this.shipFiles.size());
		Iterator var13 = this.shipFiles.iterator();

		while(var13.hasNext()) {
			File file = (File)var13.next();
			systemShipFiles.add(file.getAbsoluteFile());
		}

		String logConfigFilePath = configuration.getString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE);
		if (logConfigFilePath != null) {
			systemShipFiles.add(new File(logConfigFilePath));
		}

		ApplicationId appId = appContext.getApplicationId();
		String zkNamespace = this.getZookeeperNamespace();
		if (zkNamespace == null || zkNamespace.isEmpty()) {
			zkNamespace = configuration.getString(HighAvailabilityOptions.HA_CLUSTER_ID, String.valueOf(appId));
			this.setZookeeperNamespace(zkNamespace);
		}

		configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, zkNamespace);
		if (HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)) {
			appContext.setMaxAppAttempts(configuration.getInteger(YarnConfigOptions.APPLICATION_ATTEMPTS.key(), 2));
			this.activateHighAvailabilitySupport(appContext);
		} else {
			appContext.setMaxAppAttempts(configuration.getInteger(YarnConfigOptions.APPLICATION_ATTEMPTS.key(), 1));
		}

		Set<Path> userJarFiles = new HashSet();
		if (jobGraph != null) {
			userJarFiles.addAll((Collection)jobGraph.getUserJars().stream().map((fx) -> {
				return fx.toUri();
			}).map(Path::new).collect(Collectors.toSet()));
		}

		List<URI> jarUrls = ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, URI::create);
		if (jarUrls != null && YarnApplicationClusterEntryPoint.class.getName().equals(yarnClusterEntrypoint)) {
			userJarFiles.addAll((Collection)jarUrls.stream().map(Path::new).collect(Collectors.toSet()));
		}

		if (jobGraph != null) {
			Iterator var18 = jobGraph.getUserArtifacts().entrySet().iterator();

			while(var18.hasNext()) {
				Entry<String, DistributedCacheEntry> entry = (Entry)var18.next();
				if (!Utils.isRemotePath(((DistributedCacheEntry)entry.getValue()).filePath)) {
					Path localPath = new Path(((DistributedCacheEntry)entry.getValue()).filePath);
					Tuple2<Path, Long> remoteFileInfo = fileUploader.uploadLocalFileToRemote(localPath, (String)entry.getKey());
					jobGraph.setUserArtifactRemotePath((String)entry.getKey(), ((Path)remoteFileInfo.f0).toString());
				}
			}

			jobGraph.writeUserArtifactEntriesToConfiguration();
		}

		if (providedLibDirs == null || providedLibDirs.isEmpty()) {
			this.addLibFoldersToShipFiles(systemShipFiles);
		}

		List<String> systemClassPaths = fileUploader.registerProvidedLocalResources();
		List<String> uploadedDependencies = fileUploader.registerMultipleLocalResources((Collection)systemShipFiles.stream().map((e) -> {
			return new Path(e.toURI());
		}).collect(Collectors.toSet()), ".");
		systemClassPaths.addAll(uploadedDependencies);
		if (providedLibDirs == null || providedLibDirs.isEmpty()) {
			Set<File> shipOnlyFiles = new HashSet();
			this.addPluginsFoldersToShipFiles(shipOnlyFiles);
			fileUploader.registerMultipleLocalResources((Collection)shipOnlyFiles.stream().map((e) -> {
				return new Path(e.toURI());
			}).collect(Collectors.toSet()), ".");
		}

		List<String> userClassPaths = fileUploader.registerMultipleLocalResources(userJarFiles, this.userJarInclusion == UserJarInclusion.DISABLED ? "usrlib" : ".");
		if (this.userJarInclusion == UserJarInclusion.ORDER) {
			systemClassPaths.addAll(userClassPaths);
		}

		Collections.sort(systemClassPaths);
		Collections.sort(userClassPaths);
		StringBuilder classPathBuilder = new StringBuilder();
		Iterator var22;
		String classPath;
		if (this.userJarInclusion == UserJarInclusion.FIRST) {
			var22 = userClassPaths.iterator();

			while(var22.hasNext()) {
				classPath = (String)var22.next();
				classPathBuilder.append(classPath).append(File.pathSeparator);
			}
		}

		var22 = systemClassPaths.iterator();

		while(var22.hasNext()) {
			classPath = (String)var22.next();
			classPathBuilder.append(classPath).append(File.pathSeparator);
		}

		YarnLocalResourceDescriptor localResourceDescFlinkJar = fileUploader.uploadFlinkDist(this.flinkJarPath);
		classPathBuilder.append(localResourceDescFlinkJar.getResourceKey()).append(File.pathSeparator);
		File tmpJobGraphFile;
		String jobGraphFilename;
		if (jobGraph != null) {
			tmpJobGraphFile = null;

			try {
				tmpJobGraphFile = File.createTempFile(appId.toString(), (String)null);
				FileOutputStream output = new FileOutputStream(tmpJobGraphFile);
				Throwable var25 = null;

				try {
					ObjectOutputStream obOutput = new ObjectOutputStream(output);
					Throwable var27 = null;

					try {
						obOutput.writeObject(jobGraph);
					} catch (Throwable var92) {
						var27 = var92;
						throw var92;
					} finally {
						if (obOutput != null) {
							if (var27 != null) {
								try {
									obOutput.close();
								} catch (Throwable var90) {
									var27.addSuppressed(var90);
								}
							} else {
								obOutput.close();
							}
						}

					}
				} catch (Throwable var95) {
					var25 = var95;
					throw var95;
				} finally {
					if (output != null) {
						if (var25 != null) {
							try {
								output.close();
							} catch (Throwable var89) {
								var25.addSuppressed(var89);
							}
						} else {
							output.close();
						}
					}

				}

				jobGraphFilename = "job.graph";
				configuration.setString(FileJobGraphRetriever.JOB_GRAPH_FILE_PATH, "job.graph");
				fileUploader.registerSingleLocalResource("job.graph", new Path(tmpJobGraphFile.toURI()), "", true, false);
				classPathBuilder.append("job.graph").append(File.pathSeparator);
			} catch (Exception var97) {
				LOG.warn("Add job graph to local resource fail.");
				throw var97;
			} finally {
				if (tmpJobGraphFile != null && !tmpJobGraphFile.delete()) {
					LOG.warn("Fail to delete temporary file {}.", tmpJobGraphFile.toPath());
				}

			}
		}

		tmpJobGraphFile = null;

		try {
			tmpJobGraphFile = File.createTempFile(appId + "-flink-conf.yaml", (String)null);
			BootstrapTools.writeConfiguration(configuration, tmpJobGraphFile);
			jobGraphFilename = "flink-conf.yaml";
			fileUploader.registerSingleLocalResource(jobGraphFilename, new Path(tmpJobGraphFile.getAbsolutePath()), "", true, true);
			classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);
		} finally {
			if (tmpJobGraphFile != null && !tmpJobGraphFile.delete()) {
				LOG.warn("Fail to delete temporary file {}.", tmpJobGraphFile.toPath());
			}

		}

		if (this.userJarInclusion == UserJarInclusion.LAST) {
			Iterator var109 = userClassPaths.iterator();

			while(var109.hasNext()) {
				String userClassPath = (String)var109.next();
				classPathBuilder.append(userClassPath).append(File.pathSeparator);
			}
		}

		Path remoteKrb5Path = null;
		Path remoteYarnSiteXmlPath = null;
		boolean hasKrb5 = false;
		String keytab;
		if (System.getenv("IN_TESTS") != null) {
			File f = new File(System.getenv("YARN_CONF_DIR"), "yarn-site.xml");
			LOG.info("Adding Yarn configuration {} to the AM container local resource bucket", f.getAbsolutePath());
			Path yarnSitePath = new Path(f.getAbsolutePath());
			remoteYarnSiteXmlPath = fileUploader.registerSingleLocalResource("yarn-site.xml", yarnSitePath, "", false, false).getPath();
			keytab = System.getProperty("java.security.krb5.conf");
			if (keytab != null && keytab.length() != 0) {
				File krb5 = new File(keytab);
				LOG.info("Adding KRB5 configuration {} to the AM container local resource bucket", krb5.getAbsolutePath());
				Path krb5ConfPath = new Path(krb5.getAbsolutePath());
				remoteKrb5Path = fileUploader.registerSingleLocalResource("krb5.conf", krb5ConfPath, "", false, false).getPath();
				hasKrb5 = true;
			}
		}

		Path remotePathKeytab = null;
		String localizedKeytabPath = null;
		keytab = configuration.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB);
		if (keytab != null) {
			boolean localizeKeytab = this.flinkConfiguration.getBoolean(YarnConfigOptions.SHIP_LOCAL_KEYTAB);
			localizedKeytabPath = this.flinkConfiguration.getString(YarnConfigOptions.LOCALIZED_KEYTAB_PATH);
			if (localizeKeytab) {
				LOG.info("Adding keytab {} to the AM container local resource bucket", keytab);
				remotePathKeytab = fileUploader.registerSingleLocalResource(localizedKeytabPath, new Path(keytab), "", false, false).getPath();
			} else {
				localizedKeytabPath = this.flinkConfiguration.getString(YarnConfigOptions.LOCALIZED_KEYTAB_PATH);
			}
		}

		JobManagerProcessSpec processSpec = JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(this.flinkConfiguration, JobManagerOptions.TOTAL_PROCESS_MEMORY);
		ContainerLaunchContext amContainer = this.setupApplicationMasterContainer(yarnClusterEntrypoint, hasKrb5, processSpec);
		if (UserGroupInformation.isSecurityEnabled()) {
			LOG.info("Adding delegation token to the AM container.");
			Utils.setTokensFor(amContainer, fileUploader.getRemotePaths(), this.yarnConfiguration);
		}

		amContainer.setLocalResources(fileUploader.getRegisteredLocalResources());
		fileUploader.close();
		Map<String, String> appMasterEnv = new HashMap();
		appMasterEnv.putAll(ConfigurationUtils.getPrefixedKeyValuePairs("containerized.master.env.", configuration));
		appMasterEnv.put("_FLINK_CLASSPATH", classPathBuilder.toString());
		appMasterEnv.put("_FLINK_DIST_JAR", localResourceDescFlinkJar.toString());
		appMasterEnv.put("_APP_ID", appId.toString());
		appMasterEnv.put("_CLIENT_HOME_DIR", fileUploader.getHomeDir().toString());
		appMasterEnv.put("_CLIENT_SHIP_FILES", encodeYarnLocalResourceDescriptorListToString(fileUploader.getEnvShipResourceList()));
		appMasterEnv.put("_ZOOKEEPER_NAMESPACE", this.getZookeeperNamespace());
		appMasterEnv.put("_FLINK_YARN_FILES", fileUploader.getApplicationDir().toUri().toString());
		appMasterEnv.put("HADOOP_USER_NAME", UserGroupInformation.getCurrentUser().getUserName());
		if (localizedKeytabPath != null) {
			appMasterEnv.put("_LOCAL_KEYTAB_PATH", localizedKeytabPath);
			String principal = configuration.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL);
			appMasterEnv.put("_KEYTAB_PRINCIPAL", principal);
			if (remotePathKeytab != null) {
				appMasterEnv.put("_REMOTE_KEYTAB_PATH", remotePathKeytab.toString());
			}
		}

		if (remoteYarnSiteXmlPath != null) {
			appMasterEnv.put("_YARN_SITE_XML_PATH", remoteYarnSiteXmlPath.toString());
		}

		if (remoteKrb5Path != null) {
			appMasterEnv.put("_KRB5_PATH", remoteKrb5Path.toString());
		}

		Utils.setupYarnClassPath(this.yarnConfiguration, appMasterEnv);
		amContainer.setEnvironment(appMasterEnv);
		Resource capability = (Resource)Records.newRecord(Resource.class);
		capability.setMemory(clusterSpecification.getMasterMemoryMB());
		capability.setVirtualCores(this.flinkConfiguration.getInteger(YarnConfigOptions.APP_MASTER_VCORES));
		String customApplicationName = this.customName != null ? this.customName : applicationName;
		appContext.setApplicationName(customApplicationName);
		appContext.setApplicationType(this.applicationType != null ? this.applicationType : "Apache Flink");
		appContext.setAMContainerSpec(amContainer);
		appContext.setResource(capability);
		int priorityNum = this.flinkConfiguration.getInteger(YarnConfigOptions.APPLICATION_PRIORITY);
		if (priorityNum >= 0) {
			Priority priority = Priority.newInstance(priorityNum);
			appContext.setPriority(priority);
		}

		if (this.yarnQueue != null) {
			appContext.setQueue(this.yarnQueue);
		}

		this.setApplicationNodeLabel(appContext);
		this.setApplicationTags(appContext);
		Thread deploymentFailureHook = new YarnClusterDescriptor.DeploymentFailureHook(yarnApplication, fileUploader.getApplicationDir());
		Runtime.getRuntime().addShutdownHook(deploymentFailureHook);
		LOG.info("Submitting application master " + appId);
		yarnClient.submitApplication(appContext);
		LOG.info("Waiting for the cluster to be allocated");
		long startTime = System.currentTimeMillis();
		YarnApplicationState lastAppState = YarnApplicationState.NEW;

		while(true) {
			ApplicationReport report;
			try {
				report = yarnClient.getApplicationReport(appId);
			} catch (IOException var91) {
				throw new YarnClusterDescriptor.YarnDeploymentException("Failed to deploy the cluster.", var91);
			}

			YarnApplicationState appState = report.getYarnApplicationState();
			LOG.debug("Application State: {}", appState);
			switch(appState) {
				case FAILED:
				case KILLED:
					throw new YarnClusterDescriptor.YarnDeploymentException("The YARN application unexpectedly switched to state " + appState + " during deployment. \nDiagnostics from YARN: " + report.getDiagnostics() + "\nIf log aggregation is enabled on your cluster, use this command to further investigate the issue:\nyarn logs -applicationId " + appId);
				case RUNNING:
					LOG.info("YARN application has been deployed successfully.");
					break;
				case FINISHED:
					LOG.info("YARN application has been finished successfully.");
					break;
				default:
					if (appState != lastAppState) {
						LOG.info("Deploying cluster, current state " + appState);
					}

					if (System.currentTimeMillis() - startTime > 60000L) {
						LOG.info("Deployment took more than 60 seconds. Please check if the requested resources are available in the YARN cluster");
					}

					lastAppState = appState;
					Thread.sleep(250L);
					continue;
			}

			ShutdownHookUtil.removeShutdownHook(deploymentFailureHook, this.getClass().getSimpleName(), LOG);
			return report;
		}
	}

	private int getFileReplication() {
		int yarnFileReplication = this.yarnConfiguration.getInt("dfs.replication", 3);
		int fileReplication = this.flinkConfiguration.getInteger(YarnConfigOptions.FILE_REPLICATION);
		return fileReplication > 0 ? fileReplication : yarnFileReplication;
	}

	private List<Path> getRemoteSharedPaths(Configuration configuration) throws IOException, FlinkException {
		List<Path> providedLibDirs = ConfigUtils.decodeListFromConfig(configuration, YarnConfigOptions.PROVIDED_LIB_DIRS, Path::new);
		Iterator var3 = providedLibDirs.iterator();

		Path path;
		do {
			if (!var3.hasNext()) {
				return providedLibDirs;
			}

			path = (Path)var3.next();
		} while(Utils.isRemotePath(path.toString()));

		throw new FlinkException("The \"" + YarnConfigOptions.PROVIDED_LIB_DIRS.key() + "\" should only contain dirs accessible from all worker nodes, while the \"" + path + "\" is local.");
	}

	private static String encodeYarnLocalResourceDescriptorListToString(List<YarnLocalResourceDescriptor> resources) {
		return String.join(";", (Iterable)resources.stream().map(YarnLocalResourceDescriptor::toString).collect(Collectors.toList()));
	}

	private void failSessionDuringDeployment(YarnClient yarnClient, YarnClientApplication yarnApplication) {
		LOG.info("Killing YARN application");

		try {
			yarnClient.killApplication(yarnApplication.getNewApplicationResponse().getApplicationId());
		} catch (Exception var4) {
			LOG.debug("Error while killing YARN application", var4);
		}

	}

	private YarnClusterDescriptor.ClusterResourceDescription getCurrentFreeClusterResources(YarnClient yarnClient) throws YarnException, IOException {
		List<NodeReport> nodes = yarnClient.getNodeReports(new NodeState[]{NodeState.RUNNING});
		int totalFreeMemory = 0;
		int containerLimit = 0;
		int[] nodeManagersFree = new int[nodes.size()];

		for(int i = 0; i < nodes.size(); ++i) {
			NodeReport rep = (NodeReport)nodes.get(i);
			int free = rep.getCapability().getMemory() - (rep.getUsed() != null ? rep.getUsed().getMemory() : 0);
			nodeManagersFree[i] = free;
			totalFreeMemory += free;
			if (free > containerLimit) {
				containerLimit = free;
			}
		}

		return new YarnClusterDescriptor.ClusterResourceDescription(totalFreeMemory, containerLimit, nodeManagersFree);
	}

	public String getClusterDescription() {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(baos);
			YarnClusterMetrics metrics = this.yarnClient.getYarnClusterMetrics();
			ps.append("NodeManagers in the ClusterClient " + metrics.getNumNodeManagers());
			List<NodeReport> nodes = this.yarnClient.getNodeReports(new NodeState[]{NodeState.RUNNING});
			String format = "|%-16s |%-16s %n";
			ps.printf("|Property         |Value          %n");
			ps.println("+---------------------------------------+");
			int totalMemory = 0;
			int totalCores = 0;
			Iterator var8 = nodes.iterator();

			while(var8.hasNext()) {
				NodeReport rep = (NodeReport)var8.next();
				Resource res = rep.getCapability();
				totalMemory += res.getMemory();
				totalCores += res.getVirtualCores();
				ps.format("|%-16s |%-16s %n", "NodeID", rep.getNodeId());
				ps.format("|%-16s |%-16s %n", "Memory", res.getMemory() + " MB");
				ps.format("|%-16s |%-16s %n", "vCores", res.getVirtualCores());
				ps.format("|%-16s |%-16s %n", "HealthReport", rep.getHealthReport());
				ps.format("|%-16s |%-16s %n", "Containers", rep.getNumContainers());
				ps.println("+---------------------------------------+");
			}

			ps.println("Summary: totalMemory " + totalMemory + " totalCores " + totalCores);
			List<QueueInfo> qInfo = this.yarnClient.getAllQueues();
			Iterator var13 = qInfo.iterator();

			while(var13.hasNext()) {
				QueueInfo q = (QueueInfo)var13.next();
				ps.println("Queue: " + q.getQueueName() + ", Current Capacity: " + q.getCurrentCapacity() + " Max Capacity: " + q.getMaximumCapacity() + " Applications: " + q.getApplications().size());
			}

			return baos.toString();
		} catch (Exception var11) {
			throw new RuntimeException("Couldn't get cluster description", var11);
		}
	}

	private void activateHighAvailabilitySupport(ApplicationSubmissionContext appContext) throws InvocationTargetException, IllegalAccessException {
		YarnClusterDescriptor.ApplicationSubmissionContextReflector reflector = YarnClusterDescriptor.ApplicationSubmissionContextReflector.getInstance();
		reflector.setKeepContainersAcrossApplicationAttempts(appContext, true);
		reflector.setAttemptFailuresValidityInterval(appContext, this.flinkConfiguration.getLong(YarnConfigOptions.APPLICATION_ATTEMPT_FAILURE_VALIDITY_INTERVAL));
	}

	private void setApplicationTags(ApplicationSubmissionContext appContext) throws InvocationTargetException, IllegalAccessException {
		YarnClusterDescriptor.ApplicationSubmissionContextReflector reflector = YarnClusterDescriptor.ApplicationSubmissionContextReflector.getInstance();
		String tagsString = this.flinkConfiguration.getString(YarnConfigOptions.APPLICATION_TAGS);
		Set<String> applicationTags = new HashSet();
		String[] var5 = tagsString.split(",");
		int var6 = var5.length;

		for(int var7 = 0; var7 < var6; ++var7) {
			String tag = var5[var7];
			String trimmedTag = tag.trim();
			if (!trimmedTag.isEmpty()) {
				applicationTags.add(trimmedTag);
			}
		}

		reflector.setApplicationTags(appContext, applicationTags);
	}

	private void setApplicationNodeLabel(ApplicationSubmissionContext appContext) throws InvocationTargetException, IllegalAccessException {
		if (this.nodeLabel != null) {
			YarnClusterDescriptor.ApplicationSubmissionContextReflector reflector = YarnClusterDescriptor.ApplicationSubmissionContextReflector.getInstance();
			reflector.setApplicationNodeLabel(appContext, this.nodeLabel);
		}

	}

	@VisibleForTesting
	void addLibFoldersToShipFiles(Collection<File> effectiveShipFiles) {
		String libDir = (String)System.getenv().get("FLINK_LIB_DIR");
		if (libDir != null) {
			File directoryFile = new File(libDir);
			if (!directoryFile.isDirectory()) {
				throw new YarnClusterDescriptor.YarnDeploymentException("The environment variable 'FLINK_LIB_DIR' is set to '" + libDir + "' but the directory doesn't exist.");
			}

			effectiveShipFiles.add(directoryFile);
		} else if (this.shipFiles.isEmpty()) {
			LOG.warn("Environment variable '{}' not set and ship files have not been provided manually. Not shipping any library files.", "FLINK_LIB_DIR");
		}

	}

	@VisibleForTesting
	void addPluginsFoldersToShipFiles(Collection<File> effectiveShipFiles) {
		Optional<File> pluginsDir = PluginConfig.getPluginsDir();
		pluginsDir.ifPresent(effectiveShipFiles::add);
	}

	ContainerLaunchContext setupApplicationMasterContainer(String yarnClusterEntrypoint, boolean hasKrb5, JobManagerProcessSpec processSpec) {
		String javaOpts = this.flinkConfiguration.getString(CoreOptions.FLINK_JVM_OPTIONS);
		if (this.flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS).length() > 0) {
			javaOpts = javaOpts + " " + this.flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS);
		}

		if (hasKrb5) {
			javaOpts = javaOpts + " -Djava.security.krb5.conf=krb5.conf";
		}

		ContainerLaunchContext amContainer = (ContainerLaunchContext)Records.newRecord(ContainerLaunchContext.class);
		Map<String, String> startCommandValues = new HashMap();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");
		String jvmHeapMem = JobManagerProcessUtils.generateJvmParametersStr(processSpec, this.flinkConfiguration);
		startCommandValues.put("jvmmem", jvmHeapMem);
		startCommandValues.put("jvmopts", javaOpts);
		startCommandValues.put("logging", YarnLogConfigUtil.getLoggingYarnCommand(this.flinkConfiguration));
		startCommandValues.put("class", yarnClusterEntrypoint);
		startCommandValues.put("redirects", "1> <LOG_DIR>/jobmanager.out 2> <LOG_DIR>/jobmanager.err");
		startCommandValues.put("args", "");
		String commandTemplate = this.flinkConfiguration.getString("yarn.container-start-command-template", "%java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%");
		String amCommand = BootstrapTools.getStartCommand(commandTemplate, startCommandValues);
		amContainer.setCommands(Collections.singletonList(amCommand));
		LOG.debug("Application Master start command: " + amCommand);
		return amContainer;
	}

	private static UserJarInclusion getUserJarInclusionMode(Configuration config) {
		return (UserJarInclusion)config.getEnum(UserJarInclusion.class, YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR);
	}

	private static boolean isUsrLibDirIncludedInShipFiles(List<File> shipFiles) {
		return shipFiles.stream().filter(File::isDirectory).map(File::getName).noneMatch((name) -> {
			return name.equals("usrlib");
		});
	}

	private void setClusterEntrypointInfoToConfig(ApplicationReport report) {
		Preconditions.checkNotNull(report);
		ApplicationId clusterId = report.getApplicationId();
		String host = report.getHost();
		int port = report.getRpcPort();
		LOG.info("Found Web Interface {}:{} of application '{}'.", new Object[]{host, port, clusterId});
		this.flinkConfiguration.setString(JobManagerOptions.ADDRESS, host);
		this.flinkConfiguration.setInteger(JobManagerOptions.PORT, port);
		this.flinkConfiguration.setString(RestOptions.ADDRESS, host);
		this.flinkConfiguration.setInteger(RestOptions.PORT, port);
		this.flinkConfiguration.set(YarnConfigOptions.APPLICATION_ID, ConverterUtils.toString(clusterId));
	}

	public static void logDetachedClusterInformation(ApplicationId yarnApplicationId, Logger logger) {
		logger.info("The Flink YARN session cluster has been started in detached mode. In order to stop Flink gracefully, use the following command:\n$ echo \"stop\" | ./bin/yarn-session.sh -id {}\nIf this should not be possible, then you can also kill Flink via YARN's web interface or via:\n$ yarn application -kill {}\nNote that killing Flink might not clean up all job artifacts and temporary files.", yarnApplicationId, yarnApplicationId);
	}

	private class DeploymentFailureHook extends Thread {
		private final YarnClient yarnClient;
		private final YarnClientApplication yarnApplication;
		private final Path yarnFilesDir;

		DeploymentFailureHook(YarnClientApplication yarnApplication, Path yarnFilesDir) {
			this.yarnApplication = (YarnClientApplication)Preconditions.checkNotNull(yarnApplication);
			this.yarnFilesDir = (Path)Preconditions.checkNotNull(yarnFilesDir);
			this.yarnClient = YarnClient.createYarnClient();
			this.yarnClient.init(YarnClusterDescriptor.this.yarnConfiguration);
		}

		public void run() {
			YarnClusterDescriptor.LOG.info("Cancelling deployment from Deployment Failure Hook");
			this.yarnClient.start();
			YarnClusterDescriptor.this.failSessionDuringDeployment(this.yarnClient, this.yarnApplication);
			this.yarnClient.stop();
			YarnClusterDescriptor.LOG.info("Deleting files in {}.", this.yarnFilesDir);

			try {
				FileSystem fs = FileSystem.get(YarnClusterDescriptor.this.yarnConfiguration);
				if (!fs.delete(this.yarnFilesDir, true)) {
					throw new IOException("Deleting files in " + this.yarnFilesDir + " was unsuccessful");
				}

				fs.close();
			} catch (IOException var2) {
				YarnClusterDescriptor.LOG.error("Failed to delete Flink Jar and configuration files in HDFS", var2);
			}

		}
	}

	private static class YarnDeploymentException extends RuntimeException {
		private static final long serialVersionUID = -812040641215388943L;

		public YarnDeploymentException(String message) {
			super(message);
		}

		public YarnDeploymentException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	private static class ApplicationSubmissionContextReflector {
		private static final Logger LOG = LoggerFactory.getLogger(YarnClusterDescriptor.ApplicationSubmissionContextReflector.class);
		private static final YarnClusterDescriptor.ApplicationSubmissionContextReflector instance = new YarnClusterDescriptor.ApplicationSubmissionContextReflector(ApplicationSubmissionContext.class);
		private static final String APPLICATION_TAGS_METHOD_NAME = "setApplicationTags";
		private static final String ATTEMPT_FAILURES_METHOD_NAME = "setAttemptFailuresValidityInterval";
		private static final String KEEP_CONTAINERS_METHOD_NAME = "setKeepContainersAcrossApplicationAttempts";
		private static final String NODE_LABEL_EXPRESSION_NAME = "setNodeLabelExpression";
		private final Method applicationTagsMethod;
		private final Method attemptFailuresValidityIntervalMethod;
		private final Method keepContainersMethod;
		@Nullable
		private final Method nodeLabelExpressionMethod;

		public static YarnClusterDescriptor.ApplicationSubmissionContextReflector getInstance() {
			return instance;
		}

		private ApplicationSubmissionContextReflector(Class<ApplicationSubmissionContext> clazz) {
			Method applicationTagsMethod;
			try {
				applicationTagsMethod = clazz.getMethod("setApplicationTags", Set.class);
				LOG.debug("{} supports method {}.", clazz.getCanonicalName(), "setApplicationTags");
			} catch (NoSuchMethodException var10) {
				LOG.debug("{} does not support method {}.", clazz.getCanonicalName(), "setApplicationTags");
				applicationTagsMethod = null;
			}

			this.applicationTagsMethod = applicationTagsMethod;

			Method attemptFailuresValidityIntervalMethod;
			try {
				attemptFailuresValidityIntervalMethod = clazz.getMethod("setAttemptFailuresValidityInterval", Long.TYPE);
				LOG.debug("{} supports method {}.", clazz.getCanonicalName(), "setAttemptFailuresValidityInterval");
			} catch (NoSuchMethodException var9) {
				LOG.debug("{} does not support method {}.", clazz.getCanonicalName(), "setAttemptFailuresValidityInterval");
				attemptFailuresValidityIntervalMethod = null;
			}

			this.attemptFailuresValidityIntervalMethod = attemptFailuresValidityIntervalMethod;

			Method keepContainersMethod;
			try {
				keepContainersMethod = clazz.getMethod("setKeepContainersAcrossApplicationAttempts", Boolean.TYPE);
				LOG.debug("{} supports method {}.", clazz.getCanonicalName(), "setKeepContainersAcrossApplicationAttempts");
			} catch (NoSuchMethodException var8) {
				LOG.debug("{} does not support method {}.", clazz.getCanonicalName(), "setKeepContainersAcrossApplicationAttempts");
				keepContainersMethod = null;
			}

			this.keepContainersMethod = keepContainersMethod;

			Method nodeLabelExpressionMethod;
			try {
				nodeLabelExpressionMethod = clazz.getMethod("setNodeLabelExpression", String.class);
				LOG.debug("{} supports method {}.", clazz.getCanonicalName(), "setNodeLabelExpression");
			} catch (NoSuchMethodException var7) {
				LOG.debug("{} does not support method {}.", clazz.getCanonicalName(), "setNodeLabelExpression");
				nodeLabelExpressionMethod = null;
			}

			this.nodeLabelExpressionMethod = nodeLabelExpressionMethod;
		}

		public void setApplicationTags(ApplicationSubmissionContext appContext, Set<String> applicationTags) throws InvocationTargetException, IllegalAccessException {
			if (this.applicationTagsMethod != null) {
				LOG.debug("Calling method {} of {}.", this.applicationTagsMethod.getName(), appContext.getClass().getCanonicalName());
				this.applicationTagsMethod.invoke(appContext, applicationTags);
			} else {
				LOG.debug("{} does not support method {}. Doing nothing.", appContext.getClass().getCanonicalName(), "setApplicationTags");
			}

		}

		public void setApplicationNodeLabel(ApplicationSubmissionContext appContext, String nodeLabel) throws InvocationTargetException, IllegalAccessException {
			if (this.nodeLabelExpressionMethod != null) {
				LOG.debug("Calling method {} of {}.", this.nodeLabelExpressionMethod.getName(), appContext.getClass().getCanonicalName());
				this.nodeLabelExpressionMethod.invoke(appContext, nodeLabel);
			} else {
				LOG.debug("{} does not support method {}. Doing nothing.", appContext.getClass().getCanonicalName(), "setNodeLabelExpression");
			}

		}

		public void setAttemptFailuresValidityInterval(ApplicationSubmissionContext appContext, long validityInterval) throws InvocationTargetException, IllegalAccessException {
			if (this.attemptFailuresValidityIntervalMethod != null) {
				LOG.debug("Calling method {} of {}.", this.attemptFailuresValidityIntervalMethod.getName(), appContext.getClass().getCanonicalName());
				this.attemptFailuresValidityIntervalMethod.invoke(appContext, validityInterval);
			} else {
				LOG.debug("{} does not support method {}. Doing nothing.", appContext.getClass().getCanonicalName(), "setAttemptFailuresValidityInterval");
			}

		}

		public void setKeepContainersAcrossApplicationAttempts(ApplicationSubmissionContext appContext, boolean keepContainers) throws InvocationTargetException, IllegalAccessException {
			if (this.keepContainersMethod != null) {
				LOG.debug("Calling method {} of {}.", this.keepContainersMethod.getName(), appContext.getClass().getCanonicalName());
				this.keepContainersMethod.invoke(appContext, keepContainers);
			} else {
				LOG.debug("{} does not support method {}. Doing nothing.", appContext.getClass().getCanonicalName(), "setKeepContainersAcrossApplicationAttempts");
			}

		}
	}

	private static class ClusterResourceDescription {
		public final int totalFreeMemory;
		public final int containerLimit;
		public final int[] nodeManagersFree;

		public ClusterResourceDescription(int totalFreeMemory, int containerLimit, int[] nodeManagersFree) {
			this.totalFreeMemory = totalFreeMemory;
			this.containerLimit = containerLimit;
			this.nodeManagersFree = nodeManagersFree;
		}
	}
}
