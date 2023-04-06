package org.hadi;

public class client {
    public static void main(String[] args) throws Exception {
        // initialize and start yarn client
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // create app and app_id
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        //set the application submission context
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName(appName);

        //set local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");

        //Create a local resource to point to the destination jar path
        FileSystem fs = FileSystem.get(conf);
        addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(),
                localResources, null);

        // Set the log4j properties if needed
        if (!log4jPropFile.isEmpty()) {
            addToLocalResources(fs, log4jPropFile, log4jPath, appId.toString(),
                    localResources, null);
        }

        //put shell_script
        String hdfsShellScriptLocation = "";
        long hdfsShellScriptLen = 0;
        long hdfsShellScriptTimestamp = 0;
        if (!shellScriptPath.isEmpty()) {
            Path shellSrc = new Path(shellScriptPath);
            String shellPathSuffix =
                    appName + "/" + appId.toString() + "/" + SCRIPT_PATH;
            Path shellDst =
                    new Path(fs.getHomeDirectory(), shellPathSuffix);
            fs.copyFromLocalFile(false, true, shellSrc, shellDst);
            hdfsShellScriptLocation = shellDst.toUri().toString();
            FileStatus shellFileStatus = fs.getFileStatus(shellDst);
            hdfsShellScriptLen = shellFileStatus.getLen();
            hdfsShellScriptTimestamp = shellFileStatus.getModificationTime();
        }

        if (!shellCommand.isEmpty()) {
            addToLocalResources(fs, null, shellCommandPath, appId.toString(),
                    localResources, shellCommand);
        }

        if (shellArgs.length > 0) {
            addToLocalResources(fs, null, shellArgsPath, appId.toString(),
                    localResources, StringUtils.join(shellArgs, " "));
        }

        //set the env-variable
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();

        //put shell_script
        env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION, hdfsShellScriptLocation);
        env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP, Long.toString(hdfsShellScriptTimestamp));
        env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN, Long.toString(hdfsShellScriptLen));

        //classpath appmaster.jar
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(
                "./log4j.properties");

        //set command
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);
        // Set java executable command
        LOG.info("Setting up app master command");
        vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
        // Set Xmx based on am memory size
        vargs.add("-Xmx" + amMemory + "m");
        // Set class name
        vargs.add(appMasterMainClass);
        // Set params for Application Master
        vargs.add("--container_memory " + String.valueOf(containerMemory));
        vargs.add("--container_vcores " + String.valueOf(containerVirtualCores));
        vargs.add("--num_containers " + String.valueOf(numContainers));
        vargs.add("--priority " + String.valueOf(shellCmdPriority));

        for (Map.Entry<String, String> entry : shellEnv.entrySet()) {
            vargs.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
        }
        if (debugFlag) {
            vargs.add("--debug");
        }

        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        // Get final command
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());

        //Set up the container launch context for the application master
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        //Set up resource type requirements
        Resource capability = Resource.newInstance(amMemory, amVCores);
        appContext.setResource(capability);

        //Setup security tokens
        if (UserGroupInformation.isSecurityEnabled()) {
            // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
            Credentials credentials = new Credentials();
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null | | tokenRenewer.length() == 0) {
                throw new IOException(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }

            // For now, only getting tokens for the default file-system.
            final Token<?> tokens[] =
                    fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null) {
                for (Token<?> token : tokens) {
                    LOG.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }

        appContext.setAMContainerSpec(amContainer);

        //Set the priority for the application master
        Priority pri = Priority.newInstance(amPriority);
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(amQueue);

        // Submit the application to the applications manager
        yarnClient.submitApplication(appContext);

        //Get application report for the appId we are interested in
        ApplicationReport report = yarnClient.getApplicationReport(appId);

        //client kill
        yarnClient.killApplication(appId);
    }
}
