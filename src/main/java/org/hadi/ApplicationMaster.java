package org.hadi;

import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static org.apache.hadoop.fs.FSInputChecker.LOG;

public class ApplicationMaster{
    public static void main(String[] args) throws Exception {
        //intialize A-M
        Map<String, String> envs = System.getenv();
        String containerIdString =
                envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
        if (containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException(
                    "ContainerId not set in the environment");
        }
        ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
        ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();

        //start client of R-M and N-M
        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amRMClient.init(conf);
        amRMClient.start();

        containerListener = createNMCallbackHandler();
        NMClientAsyncImpl nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        //The ApplicationMaster needs to register itself with the ResourceManager to start heartbeating
        appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort,appMasterTrackingUrl);

        // set resource manager
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capability of resources in this cluster " + maxMem);

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capability of resources in this cluster " + maxVCores);

        // A resource ask cannot exceed the max.
        if (containerMemory > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + containerMemory + ", max="
                    + maxMem);
            containerMemory = maxMem;
        }

        if (containerVirtualCores > maxVCores) {
            LOG.info("Container virtual cores specified above max threshold of  cluster."
                    + " Using max value." + ", specified=" + containerVirtualCores + ", max="
                    + maxVCores);
            containerVirtualCores = maxVCores;
        }
        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info("Received " + previousAMRunningContainers.size()
                + " previous AM's running containers on AM registration.");

        //calculate containers and requeste theme
        previousAMRunningContainers = response.getContainersFromPreviousAttempts();
        LOG.info("Received " + previousAMRunningContainers.size()
                + " previous AM's running containers on AM registration.");

        int numTotalContainersToRequest =
                numTotalContainers - previousAMRunningContainers.size();

        for (int i = 0; i < numTotalContainersToRequest; ++i) {
            ContainerRequest containerAsk = setupContainerAskForRM();
            amRMClient.addContainerRequest(containerAsk);
        }

        private ContainerRequest setupContainerAskForRM() {
            // set the priority for the request
            Priority pri = Priority.newInstance(requestPriority);

            // Set up resource type requirements
            // For now, memory and CPU are supported so we set memory and cpu requirements
            Resource capability = Resource.newInstance(containerMemory,
                    containerVirtualCores);

            ContainerRequest request = new ContainerRequest(capability, null, null,
                    pri);
            LOG.info("Requested container ask: " + request.toString());
            return request;
        }

        //launch container
        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                LaunchContainerRunnable runnableLaunchContainer =
                        new LaunchContainerRunnable(allocatedContainer, containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);

                // launch and start the container on a separate thread to keep
                // the main thread unblocked
                // as all containers may not be allocated at one go.
                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        //get progress
        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            float progress = (float) numCompletedContainers.get()
                    / numTotalContainers;
            return progress;
        }

        //set command
        Vector<CharSequence> vargs = new Vector<CharSequence>(5);
        // Set executable command
        vargs.add(shellCommand);
        // Set shell script path
        if (!scriptPath.isEmpty()) {
            vargs.add(Shell.WINDOWS ? ExecBatScripStringtPath
                    : ExecShellStringPath);
        }
        // Set args for the shell command if any
        vargs.add(shellArgs);
        // Add log redirect params
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        // Get final command
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());

        //setup the container launch context
        ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                localResources, shellEnv, commands, null, allTokens.duplicate(), null);
        Thread container;
        containerListener.addContainer(container.getId(), container);
        nmClientAsync.startContainerAsync(container, ctx);

        //AM_RM client
        try {
            Object appMessage;
            Object appStatus;
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }

        amRMClient.stop();


    }
}
