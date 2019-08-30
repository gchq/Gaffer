/*
 * Copyright 2019 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.python.operation.handler;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.exceptions.DockerRequestException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.Image;
import com.spotify.docker.client.messages.PortBinding;
import org.eclipse.jgit.api.Git;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.python.operation.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RunPythonScriptHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunPythonScriptHandler.class);
    private final SetUpAndCloseContainer setUpAndCloseContainer = new SetUpAndCloseContainer();
    private final PullOrCloneRepo pullOrCloneRepo = new PullOrCloneRepo();
    private final BuildImageFromDockerfile buildImageFromDockerfile = new BuildImageFromDockerfile();
    private final GetPort getPort = new GetPort();
    private Git git = null;
    private DockerClient docker = null;
    private String containerId = null;

    public Object doOperation(final RunPythonScript operation) throws OperationException {

        final String repoName = operation.getRepoName();
        final Path pathAbsolutePythonRepo = Paths.get(System.getProperty("user.home"),"Documents","/Gaffer/NeilG/Gaffer","/library/python-library/src/main/resources/",repoName);
        Object output = null;
        final String scriptName = operation.getScriptName();
        final Map<String, Object> scriptParameters = operation.getScriptParameters();
        final ScriptOutputType scriptOutputType = operation.getScriptOutputType();
        final ScriptInputType scriptInputType = operation.getScriptInputType();

        // Pull or Clone the repo with the files
        pullOrCloneRepo.pullOrClone(git, pathAbsolutePythonRepo.toString(), operation);

        try {

            // Connect to the Docker client. To ensure only one reference to the Docker client and to avoid
            // memory leaks, synchronize this code amongst multiple threads.
            LOGGER.info("Connecting to the Docker client...");

            synchronized (this) {
                docker = DefaultDockerClient.fromEnv().build();
            }
            LOGGER.info("Docker is now: {}", docker);
            final String returnedImageId = buildImageFromDockerfile.buildImage(scriptName, scriptParameters, scriptInputType, docker, pathAbsolutePythonRepo.toString());

            // Remove the old images
            final List<Image> images;
            images = docker.listImages();
            String repoTag = "[<none>:<none>]";
            for (final Image image : images) {
                if (Objects.requireNonNull(image.repoTags()).toString().equals(repoTag)) {
                    docker.removeImage(image.id());
                }
            }

            // Keep trying to start a container and find a free port.
            String port = null;
            boolean portAvailable = false;
            String ip = operation.getIp();
            for (int i = 0; i < 100; i++) {
                try {
                    port = getPort.getPort();

                    // Create a container from the image and bind ports
                    final ContainerConfig containerConfig = ContainerConfig.builder().hostConfig(HostConfig.builder().portBindings(ImmutableMap.of("80/tcp", Collections.singletonList(PortBinding.of(ip, port)))).build()).image(returnedImageId).exposedPorts("80/tcp").cmd("sh", "-c", "while :; do sleep 1; done").build();
                    final ContainerCreation creation = docker.createContainer(containerConfig);
                    containerId = creation.id();

                    // Start the container
                    LOGGER.info("Starting the Docker container...");
                    docker.startContainer(containerId);

                    portAvailable = true;
                    break;
                } catch (final DockerRequestException ignored) {
                }
            }
            LOGGER.info("Port number is: "+ port);

            if (!portAvailable) {
                LOGGER.info("Failed to find an available port");
            }
            StringBuilder dataReceived = setUpAndCloseContainer.setUpAndCloseContainer(operation, docker, port, containerId);

            switch(scriptOutputType) {
                case ELEMENTS:
                    output = JSONSerialiser.deserialise(dataReceived.toString(), operation.getOutputClass());
                    break;
                case JSON:
                    output = dataReceived.toString();
                    break;
                default:
                    output = null;
            }

            LOGGER.info("Closed the connection.");

        } catch (final DockerCertificateException | InterruptedException | DockerException | IOException e) {
            e.printStackTrace();
        }
        finally {
            LOGGER.info("Deleting the container...");
            try {
                docker.waitContainer(containerId);
                docker.removeContainer(containerId);
            }
            catch (DockerException | InterruptedException e) {
                e.printStackTrace();
            }
            docker.close();
        }
        return output;
    }
}
