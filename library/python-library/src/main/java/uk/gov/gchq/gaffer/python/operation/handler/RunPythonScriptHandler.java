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
import uk.gov.gchq.gaffer.python.operation.BuildImageFromDockerfile;
import uk.gov.gchq.gaffer.python.operation.GetPort;
import uk.gov.gchq.gaffer.python.operation.PullOrCloneRepo;
import uk.gov.gchq.gaffer.python.operation.RunPythonScript;
import uk.gov.gchq.gaffer.python.operation.SetUpAndCloseContainer;

import java.io.IOException;
import java.nio.file.FileSystems;
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
    private final String repoName = "test";
    private final String pathAbsolutePythonRepo =
            FileSystems.getDefault().getPath(".").toAbsolutePath() + "/library/python-library/src" +
                    "/main/resources" + "/" + repoName;

    public Object doOperation(final RunPythonScript operation) throws OperationException {

        final String scriptName = operation.getScriptName();
        final Map<String, Object> parameters = operation.getParameters();
        Object output = null;

        // Pull or Clone the repo with the files
        pullOrCloneRepo.pullOrClone(git, pathAbsolutePythonRepo);

        try {

            // Connect to the Docker client. To ensure only one reference to the Docker client and to avoid
            // memory leaks, synchronize this code amongst multiple threads.
            LOGGER.info("Connecting to the Docker client...");

            DockerClient docker;
            synchronized (this) {
                docker = DefaultDockerClient.fromEnv().build();
            }
            LOGGER.info("Docker is now: {}", docker);
            final String returnedImageId = buildImageFromDockerfile.buildImage(scriptName, parameters, docker, pathAbsolutePythonRepo);

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
            String containerId = null;
            for (int i = 0; i < 100; i++) {
                try {
                    port = getPort.getPort();

                    // Create a container from the image and bind ports
                    final ContainerConfig containerConfig = ContainerConfig.builder().hostConfig(HostConfig.builder().portBindings(ImmutableMap.of("80/tcp", Collections.singletonList(PortBinding.of("127.0.0.1", port)))).build()).image(returnedImageId).exposedPorts("80/tcp").cmd("sh", "-c", "while :; do sleep 1; done").build();
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

            LOGGER.info("Closed the connection.");

            output = JSONSerialiser.deserialise(dataReceived.toString(),
                    operation.getOutputClass());

            // Delete the container
            LOGGER.info("Deleting the container...");
            docker.waitContainer(containerId);
            docker.removeContainer(containerId);

            docker.close();

        } catch (final DockerCertificateException | InterruptedException | DockerException | IOException e) {
            e.printStackTrace();
        }
        return output;
    }
}
