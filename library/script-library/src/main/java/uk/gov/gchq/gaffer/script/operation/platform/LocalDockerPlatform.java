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
package uk.gov.gchq.gaffer.script.operation.platform;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.script.operation.builder.DockerImageBuilder;
import uk.gov.gchq.gaffer.script.operation.container.Container;
import uk.gov.gchq.gaffer.script.operation.container.LocalDockerContainer;
import uk.gov.gchq.gaffer.script.operation.generator.RandomPortGenerator;
import uk.gov.gchq.gaffer.script.operation.handler.RunScriptHandler;
import uk.gov.gchq.gaffer.script.operation.image.DockerImage;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LocalDockerPlatform implements ImagePlatform {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunScriptHandler.class);
    private final DockerImageBuilder dockerImageBuilder = new DockerImageBuilder();
    private DockerClient docker = null;
    private String dockerfilePath = "";
    private int port;

    /**
     * Builds a docker image and creates a docker container instance.
     *
     * @param scriptName             the name of the script being run
     * @param scriptParameters       the parameters of the script being run
     * @param pathToBuildFiles       the path to the directory containing the build files
     * @param ip                     the ip the container is connected to
     * @return the docker container
     */
    @Override
    public Container createContainer(final String scriptName, final Map<String, Object> scriptParameters, final String pathToBuildFiles, final String ip) {
        String containerId = "";
        try {
            dockerImageBuilder.getFiles(pathToBuildFiles, dockerfilePath);
            // Connect to the Docker client. To ensure only one reference to the Docker client and to avoid
            // memory leaks, synchronize this code amongst multiple threads.
            LOGGER.info("Connecting to the Docker client...");

            synchronized (this) {
                docker = DefaultDockerClient.fromEnv().build();
            }
            LOGGER.info("Docker is now: {}", docker);
            final DockerImage returnedImageId = (DockerImage) dockerImageBuilder.buildImage(scriptName, scriptParameters, docker, pathToBuildFiles);

            // Remove the old images
            final List<com.spotify.docker.client.messages.Image> images;
            images = docker.listImages();
            String repoTag = "[<none>:<none>]";
            for (final Image image : images) {
                if (Objects.requireNonNull(image.repoTags()).toString().equals(repoTag)) {
                    docker.removeImage(image.id());
                }
            }
            // Keep trying to create a container and find a free port.
                try {
                    port = RandomPortGenerator.getInstance().generatePort();

                    // Create a container from the image and bind ports
                    final ContainerConfig containerConfig = ContainerConfig.builder().hostConfig(HostConfig.builder().portBindings(ImmutableMap.of("80/tcp", Collections.singletonList(PortBinding.of(ip, port)))).build()).image(returnedImageId.getImageString()).exposedPorts("80/tcp").cmd("sh", "-c", "while :; do sleep 1; done").build();
                    final ContainerCreation creation = docker.createContainer(containerConfig);
                    containerId = creation.id();
                } catch (final DockerRequestException ignored) {
                }
        } catch (final DockerCertificateException | InterruptedException | DockerException e) {
            e.printStackTrace();
        }
        return new LocalDockerContainer(containerId, port);
    }

    /**
     * Starts a docker container
     *
     * @param container             the container
     */
    private void startContainer(final Container container) {
        for (int i = 0; i < 100; i++) {
            try {
                LOGGER.info("Starting the Docker container...");
                docker.startContainer(container.getContainerId());
                break;
            } catch (final DockerException | InterruptedException ignored) {
            }
        }
    }

    /**
     * Stops and closes a docker container
     *
     * @param container             the container
     */
    private void closeContainer(final Container container) {
        try {
            LOGGER.info("Closing the Docker container...");
            docker.waitContainer(container.getContainerId());
            docker.removeContainer(container.getContainerId());
            docker.close();
        } catch (final DockerException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Runs a docker container
     *
     * @param container              the container to run
     * @param inputData              the data to pass to the container
     * @return the result of the container
     */
    @Override
    public StringBuilder runContainer(final Container container, final Iterable inputData) {
        startContainer(container);
        container.sendData(inputData);
        StringBuilder output = container.receiveData();
        closeContainer(container);
        RandomPortGenerator.getInstance().freePort(port);
        return output;
    }

    private String getDockerfilePath() {
        return dockerfilePath;
    }

    private void setDockerfilePath(final String dockerfilePath) {
        this.dockerfilePath = dockerfilePath;
    }
}
