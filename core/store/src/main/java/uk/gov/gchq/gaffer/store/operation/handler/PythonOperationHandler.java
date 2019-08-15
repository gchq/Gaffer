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
package uk.gov.gchq.gaffer.store.operation.handler;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.exceptions.DockerRequestException;
import com.spotify.docker.client.messages.*;
import org.eclipse.jgit.api.Git;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.PythonOperation;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URLEncoder;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PythonOperationHandler implements OperationHandler<PythonOperation> {

    private final SetUpAndCloseContainer setUpAndCloseContainer = new SetUpAndCloseContainer(this);
    private final PullOrCloneRepo pullOrCloneRepo = new PullOrCloneRepo();
    private Git git = null;
    private final String repoName = "test";
    private final String pathAbsolutePythonRepo = FileSystems.getDefault().getPath(".").toAbsolutePath() + "/core/store/src/main/resources" + "/" + repoName;

    @Override
    public Object doOperation(final PythonOperation operation, final Context context, final Store store) throws OperationException {

        final String scriptName = operation.getScriptName();
        final List<Object> parameters = operation.getParameters();
        Object output = null;

        // Pull or Clone the repo with the files
        pullOrCloneRepo.pullOrClone(git);

        try {

            // Connect to the Docker client. To ensure only one reference to the Docker client and to avoid
            // memory leaks, synchronize this code amongst multiple threads.
            System.out.println("Connecting to the Docker client...");

            DockerClient docker;
            synchronized(this){
                docker = DefaultDockerClient.fromEnv().build();
            }
            System.out.println("Docker is now: " + docker);
            final String returnedImageId = buildImage(scriptName, parameters, docker);

            // Remove the old images
            final List<Image> images;
            images = docker.listImages();
            String repoTag = "[<none>:<none>]";
            for (Image image : images) {
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
                    port = getPort();

                    // Create a container from the image and bind ports
                    final ContainerConfig containerConfig = ContainerConfig.builder().hostConfig(HostConfig.builder().portBindings(ImmutableMap.of("80/tcp", Collections.singletonList(PortBinding.of("127.0.0.1", port)))).build()).image(returnedImageId).exposedPorts("80/tcp").cmd("sh", "-c", "while :; do sleep 1; done").build();
                    final ContainerCreation creation = docker.createContainer(containerConfig);
                    containerId = creation.id();

                    // Start the container
                    System.out.println("Starting the Docker container...");
                    docker.startContainer(containerId);

                    portAvailable = true;
                    break;
                } catch (DockerRequestException ignored) {
                }
            }
            System.out.println("Port number is: "+ port);

            if (!portAvailable) {
                System.out.println("Failed to find an available port");
            }
            StringBuilder dataReceived = setUpAndCloseContainer(operation, docker, port, containerId);

            System.out.println("Closed the connection.");
            System.out.println(dataReceived);

            output = JSONSerialiser.deserialise(dataReceived.toString(),
                    operation.getOutputClass());

            // Delete the container
            System.out.println("Deleting the container...");
            docker.waitContainer(containerId);
            docker.removeContainer(containerId);

            docker.close();

        } catch (final DockerCertificateException | InterruptedException | DockerException | IOException e) {
            e.printStackTrace();
        }
        return output;
    }

    /** Builds docker imafe from Dockerfile */
    private String buildImage(String scriptName, List<Object> parameters, DockerClient docker) throws DockerException, InterruptedException, IOException {
        // Build an image from the Dockerfile
        final String buildargs = "{\"scriptName\":\"" + scriptName + "\",\"parameters\":\"" + parameters + "\",\"modulesName\":\"" + scriptName + "Modules" + "\"}";
        System.out.println(buildargs);
        final DockerClient.BuildParam buildParam = DockerClient.BuildParam.create("buildargs", URLEncoder.encode(buildargs, "UTF-8"));

        System.out.println("Building the image from the Dockerfile...");
        final AtomicReference<String> imageIdFromMessage = new AtomicReference<>();
        return docker.build(Paths.get(pathAbsolutePythonRepo + "/../"), "pythonoperation:" + scriptName, "Dockerfile", message -> {
            final String imageId = message.buildImageId();
            if (imageId != null) {
                imageIdFromMessage.set(imageId);
            }
            System.out.println(message);
        }, buildParam);
    }

    /** Sets up and closes container */
    private StringBuilder setUpAndCloseContainer(PythonOperation operation, DockerClient docker, String port, String containerId) throws InterruptedException, DockerException, IOException {
        // Keep trying to connect to container and give the container some time to load up
        return setUpAndCloseContainer.setUpAndCloseContainer(operation, docker, port, containerId);
    }

    /** Sends data to and gets data from container */
    DataInputStream sendAndGetData(PythonOperation operation, Socket clientSocket) throws IOException {
        // Send the data
        System.out.println("Sending data to docker container from " + clientSocket.getLocalSocketAddress() + "...");
        OutputStream outToContainer = clientSocket.getOutputStream();
        DataOutputStream out = new DataOutputStream(outToContainer);
        boolean firstObject = true;
        for (Object current : operation.getInput()) {
            if (firstObject) {
                out.writeUTF("[" + new String(JSONSerialiser.serialise(current)));
                firstObject = false;
            }
            else {
                out.writeUTF(", " + new String(JSONSerialiser.serialise(current)));
            }
        }
        out.writeUTF("]");
        out.flush();
        //out.writeUTF(dataToSend);
        System.out.println("Waiting for response from Container...");
        // Get the data from the container
        InputStream inFromContainer = clientSocket.getInputStream();
        return new DataInputStream(inFromContainer);
    }

    /** Pulls or clones repo of python scripts as needed */
    private void pullOrClone(Git git, String pathAbsolutePythonRepo) {
        pullOrCloneRepo.pullOrClone(git, pathAbsolutePythonRepo);
    }

    /** Get a random port number */
    private String getPort() {
        List<Integer> portsList = IntStream.rangeClosed(50000, 65535).boxed().collect(Collectors.toList());
        Random rand = new Random();
        Integer portNum = portsList.get(rand.nextInt(portsList.size()));
        return String.valueOf(portNum);
    }
}
