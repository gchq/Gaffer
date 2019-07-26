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
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import org.apache.commons.io.IOUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.PythonOperation;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PythonOperationHandler implements OperationHandler<PythonOperation> {

    private Git git;
    private final String pathAbsoluteResources = FileSystems.getDefault().getPath(".").toAbsolutePath() + "/core/store/src/main/resources";
    private final String repoName = "test";
    private final String repoURI = "https://github.com/g609bmsma/test";

    // Clone the git repo
    private Git getGit() {
        if (git == null) {
            try {
                git = Git.open(new File(pathAbsoluteResources + "/" + repoName));
            } catch (RepositoryNotFoundException e) {
                try {
                    git = Git.cloneRepository()
                            .setDirectory(new File(pathAbsoluteResources + "/" + repoName))
                            .setURI(repoURI)
                            .call();
                    System.out.println("Cloned the repo.");
                } catch (GitAPIException e1) {
                    e1.printStackTrace();
                    git = null;
                }
            } catch (IOException e) {
                e.printStackTrace();
                git = null;
            }
        }
        return git;
    }

    @Override
    public Object doOperation(final PythonOperation operation, final Context context, final Store store) throws OperationException {

        final String operationName = "PythonOperation1";
        final String entrypointFilename = "entrypoint" + operationName + ".py";
        final String scriptFilename = operationName + ".py";
        final String modulesFilename = operationName + "Modules.txt";

        // Pull or Clone the repo with the files
        System.out.println("Fetching the repo...");
        File dir = new File(pathAbsoluteResources + "/" + repoName);
        try {
            if (getGit() != null) {
                System.out.println("Repo already cloned, pulling files...");
                getGit().pull().call();
                System.out.println("Pulled the latest files.");
            } else {
                System.out.println("Repo has not been cloned, cloning the repo...");
                Git.cloneRepository().setDirectory(dir).setURI(repoURI).call();
                System.out.println("Cloned the repo.");
            }
        } catch (GitAPIException e) {
            e.printStackTrace();
        }

        // Get ready to copy the entrypoint, script and modules files
        File[] sources = new File[3];
        File[] destinations = new File[3];
        sources[0] = new File(dir + "/" + entrypointFilename);
        destinations[0] = new File(pathAbsoluteResources + "/" + entrypointFilename);
        sources[1] = new File(dir + "/" + scriptFilename);
        destinations[1] = new File(pathAbsoluteResources + "/" + scriptFilename);
        sources[2] = new File(dir + "/" + modulesFilename);
        destinations[2] = new File(pathAbsoluteResources + "/" + modulesFilename);

        // Copy the entrypoint, script and modules files
        System.out.println("Copying files...");
        for (int i = 0; i < sources.length; i++) {
            try (FileInputStream fis = new FileInputStream(sources[i]);
                 FileOutputStream fos = new FileOutputStream(destinations[i])) {

                byte[] buffer = new byte[1024];
                int length;

                while ((length = fis.read(buffer)) > 0) {

                    fos.write(buffer, 0, length);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Load the python modules file
        String moduleData = "";
        try {
            FileInputStream fis = new FileInputStream(pathAbsoluteResources + "/" + modulesFilename);
            moduleData = IOUtils.toString(fis, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[] modules = moduleData.split("\\n");

        // Create Dockerfile data
        StringBuilder dockerFileData = new StringBuilder("FROM python:3\n");
        String addEntrypointFileLine = "ADD " + entrypointFilename + " /\n";
        String addScriptFileLine = "ADD " + scriptFilename + " /\n";
        dockerFileData.append(addEntrypointFileLine).append(addScriptFileLine);

        for (String module : modules) {
            String installLine = "RUN pip install " + module + "\n";
            dockerFileData.append(installLine);
        }

        String entrypointLine = "ENTRYPOINT [ \"python\", \"./" + entrypointFilename + "\"]";
        dockerFileData.append(entrypointLine);

        // Create a new Dockerfile from the modules file
        File file = new File(pathAbsoluteResources + "/Dockerfile" + operationName + ".yml");
        System.out.println("Checking for Dockerfile...");
        try {
            if(file.createNewFile()) {
                // File created
                System.out.println("Creating a new Dockerfile...");
                Files.write(Paths.get(pathAbsoluteResources + "/Dockerfile" + operationName + ".yml"), dockerFileData.toString().getBytes());
            } else {
                // File already created
                System.out.println("Dockerfile already exists.");
            }
        } catch (IOException e) {
            System.out.println("Failed to create a Dockerfile");
            e.printStackTrace();
        }

        try {

            // Start the docker client
            System.out.println("Starting the Docker client...");
            DockerClient docker = DefaultDockerClient.fromEnv().build();

            // Build an image from the Dockerfile
            System.out.println("Building the image from the Dockerfile...");
            final AtomicReference<String> imageIdFromMessage = new AtomicReference<>();
            final String returnedImageId = docker.build(Paths.get(pathAbsoluteResources),"myimage:latest", "Dockerfile" + operationName + ".yml", message -> {
                final String imageId = message.buildImageId();
                if (imageId != null) {
                    imageIdFromMessage.set(imageId);
                }
                System.out.println(message);
            }, DockerClient.BuildParam.pullNewerImage());

            // Create a container from the image and bind ports
            final ContainerConfig containerConfig = ContainerConfig.builder()
                    .hostConfig( HostConfig.builder()
                            .portBindings(ImmutableMap.of("8080/tcp", Collections.singletonList(PortBinding.of("127.0.0" + ".1", "8080")))).build())
                    .image(returnedImageId)
                    .exposedPorts( "8080/tcp" )
                    .cmd("sh", "-c", "while :; do sleep 1; done")
                    .build();
            final ContainerCreation creation = docker.createContainer(containerConfig);
            final String id = creation.id();

            // Start the container
            System.out.println("Starting the Docker container...");
            docker.startContainer(id);

            // Keep trying to connect to container and give the container some time to load up
            boolean failedToConnect = true;
            IOException error = null;
            for (int i = 0; i < 10; i++) {
                System.out.println("Attempting to send data to container...");
                Socket clientSocket;

                try {
                    clientSocket = new Socket("127.0.0.1", 8080);
                    System.out.println("Connected to container port at " + clientSocket.getRemoteSocketAddress());

                    // Send the data
                    System.out.println("Sending data to docker container from " + clientSocket.getLocalSocketAddress() + "...");
                    OutputStream outToContainer = clientSocket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(outToContainer);
                    out.writeUTF("Hello from client at " + clientSocket.getLocalSocketAddress());

                    // Get the data from the container
                    System.out.println("Fetching data from container...");
                    InputStream inFromContainer = clientSocket.getInputStream();
                    DataInputStream in = new DataInputStream(inFromContainer);
                    System.out.println("Container says " + in.readUTF());
                    failedToConnect = false;
                    clientSocket.close();
                    System.out.println("Closed the connection.");
                    break;
                } catch (IOException e) {
                    error = e;
                    TimeUnit.MILLISECONDS.sleep(50);
                }
            }

            if (failedToConnect) {
                System.out.println("Connection failed, stopping the container...");
                error.printStackTrace();
                docker.stopContainer(id,1); // Kill the container after 1 second
            }

            // Delete the container
//            System.out.println("Deleting the container...");
//            docker.removeContainer(id);

            // Close the docker client
            System.out.println("Closing the docker client...");
            docker.close();
            System.out.println("Closed the docker client.");

        } catch (DockerCertificateException | InterruptedException | DockerException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}












