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
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.errors.RepositoryNotFoundException;

import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.PythonOperation;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import javax.imageio.IIOException;
import java.io.*;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PythonOperationHandler implements OperationHandler<PythonOperation> {

    private Git git;
    private final String repoName = "test";
    private final String repoURI = "https://github.com/g609bmsma/test";
    private final String pathAbsolutePythonRepo = FileSystems.getDefault().getPath(".").toAbsolutePath() + "/core/store/src/main/resources" + "/" + repoName;

    // Clone the git repo
    private Git getGit() {

        if (git == null) {
            try {
                git = Git.open(new File(pathAbsolutePythonRepo));
            } catch (RepositoryNotFoundException e) {
                try {
                    git = Git.cloneRepository()
                            .setDirectory(new File(pathAbsolutePythonRepo))
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

        final String scriptName = "script1";
        final String scriptFilename = scriptName + ".py";
        final String entrypointFilename = scriptName + "Entrypoint.py";
        final String modulesFilename = scriptName + "Modules.txt";
        final String dockerfileName = scriptName + "Dockerfile.yml";

        // Pull or Clone the repo with the files
        System.out.println("Fetching the repo...");
        File dir = new File(pathAbsolutePythonRepo);
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

        // Load the python modules file
        String moduleData = "";
        try {
            FileInputStream fis = new FileInputStream(pathAbsolutePythonRepo + "/" + modulesFilename);
            System.out.println("Modules file found. Loading module data...");
            moduleData = IOUtils.toString(fis, "UTF-8");
            System.out.println("Loaded module data.");
        } catch (FileNotFoundException e) {
            System.out.println("No modules file found. Continuing without.");
        } catch (IOException e) {
            System.out.println("Unable to load modules file.");
            e.printStackTrace();
        }
        String[] modules = moduleData.split("\\n");
        System.out.println(modules);

        // Create Dockerfile data
        StringBuilder dockerFileData = new StringBuilder("FROM python:3\n");
        String addEntrypointFileLine = "ADD " + entrypointFilename + " /\n";
        String addScriptFileLine = "ADD " + scriptFilename + " /\n";
        dockerFileData.append(addEntrypointFileLine).append(addScriptFileLine);

        for (String module : modules) {
            if (module != "") {
                String installLine = "RUN pip install " + module + "\n";
                dockerFileData.append(installLine);
            }
        }

        String entrypointLine = "ENTRYPOINT [ \"python\", \"./" + entrypointFilename + "\"]";
        dockerFileData.append(entrypointLine);

        // Create a new Dockerfile from the modules file
        System.out.println("Creating a new Dockerfile...");
        try {
            Files.write(Paths.get(pathAbsolutePythonRepo + "/" + dockerfileName), dockerFileData.toString().getBytes());
            System.out.println("Dockerfile created.");
        } catch (IOException e) {
            System.out.println("Failed to create a new Dockerfile");
            e.printStackTrace();
        }

        // Load the data from the base entrypoint file
        String entrypointFileData = "from " + scriptName + " import run\n";
        System.out.println("Attempting to load the base entrypoint file...");
        try {
            FileInputStream fis = new FileInputStream(pathAbsolutePythonRepo + "/../entrypoint.py");
            System.out.println("Entrypoint file found. Loading data...");
            String entrypointBaseFileData = IOUtils.toString(fis, "UTF-8");
            entrypointFileData = entrypointFileData + entrypointBaseFileData;
            System.out.println("Loaded module data.");
        } catch (IOException e) {
            System.out.println("Unable to load file.");
            e.printStackTrace();
        }

        // Create the Entrypoint file
        System.out.println("Creating a new Entrypoint file...");
        try {
            Files.write(Paths.get(pathAbsolutePythonRepo + "/" + entrypointFilename), entrypointFileData.getBytes());
            System.out.println("Entrypoint file created.");
        } catch (IOException e) {
            System.out.println("Failed to create an Entrypoint file.");
            e.printStackTrace();
        }

        try {

            // Start the docker client
            System.out.println("Starting the Docker client...");
            DockerClient docker = DefaultDockerClient.fromEnv().build();

            // Build an image from the Dockerfile
            System.out.println("Building the image from the Dockerfile...");
            final AtomicReference<String> imageIdFromMessage = new AtomicReference<>();
            final String returnedImageId = docker.build(Paths.get(pathAbsolutePythonRepo),"myimage:latest", dockerfileName, message -> {
                final String imageId = message.buildImageId();
                if (imageId != null) {
                    imageIdFromMessage.set(imageId);
                }
                System.out.println(message);
            }, DockerClient.BuildParam.pullNewerImage());

            // Create a container from the image and bind ports
            final ContainerConfig containerConfig = ContainerConfig.builder()
                    .hostConfig( HostConfig.builder()
                            .portBindings(ImmutableMap.of("8080/tcp", Collections.singletonList(PortBinding.of("127.0.0.1", "8080")))).build())
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
                    System.out.println("Failed to fetch data.");
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












