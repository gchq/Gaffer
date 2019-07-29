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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

        final String operationName = "PythonOperation1";
        final String entrypointFilename = "entrypoint" + operationName + ".py";
        final String scriptFilename = operationName + ".py";
        final String modulesFilename = operationName + "Modules.txt";

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
        File file = new File(pathAbsolutePythonRepo + "/Dockerfile" + operationName + ".yml");
        System.out.println("Checking for Dockerfile...");
        try {
            if(file.createNewFile()) {
                // File created
                System.out.println("Creating a new Dockerfile...");
                Files.write(Paths.get(pathAbsolutePythonRepo + "/Dockerfile" + operationName + ".yml"), dockerFileData.toString().getBytes());
            } else {
                // File already created
                System.out.println("Dockerfile already exists.");
            }
        } catch (IOException e) {
            System.out.println("Failed to create a Dockerfile");
            e.printStackTrace();
        }

        // Create entrypoint file data
        StringBuilder entrypointFileData = new StringBuilder("from PythonOperation1 import pythonOperation1\n" +
                "import socket\n" +
                "import json\n" +
                "\n" +
                "HOST = socket.gethostbyname(socket.gethostname())\n" +
                "PORT = 8080\n" +
                "print('Listening for connections from host: ', socket.gethostbyname(socket.gethostname())) #172.17.0.2\n" +
                "\n" +
                "with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:\n" +
                "    # Setup the port and get it ready for listening for connections\n" +
                "    s.bind((HOST,PORT))\n" +
                "    s.listen()\n" +
                "    print('Yaaas queen it worked')\n" +
                "    print('Waiting for incoming connections...')\n" +
                "    conn, addr = s.accept() # Wait for incoming connections # Causes nothing to be printed to logs\n" +
                "    print('Connected to: ', addr)\n" +
                "    dataReceived = False\n" +
                "    while not dataReceived:\n" +
                "        data = conn.recv(1024)\n" +
                "        if data:\n" +
                "            jdata = data.decode(\"utf-8\", errors=\"ignore\")\n" +
                "            jdata = json.dumps(jdata)\n" +
                "            print(type(data))\n" +
                "            print('Recieved data : ', data)\n" +
                "            dataReceived = True\n" +
                "            # data = pythonOperation1(data)\n" +
                "            print('Resulting data : ', data)\n" +
                "            conn.sendall(data)  # Send the data back");

        // Create entrypoint file
        File entrypointFile = new File(pathAbsolutePythonRepo + "/" + entrypointFilename);
        System.out.println("Checking for entrypoint file...");
        try {
            if(entrypointFile.createNewFile()) {
                // File created
                System.out.println("Creating a new entrypoint file...");
                Files.write(Paths.get(pathAbsolutePythonRepo + "/" + entrypointFilename), entrypointFileData.toString().getBytes());
            } else {
                // File already created
                System.out.println("Entrypoint file already exists.");
            }
        } catch (IOException e) {
            System.out.println("Failed to create an entrypoint file.");
            e.printStackTrace();
        }

        try {

            // Start the docker client
            System.out.println("Starting the Docker client...");
            DockerClient docker = DefaultDockerClient.fromEnv().build();

            // Build an image from the Dockerfile
            System.out.println("Building the image from the Dockerfile...");
            final AtomicReference<String> imageIdFromMessage = new AtomicReference<>();
            final String returnedImageId = docker.build(Paths.get(pathAbsolutePythonRepo),"myimage:latest", "Dockerfile" + operationName + ".yml", message -> {
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












