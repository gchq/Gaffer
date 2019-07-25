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

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.PythonOperation;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;

public class PythonOperationHandler implements OperationHandler<PythonOperation> {

    private static Git git;

    private static Git getGit() {
        if (git == null) {
            try {
                git = Git.open(new File(FileSystems.getDefault().getPath(".").toAbsolutePath() + "/PythonOperation/src/main/resources/test"));
            } catch (RepositoryNotFoundException e) {
                try {
                    git = Git.cloneRepository()
                            .setDirectory(new File(FileSystems.getDefault().getPath(".").toAbsolutePath() + "/PythonOperation/src/main/resources/test"))
                            .setURI("https://github.com/g609bmsma/test")
                            .call();
                    System.out.println("git cloned");
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
        final Path hostAbsolutePathRoot = FileSystems.getDefault().getPath(".").toAbsolutePath();
        final String hostAbsolutePathContainerResults = hostAbsolutePathRoot + "/PythonOperation/src/main/resources";
        final String containerResultsPath = "/hostBindMount";
        final String relativeImagePath = "PythonOperation/src/main/resources";
        String filename = "/testFileparameter.txt";

        File dir = new File(hostAbsolutePathContainerResults + "/test");

        try {
            if (getGit() != null) {
                getGit().pull().call();
                System.out.println("git pulled");
            } else {
                Git.cloneRepository().setDirectory(dir).setURI("https://github.com/g609bmsma/test").call();
                System.out.println("git cloned");
            }
        } catch (GitAPIException e) {
            e.printStackTrace();
        }

        File source = new File(dir + "/my_script.py");
        File dest = new File(hostAbsolutePathContainerResults + "/my_script.py");

        try (FileInputStream fis = new FileInputStream(source);
             FileOutputStream fos = new FileOutputStream(dest)) {

            byte[] buffer = new byte[1024];
            int length;

            while ((length = fis.read(buffer)) > 0) {

                fos.write(buffer, 0, length);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {

            // Start the docker client
            System.out.println("Starting the docker client...");
            DockerClient docker = DefaultDockerClient.fromEnv().build();

            // Define the bind mount between the container and the docker host
            final HostConfig hostConfig =
                    HostConfig.builder()
                            .appendBinds(
                                    HostConfig.Bind
                                            .from(hostAbsolutePathContainerResults)
                                            .to(containerResultsPath)
                                            .build()
                            )
                            .build();

            // Build an image from the Dockerfile
            System.out.println("Building the image from Dockerfile...");
            final String returnedImageId = docker.build(Paths.get(relativeImagePath), "myimage:latest");

            // Create a container from the image id with a bind mount to the docker host
            final ContainerConfig containerConfig = ContainerConfig.builder()
                    .hostConfig(hostConfig)
                    .image(returnedImageId)
                    .build();
            final ContainerCreation creation = docker.createContainer(containerConfig);
            final String id = creation.id();

            // Start the container
            System.out.println("Starting the docker container...");
            docker.startContainer(id);

            // Wait for the container to finish
            docker.waitContainer(id);

            // Print out the logs of the container
            final String logs;
            try (LogStream stream = docker.logs(id, DockerClient.LogsParam.stdout(), DockerClient.LogsParam.stderr())) {
                logs = stream.readFully();
            }
            System.out.println("Container logs: " + logs);

            // Delete the container
            System.out.println("Deleting the container...");
            docker.removeContainer(id);

            // Close the docker client
            System.out.println("Closing the docker client...");
            docker.close();
            System.out.println("Closed the docker client");

            // Get the data from the file created
            String data = "";
            data = new String(Files.readAllBytes(Paths.get(hostAbsolutePathContainerResults + filename)));
            System.out.println("The contents of the file created are: \n" + data);

            // Delete the file
            File file = new File(hostAbsolutePathContainerResults + filename);
            if (file.delete()) {
                System.out.println("File deleted successfully");
            } else {
                System.out.println("Failed to delete the file");
            }

        } catch (DockerCertificateException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (DockerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


}












