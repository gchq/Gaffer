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
package uk.gov.gchq.gaffer;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PythonOperation {

    public static void main(String[] args) {

        final Path hostAbsolutePathRoot = FileSystems.getDefault().getPath(".").toAbsolutePath();
        final String hostAbsolutePathContainerResults = hostAbsolutePathRoot + "/PythonOperation/src/main/resources";
        final String containerResultsPath = "/hostBindMount";
        final String relativeImagePath = "PythonOperation/src/main/resources";
        String filename = "/testFileparameter.txt";

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
            final String returnedImageId = docker.build(Paths.get(relativeImagePath),"myimage:latest");

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

            // Print out the logs of the container
            final String logs;
            try (LogStream stream = docker.logs(id, DockerClient.LogsParam.stdout(), DockerClient.LogsParam.stderr())) {
                logs = stream.readFully();
            }
            System.out.println("Container logs: " + logs);

            // Wait for the container to finish then remove the container
            docker.waitContainer(id);
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
            if(file.delete())
            {
                System.out.println("File deleted successfully");
            }
            else
            {
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

    }

}
