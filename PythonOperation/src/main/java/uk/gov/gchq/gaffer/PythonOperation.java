package uk.gov.gchq.gaffer;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.ProgressHandler;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class PythonOperation {

    public static void main(String[] args) {

        System.out.println("Got here 1");

        // Bind container ports to host ports
        final String[] ports = {"80", "22"};
        final Map<String, List<PortBinding>> portBindings = new HashMap<>();
        for (String port : ports) {
            List<PortBinding> hostPorts = new ArrayList<>();
            hostPorts.add(PortBinding.of("0.0.0.0", port));
            portBindings.put(port, hostPorts);
        }

        System.out.println("Got here 2");

        // Bind container port 443 to an automatically allocated available host port.
        List<PortBinding> randomPort = new ArrayList<>();
        randomPort.add(PortBinding.randomPort("0.0.0.0"));
        portBindings.put("443", randomPort);

        final HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();

        // Create a client based on DOCKER_HOST and DOCKER_CERT_PATH env vars
        DockerClient docker = null;

        System.out.println("Got here 3");
        try {
            System.out.println("Got here 4");
            docker = DefaultDockerClient.fromEnv().build();
            System.out.println("Got here 4.1");

            final AtomicReference<String> imageIdFromMessage = new AtomicReference<>();

            final String returnedImageId = docker.build(Paths.get("pythonOperation/src/main/resources"),"myimage:latest");


            System.out.println("Got here 4.2");
            // Create container with exposed ports
            final ContainerConfig containerConfig = ContainerConfig.builder()
                    .hostConfig(hostConfig)
                    .image(returnedImageId).exposedPorts(ports)
                    .build();
            System.out.println("Got here 4.3");
            final ContainerCreation creation = docker.createContainer(containerConfig);
            System.out.println("Got here 4.4");
            final String id = creation.id();
            System.out.println("Got here 5");
            // Inspect container
            final ContainerInfo info = docker.inspectContainer(id);

            // Start container
            docker.startContainer(id);
            System.out.println("Got here 6");
            // Exec command inside running container with attached STDOUT and STDERR
            final String[] command = {"sh", "-c", "ls"};
            final ExecCreation execCreation = docker.execCreate(
                    id, command, DockerClient.ExecCreateParam.attachStdout(),
                    DockerClient.ExecCreateParam.attachStderr());
            final LogStream output = docker.execStart(execCreation.id());
            final String execOutput = output.readFully();
            System.out.println("Got here 7");

            // Kill container
//            docker.killContainer(id);

            // Remove container
//            docker.removeContainer(id);

            // Close the docker client
            docker.close();
            System.out.println("Got here");

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
