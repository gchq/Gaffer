package uk.gov.gchq.gaffer.store.operation.handler;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class BuildImageFromDockerfile {
    public BuildImageFromDockerfile() {
    }

    /**
     * Builds docker imafe from Dockerfile
     */
    String buildImage(String scriptName, List<Object> parameters, DockerClient docker, String pathAbsolutePythonRepo) throws DockerException, InterruptedException, IOException {
        // Build an image from the Dockerfile
        final String buildargs = "{\"scriptName\":\"" + scriptName + "\",\"parameters\":\"" + parameters + "\",\"modulesName\":\"" + scriptName + "Modules" + "\"}";
        System.out.println(buildargs);
        final DockerClient.BuildParam buildParam = DockerClient.BuildParam.create("buildargs", URLEncoder.encode(buildargs, "UTF-8"));

        System.out.println("Building the image from the Dockerfile...");
        final AtomicReference<String> imageIdFromMessage = new AtomicReference<String>();
        return docker.build(Paths.get(pathAbsolutePythonRepo + "/../"), "pythonoperation:" + scriptName, "Dockerfile", message -> {
            final String imageId = message.buildImageId();
            if (imageId != null) {
                imageIdFromMessage.set(imageId);
            }
            System.out.println(message);
        }, buildParam);
    }
}