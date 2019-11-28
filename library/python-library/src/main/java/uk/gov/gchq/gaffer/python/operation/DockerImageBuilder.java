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

package uk.gov.gchq.gaffer.python.operation;

import com.google.gson.Gson;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class DockerImageBuilder implements ImageBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerImageBuilder.class);

    /**
     * Builds docker image from Dockerfile
     *
     * @param scriptName             the name of the script being run
     * @param scriptParameters       the parameters of the script being run
     * @param dockerObject           the docker client the script is being run on
     * @param pathToBuildFiles       the absolute path for the repo
     * @return docker image from Dockerfile
     */

    @Override
    public Image buildImage(final String scriptName, final Map<String, Object> scriptParameters,
                        final Object dockerObject, final String pathToBuildFiles) {

        DockerClient docker = (DockerClient) dockerObject;

        // Convert the script parameters into a string
        String params = stringifyParameters(scriptParameters);

        // Create the build arguments
        StringBuilder buildargs = new StringBuilder();
        buildargs.append("{\"scriptName\":\"").append(scriptName).append("\",");
        buildargs.append("\"scriptParameters\":\"").append(params).append("\",");
        buildargs.append("\"modulesName\":\"").append(scriptName).append("Modules").append("\"}");
        LOGGER.info(String.valueOf(buildargs));

        DockerClient.BuildParam buildParam = null;
        try {
            buildParam = DockerClient.BuildParam.create("buildargs", URLEncoder.encode(String.valueOf(buildargs), "UTF-8"));
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        // Build an image from the Dockerfile
        LOGGER.info("Building the image from the Dockerfile...");
        LOGGER.info("Absolute repo path: " + Paths.get(pathToBuildFiles).toString());
        try {
            final AtomicReference<String> imageIdFromMessage = new AtomicReference<>();
            return new DockerImage(docker.build(Paths.get(pathToBuildFiles + "/"),
                    "scriptoperation:" + scriptName, "Dockerfile", message -> {
                final String imageId = message.buildImageId();
                if (imageId != null) {
                    imageIdFromMessage.set(imageId);
                }
                LOGGER.info(String.valueOf(message));
            }, buildParam));
        } catch (final DockerException | InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void getFiles(final String pathToBuildFiles, final String dockerfilePath) {
        String[] fileNames = new String[] {"DataInputStream.py", "entrypoint.py", "modules.txt"};
        if (dockerfilePath.equals("")) {
            // Use the default file
            LOGGER.info("DockerfilePath unspecified, using default Dockerfile");
            createFile("Dockerfile", pathToBuildFiles);
        } else {
            LOGGER.info("DockerfilePath specified, using non-default dockerfile");
            final String[] pathSplit = dockerfilePath.split("/");
            final String fileName = pathSplit[pathSplit.length - 1];
            final String fileLocation = dockerfilePath.substring(0, dockerfilePath.length() - fileName.length());
            createFile(fileName, pathToBuildFiles, fileLocation);
        }
        for (final String fileName : fileNames) {
            createFile(fileName, pathToBuildFiles);
        }
    }

    private void createFile(final String fileName, final String destination) {
        createFile(fileName, destination, "/.ScriptBin/");
    }

    private void createFile(final String fileName, final String destination, final String fileLocation) {
        try (InputStream inputStream = StreamUtil.openStream(getClass(), fileLocation + fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String fileData = reader.lines().collect(Collectors.joining(System.lineSeparator()));
            inputStream.close();
            Files.write(Paths.get(destination + "/" + fileName), fileData.getBytes());
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    private String stringifyParameters(final Map<String, Object> scriptParameters) {
        String params = " ";
        if (scriptParameters != null) {
            Map<String, String> stringParameters = new HashMap<>();

            for (final String parameterName: scriptParameters.keySet()) {
                if (scriptParameters.get(parameterName) != null) {
                    stringParameters.put(parameterName, scriptParameters.get(parameterName).toString());
                }
            }
            params = new Gson().toJson(stringParameters).replaceAll("\"", "'");
        }
        return params;
    }
}
