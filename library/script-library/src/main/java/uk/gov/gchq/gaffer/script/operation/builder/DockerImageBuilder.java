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

package uk.gov.gchq.gaffer.script.operation.builder;

import com.google.gson.Gson;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.script.operation.image.DockerImage;
import uk.gov.gchq.gaffer.script.operation.image.Image;
import uk.gov.gchq.gaffer.script.operation.util.DockerClientSingleton;

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
     * Builds a docker image, which runs a script, from a Dockerfile
     *
     * @param scriptName             the name of the script being run
     * @param scriptParameters       the parameters of the script being run
     * @param pathToBuildFiles       the path to the directory containing the Dockerfile and other build files
     * @return the docker image
     */
    @Override
    public Image buildImage(final String scriptName, final Map<String, Object> scriptParameters,
                            final String pathToBuildFiles) {

        DockerClient docker = DockerClientSingleton.getInstance();

        // Convert the script parameters into a string
        String params = stringifyParameters(scriptParameters);
        String buildArgs = buildArguments(scriptName, params);
        DockerClient.BuildParam buildParam = null;

        // Build an image from the Dockerfile
        LOGGER.info("Building the image from the Dockerfile...");
        LOGGER.info("Path to build files: " + Paths.get(pathToBuildFiles).toString());
        try {
            buildParam = DockerClient.BuildParam.create("buildargs", buildArgs);
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
            LOGGER.error(e.getMessage());
        }
        return null;
    }

    /**
     * Builds the arguments string to build the Docker Client.
     *
     * @param scriptName - String name of the script to run.
     * @param params - String parameters to be passed to the script to be run.
     *
     * @return String the build argumnets string.
     */
    private String buildArguments(final String scriptName, final String params) {
        // Create the build arguments
        String retVal = "";

        StringBuilder buildargs = new StringBuilder();
        buildargs.append("{\"scriptName\":\"").append(scriptName).append("\",");
        buildargs.append("\"scriptParameters\":\"").append(params).append("\",");
        buildargs.append("\"modulesName\":\"").append(scriptName).append("Modules").append("\"}");
        LOGGER.info(String.valueOf(buildargs));

        try {
            retVal = URLEncoder.encode(String.valueOf(buildargs), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            LOGGER.error(e.getMessage());
        }

        return retVal;
    }

    /**
     * Copies the files to be used into the build directory.
     *
     * @param pathToBuildFiles       the path to the directory containing the Dockerfile and other build files
     * @param dockerfilePath         the path to the non-default dockerfile
     */
    public void getFiles(final String pathToBuildFiles, final String dockerfilePath) {
        String[] fileNames = new String[] {"DataInputStream.py", "entrypoint.py", "modules.txt"};
        // Copy the Dockerfile
        if (dockerfilePath.equals("")) {
            LOGGER.info("DockerfilePath unspecified, using default Dockerfile");
            createFile("Dockerfile", pathToBuildFiles, "/.ScriptBin/default/");
        } else {
            LOGGER.info("DockerfilePath specified, using non-default dockerfile");
            final String[] pathSplit = dockerfilePath.split("/");
            final String fileName = pathSplit[pathSplit.length - 1];
            final String fileLocation = dockerfilePath.substring(0, dockerfilePath.length() - fileName.length());
            createFile(fileName, pathToBuildFiles, fileLocation);
        }
        // Copy the rest of the files
        for (final String fileName : fileNames) {
            createFile(fileName, pathToBuildFiles, "/.ScriptBin/");
        }
    }

    /**
     * Copies a file from the given file location to the given destination
     *
     * @param fileName            the filename of the file to copy
     * @param destination         the destination of the file
     * @param fileLocation        the original location of the file
     */
    private void createFile(final String fileName, final String destination, final String fileLocation) {
        try (InputStream inputStream = StreamUtil.openStream(getClass(), fileLocation + fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String fileData = reader.lines().collect(Collectors.joining(System.lineSeparator()));
            inputStream.close();
            Files.write(Paths.get(destination + "/" + fileName), fileData.getBytes());
        } catch (final IOException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Converts the script parameters into a JSON string
     *
     * @param scriptParameters            the script parameters
     * @return the JSON string of script parameters
     */
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
