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
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class BuildImageFromDockerfile {
    private static final Logger LOGGER = LoggerFactory.getLogger(BuildImageFromDockerfile.class);

    /**
     * Builds docker image from Dockerfile
     * @param scriptName the name of the python script being run
     * @param scriptParameters the parameters of the script being run
     * @param scriptInputType the type of input for the script
     * @param docker the docker client the script is being run on
     * @param pathAbsolutePythonRepo the absolute path for the python repo
     * @return docker image from Dockerfile
     * @throws DockerException should the docker encounter an error, this will be thrown
     * @throws InterruptedException should this fail, this will be thrown
     * @throws IOException this will be thrown if non-compliant data is sent
     */
    public String buildImage(final String scriptName, final Map<String, Object> scriptParameters, final ScriptInputType scriptInputType, final DockerClient docker, final String pathAbsolutePythonRepo) throws DockerException, InterruptedException, IOException {
        // Build an image from the Dockerfile
        String params = " ";
        if (scriptParameters != null) {
            Map<String, String> map = new HashMap<>();

            for (final String current: scriptParameters.keySet()) {
                if (scriptParameters.get(current) != null) {
                    map.put(current, scriptParameters.get(current).toString());
                }
            }
            params = new Gson().toJson(map).replaceAll("\"", "'");
        }

        final String buildargs =
                "{\"scriptName\":\"" + scriptName + "\",\"scriptParameters\":\"" + params + "\"," +
                        "\"modulesName\":\"" + scriptName + "Modules" + "\",\"scriptInputType\":\"" + scriptInputType.toString() + "\"}";
        LOGGER.info(buildargs);
        final DockerClient.BuildParam buildParam = DockerClient.BuildParam.create("buildargs", URLEncoder.encode(buildargs, "UTF-8"));

        LOGGER.info("Building the image from the Dockerfile...");
        final AtomicReference<String> imageIdFromMessage = new AtomicReference<String>();
        LOGGER.info("Absolute Python repo path: " + Paths.get(pathAbsolutePythonRepo).toString());
        return docker.build(Paths.get(pathAbsolutePythonRepo + "/../"), "pythonoperation:" + scriptName, "Dockerfile", message -> {
            final String imageId = message.buildImageId();
            if (imageId != null) {
                imageIdFromMessage.set(imageId);
            }
            LOGGER.info(String.valueOf(message));
        }, buildParam);
    }

    public void getFiles(final String pathAbsolutePythonRepo) throws IOException {
        String[] fileNames = new String[] {"Dockerfile",
                                            "DataInputStream.py",
                                            "entrypoint.py",
                                            "modules.txt" };

        for (final String fileName : fileNames) {
            // Use the default file
            LOGGER.info("Using the default Dockerfile");
            InputStream inputStream = StreamUtil.openStream(getClass(), "/.PythonBin/" + fileName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String fileData = reader.lines().collect(Collectors.joining(System.lineSeparator()));
            inputStream.close();
            Files.write(Paths.get(pathAbsolutePythonRepo + "/../" + fileName), fileData.getBytes());
        }
    }
}
