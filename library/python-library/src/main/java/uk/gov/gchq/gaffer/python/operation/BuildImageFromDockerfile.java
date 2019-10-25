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

import java.io.*;
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
        System.out.println("Absolute Python repo path: " + Paths.get(pathAbsolutePythonRepo + "/../").toString());
        return docker.build(Paths.get(pathAbsolutePythonRepo + "/../"), "pythonoperation:" + scriptName, "Dockerfile", message -> {
            final String imageId = message.buildImageId();
            if (imageId != null) {
                imageIdFromMessage.set(imageId);
            }
            LOGGER.info(String.valueOf(message));
        }, buildParam);
    }

    public void getFiles(final String pathAbsolutePythonRepo) throws IOException {

        // TODO: Replace with a loop over the files in the resources directory
        String[] fileNames = new String[4];
        fileNames[0] = "Dockerfile";
        fileNames[1] = "DataInputStream.py";
        fileNames[2] = "entrypoint.py";
        fileNames[3] = "modules.txt";

        for (int i = 0; i <= 3; i++) {
            // If alternative file provided
            if (false) {
                // Load the file
            } else {
                // Use the default file
                InputStream inputStream = StreamUtil.openStream(getClass(),"/.PythonBin/" + fileNames[i]);
                LOGGER.info("Dockerfile inputstream is: " + inputStream.toString());
                String fileData = null;
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    fileData = reader.lines().collect(Collectors.joining(System.lineSeparator()));
                }
                LOGGER.info("Dockerfile data is: " + fileData);
                System.out.println("Dockerfile data is: " + fileData);
                Files.write(Paths.get(pathAbsolutePythonRepo + "/../" + fileNames[i]), fileData.getBytes());
            }
        }
    }
}
