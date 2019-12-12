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
package uk.gov.gchq.gaffer.script.operation.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.script.operation.RunScript;
import uk.gov.gchq.gaffer.script.operation.container.Container;
import uk.gov.gchq.gaffer.script.operation.image.Image;
import uk.gov.gchq.gaffer.script.operation.platform.ImagePlatform;
import uk.gov.gchq.gaffer.script.operation.platform.LocalDockerPlatform;
import uk.gov.gchq.gaffer.script.operation.provider.GitScriptProvider;
import uk.gov.gchq.gaffer.script.operation.provider.ScriptProvider;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class RunScriptHandler implements OperationHandler<RunScript> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunScriptHandler.class);
    private static final Integer TIMEOUT_100 = 100;
    private static final Integer TIMEOUT_200 = 200;
    private static final Integer MAX_BYTES = 65000;

    private ImagePlatform imagePlatform = new LocalDockerPlatform();
    private ScriptProvider scriptProvider = new GitScriptProvider();
    private String repoName = "test";
    private String repoURI = "https://github.com/g609bmsma/test";
    private String ip = "127.0.0.1";

    @Override
    public Object doOperation(final RunScript operation, final Context context, final Store store) throws OperationException {

        final String scriptName = operation.getScriptName();
        final Map<String, Object> scriptParameters = operation.getScriptParameters();
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String pathToBuildFiles = currentWorkingDirectory.concat("/src/main/resources/" + ".ScriptBin");
        final Path absoluteRepoPath = Paths.get(pathToBuildFiles, repoName);
        final File directory = new File(pathToBuildFiles);
        if (!directory.exists()) {
            directory.mkdir();
        }

        // Pull or Clone the repo with the files
        scriptProvider.getScripts(absoluteRepoPath.toString(), repoURI);
        // Build the image
        final Image image = imagePlatform.buildImage(scriptName, scriptParameters, pathToBuildFiles);
        // Create the container
        final Container container = imagePlatform.createContainer(image, ip);
        // Run the container
        imagePlatform.runContainer(container, operation.getInput());
        // Get the input stream
        DataInputStream inputStream = container.receiveData();
        // Get the results back from the container
        Object results = receiveData(inputStream);
        // Close the container
        imagePlatform.closeContainer(container);

        return results;
    }

    private Object receiveData(final DataInputStream inputStream) {

        // First get the length of the data coming from the container. Keep trying until the container is ready.
        LOGGER.info("Inputstream is: {}", inputStream);
        Reader inputStreamReader = new InputStreamReader(inputStream);
        int incomingDataLength = 0;
        Exception error = null;
        if (inputStream != null) {
            int tries = 0;
            while (tries < TIMEOUT_100) {
                try {
                    int asciiCode = inputStreamReader.read();
                    if (asciiCode != 0) {
                        incomingDataLength = asciiCode;
                        LOGGER.info("Length of container...{}", incomingDataLength);
                        error = null;
                        break;
                    } else {
                       throw new NullPointerException("No data length found");
                    }
                } catch (final IOException | NullPointerException e) {
                    tries += 1;
                    error = e;
                    sleep(TIMEOUT_200);
                }
            }
        }

        // If it failed to get the length of the incoming data then show the error, otherwise return the data.
        StringBuilder dataReceived = new StringBuilder();
        if (null != error) {
            LOGGER.info("Connection failed, stopping the container...");
            error.printStackTrace();
        } else {
            try {
                // Get the data
                char[] charBuffer = new char[incomingDataLength];
                inputStreamReader.read(charBuffer, 0, incomingDataLength);
                dataReceived.append(charBuffer);
                // Show the error message if the script failed and return no data
                System.out.println("dataReceived is" + dataReceived);
                if (dataReceived.subSequence(0, 5) == "Error") {
                    LOGGER.info(dataReceived.subSequence(5, dataReceived.length()).toString());
                    dataReceived = null;
                }
            } catch (final IOException e) {
                LOGGER.info(e.getMessage());
            }
        }
        // Close the connection
        try {
            if (inputStream != null) {
                inputStream.close();
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }

        return dataReceived;
    }

    private void sleep(final Integer time) {
        try {
            Thread.sleep(time);
        } catch (final InterruptedException e) {
            LOGGER.info(e.getMessage());
        }
    }

    private ImagePlatform getImagePlatform() {
        return imagePlatform;
    }

    private void setImagePlatform(final ImagePlatform imagePlatform) {
        this.imagePlatform = imagePlatform;
    }

    private ScriptProvider getScriptProvider() {
        return scriptProvider;
    }

    private void setScriptProvider(final ScriptProvider scriptProvider) {
        this.scriptProvider = scriptProvider;
    }

    private String getRepoName() {
        return repoName;
    }

    private void setRepoName(final String repoName) {
        this.repoName = repoName;
    }

    private String getRepoURI() {
        return repoURI;
    }

    private void setRepoURI(final String repoURI) {
        this.repoURI = repoURI;
    }

    private String getIp() {
        return ip;
    }

    private void setIp(final String ip) {
        this.ip = ip;
    }
}
