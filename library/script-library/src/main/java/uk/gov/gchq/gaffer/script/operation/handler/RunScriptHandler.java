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
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class RunScriptHandler implements OperationHandler<RunScript> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunScriptHandler.class);
    private static final Integer TIMEOUT_200 = 200;
    private static final int MAX_BYTES = 65000;
    private static final int MAX_TRIES = 100;
    private ImagePlatform imagePlatform = LocalDockerPlatform.localDockerPlatform();
    private ScriptProvider scriptProvider = GitScriptProvider.gitScriptProvider();
    private String repoName;
    private String repoURI;


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

        try {
            // Pull or Clone the repo with the files
            scriptProvider.retrieveScripts(absoluteRepoPath.toString(), repoURI);
            // Build the image
            final Image image = imagePlatform.buildImage(scriptName, scriptParameters, pathToBuildFiles);
            // Create the container
            final Container container = imagePlatform.createContainer(image);
            // Run the container and return the result
            DataInputStream inputStream = container.receiveData();
            // Get the results back from the container
            Object results = receiveData(inputStream);
            // Close the container
            imagePlatform.closeContainer(container);

            return results;
        } catch (final Exception e) {
            LOGGER.error("Failed to run the script");
            throw new OperationException(e);
        }
    }

    public Object receiveData(DataInputStream inputStream) {
        // First get the length of the data coming from the container. Keep trying until the container is ready.
        LOGGER.info("Inputstream is: {}", inputStream);
        int incomingDataLength = getIncomingDataLength(inputStream);

        StringBuilder dataReceived = new StringBuilder();
        dataReceived = getDataReceived(incomingDataLength, dataReceived, inputStream);

        return dataReceived;
    }

    private StringBuilder getDataReceived(final int incomingDataLength,
                                          final StringBuilder dataReceived,
                                          final DataInputStream inputStream) {
        // If it failed to get the length of the incoming data then show the error, otherwise return the data.
        StringBuilder dataRecvd = dataReceived;

        if (incomingDataLength == 0) {
            LOGGER.info("Connection failed, stopping the container...");
        } else {
            try {
                // Get the data
                for (int i = 0; i < incomingDataLength / MAX_BYTES; i++) {
                    dataReceived.append(inputStream.readUTF());
                }
                dataReceived.append(inputStream.readUTF());
                dataRecvd = checkIfDataReceivedBeginsWithError(dataReceived);
            } catch (final IOException e) {
                LOGGER.error(e.getMessage());
            }
        }
        return dataRecvd;
    }

    private StringBuilder checkIfDataReceivedBeginsWithError(final StringBuilder dataReceived) {
        // Show the error message if the script failed and return no data
        StringBuilder dataRecvd = dataReceived;

        if (dataReceived.subSequence(0, 5) == "Error") {
            LOGGER.info(dataReceived.subSequence(5, dataReceived.length()).toString());
            dataRecvd = null;
        }
        return dataRecvd;
    }

    private int getIncomingDataLength(DataInputStream inputStream) {
        int incomingDataLength = 0;

        if (inputStream != null) {
            int tries = 0;
            while (tries < MAX_TRIES) {
                try {
                    incomingDataLength = inputStream.readInt();
                    LOGGER.info("Length of container...{}", incomingDataLength);
                    break;
                } catch (final IOException e) {
                    tries += 1;
                    LOGGER.error(e.getMessage());
                    sleep();
                }
            }
        }
        return incomingDataLength;
    }

    private void sleep() {
        try {
            Thread.sleep(TIMEOUT_200);
        } catch (final InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
    }

    private ImagePlatform getImagePlatform() {
        return imagePlatform;
    }

    public void setImagePlatform(final ImagePlatform imagePlatform) {
        this.imagePlatform = imagePlatform;
    }

    private ScriptProvider getScriptProvider() {
        return scriptProvider;
    }

    public void setScriptProvider(final ScriptProvider scriptProvider) {
        this.scriptProvider = scriptProvider;
    }

    private String getRepoName() {
        return repoName;
    }

    public void setRepoName(final String repoName) {
        this.repoName = repoName;
    }

    private String getRepoURI() {
        return repoURI;
    }

    public void setRepoURI(final String repoURI) {
        this.repoURI = repoURI;
    }
}
