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
import java.util.concurrent.TimeoutException;

public class RunScriptHandler implements OperationHandler<RunScript> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunScriptHandler.class);
    private static final int TIMEOUT_100 = 100;
    private static final int TIMEOUT_200 = 200;
    private static final int MAX_BYTES = 65000;
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
            // Run the container
            imagePlatform.runContainer(container, operation.getInput());
            // Get the input stream
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

    /**
     * Gets the data from the container.
     *
     * @param inputStream           the input stream from the container
     * @return the data from the container
     * @throws TimeoutException     if it times out getting the data from the container
     * @throws IOException          if it fails to get the data from the container
     */
    public Object receiveData(final DataInputStream inputStream) throws TimeoutException, IOException {
        // Get the length of the data coming from the container.
        LOGGER.info("Inputstream is: {}", inputStream);
        Reader inputStreamReader = new InputStreamReader(inputStream);
        int incomingDataLength = getIncomingDataLength(inputStreamReader);

        // Get the data from the container
        StringBuilder dataReceived = getDataReceived(incomingDataLength, inputStreamReader);

        // Close the connection
        try {
            inputStream.close();
        } catch (final IOException e) {
            LOGGER.info(e.toString());
            LOGGER.info("Failed to close the input stream from the container");
        }

        return dataReceived;
    }

    /**
     * Gets the data from the container. Checks if any errors occurred within the container itself.
     *
     * @param incomingDataLength            The length of the data from the container
     * @param inputStreamReader             the input stream reader
     * @return the data from the container
     * @throws IOException                  if it fails to get the data
     */
    private StringBuilder getDataReceived(final int incomingDataLength,
                                          final Reader inputStreamReader) throws IOException {
        // Get the data from the container
        StringBuilder dataReceived = new StringBuilder();
        try {
            char[] charBuffer = new char[incomingDataLength];
            inputStreamReader.read(charBuffer, 0, incomingDataLength);
            dataReceived.append(charBuffer);
            LOGGER.info("Data received is: " + dataReceived);
        } catch (final IOException e) {
            LOGGER.error(e.toString());
            LOGGER.error("Error reading the data from the container");
            throw e;
        }
        return checkForScriptError(dataReceived);
    }

    /**
     * Checks if there is a error whilst running the script.
     *
     * @param dataReceived          the data received from the container
     * @return the data from the container or the error message.
     */
    private StringBuilder checkForScriptError(final StringBuilder dataReceived) {
        // Show the error message if the script failed and return no data
        StringBuilder dataRecvd = dataReceived;
        if (dataReceived.subSequence(0, 5) == "Error") {
            LOGGER.info(dataReceived.subSequence(5, dataReceived.length()).toString());
            dataRecvd = null;
        }
        return dataRecvd;
    }

    /**
     * Gets the length of the data coming from the container
     *
     * @param inputStreamReader          the input stream reader
     * @return the length of the data
     * @throws TimeoutException          if it times out getting the length of the data
     * @throws IOException               if it fails to get the length of the data
     */
    private int getIncomingDataLength(final Reader inputStreamReader) throws TimeoutException, IOException {
        // Keep trying to get the length of the data until the container is ready.
        int incomingDataLength = 0;
        if (inputStreamReader != null) {
            long start = System.currentTimeMillis();
            boolean timedOut = true;
            IOException error = null;
            while (System.currentTimeMillis() - start < TIMEOUT_100) {
                try {
                    if (inputStreamReader.ready()) {
                        timedOut = false;
                        break;
                    }
                } catch (final IOException e) {
                    error = e;
                }
            }
            if (!timedOut) {
                try {
                    while (incomingDataLength == 0) {
                        int asciiCode = inputStreamReader.read();
                        if (asciiCode != 0) {
                            incomingDataLength = asciiCode;
                        }
                    }
                } catch (final IOException e) {
                    LOGGER.error(e.toString());
                    LOGGER.error("Failed to read from input stream of the container");
                    throw e;
                }
            } else {
                // timed out
                if (error != null) {
                    LOGGER.error(error.toString());
                    LOGGER.error("Error checking the input stream of the container is ready");
                    throw error;
                } else {
                    TimeoutException err = new TimeoutException("Timed out waiting for the input stream");
                    LOGGER.error(error.toString());
                    LOGGER.error("Timed out checking the input stream is ready");
                    throw err;
                }
            }
        }
        return incomingDataLength;
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
