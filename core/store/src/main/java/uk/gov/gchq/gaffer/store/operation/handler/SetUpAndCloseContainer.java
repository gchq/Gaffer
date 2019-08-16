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

package uk.gov.gchq.gaffer.store.operation.handler;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.operation.RunPythonScript;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

class SetUpAndCloseContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SetUpAndCloseContainer.class);

    SetUpAndCloseContainer() {
    }

    /**
     * Sets up and closes container
     */
    StringBuilder setUpAndCloseContainer(final RunPythonScript operation, final DockerClient docker, final String port, final String containerId) throws InterruptedException, DockerException, IOException {
        // Keep trying to connect to container and give the container some time to load up
        boolean failedToConnect = true;
        IOException error = null;
        Socket clientSocket = null;
        DataInputStream in = null;
        LOGGER.info("Attempting to send data to container...");
        for (int i = 0; i < 100; i++) {
            try {
                clientSocket = new Socket("127.0.0.1", Integer.parseInt(port));
                LOGGER.info("Connected to container port at {}", clientSocket.getRemoteSocketAddress());
                in = SendAndGetDataFromContainer.sendAndGetData(operation, clientSocket);

                LOGGER.info("Container ready status: {}", in.readBoolean());
                break;
            } catch (final IOException e) {
                LOGGER.info("Failed to send data.");
                error = e;
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }
        LOGGER.info("In is: {}", in);
        LOGGER.info("clientSocket is: {}", clientSocket);
        int incomingDataLength = 0;
        if (clientSocket != null && in != null) {
            int timeout = 0;
            while (timeout < 100) {
                try {
                    // Get the data from the container
                    incomingDataLength = in.readInt();
                    LOGGER.info("Length of container...{}", incomingDataLength);
                    failedToConnect = false;
                    break;
                } catch (final IOException e) {
                    timeout += 1;
                    error = e;
                    TimeUnit.MILLISECONDS.sleep(200);
                }
            }
        }
        StringBuilder dataReceived = new StringBuilder();
        if (failedToConnect) {
            LOGGER.info("Connection failed, stopping the container...");
            error.printStackTrace();
            docker.stopContainer(containerId, 1); // Kill the container after 1 second
        } else {
            for (int i = 0; i < incomingDataLength / 65000; i++) {
                dataReceived.append(in.readUTF());
            }
            dataReceived.append(in.readUTF());
            clientSocket.close();
        }
        return dataReceived;
    }
}
