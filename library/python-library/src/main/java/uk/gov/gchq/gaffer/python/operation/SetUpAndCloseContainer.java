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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

public class SetUpAndCloseContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SetUpAndCloseContainer.class);

    public SetUpAndCloseContainer() {
    }

    /**
     * @param operation the RunPythonScript operation
     * @param port the port of the docker client where the data will be passed
     * @return Sets up and closes container
     * @throws InterruptedException should this fail, this will be thrown
     * @throws IOException this will be thrown if non-compliant data is sent
     */
    public StringBuilder setUpAndCloseContainer(final RunPythonScript operation, final String port) throws InterruptedException, IOException {
        // Keep trying to connect to container and give the container some time to load up
        boolean failedToConnect = true;
        IOException error = null;
        Socket clientSocket = null;
        DataInputStream in = null;
        Thread.sleep(1000);
        LOGGER.info("Attempting to connect with the container...");
        for (int i = 0; i < 100; i++) {
            try {
                clientSocket = new Socket("127.0.0.1", Integer.parseInt(port));
                LOGGER.info("Connected to container port at {}", clientSocket.getRemoteSocketAddress());
                in = SendAndGetDataFromContainer.getInputStream(clientSocket);
                LOGGER.info("Container ready status: {}", in.readBoolean());
                SendAndGetDataFromContainer.sendData(operation, clientSocket);
                break;
            } catch (final IOException e) {
                LOGGER.info(e.getMessage());
                error = e;
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }
        LOGGER.info("clientSocket is: {}", clientSocket);
        LOGGER.info("In is: {}", in);
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
//            docker.stopContainer(containerId, 1); // Kill the container after 1 second
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
