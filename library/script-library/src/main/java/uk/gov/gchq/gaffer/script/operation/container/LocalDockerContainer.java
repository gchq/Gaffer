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

package uk.gov.gchq.gaffer.script.operation.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.script.operation.generator.RandomPortGenerator;
import uk.gov.gchq.gaffer.script.operation.util.DockerClientSingleton;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class LocalDockerContainer implements Container {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalDockerContainer.class);

    private static final String LOCALHOST = "127.0.0.1";
    private static final Integer TIMEOUT_100 = 100;
    private static final Integer MAX_TRIES = 100;

    private String containerId;
    private DataInputStream inputStream;
    private int port;

    public LocalDockerContainer(final String containerId, final int port) {
        this.containerId = containerId;
        this.port = port;
    }

    /**
     * Sends data to the docker container
     *
     * @param data             the data being sent
     */
    @Override
    public void sendData(final Iterable data) throws IOException {
        LOGGER.info("Attempting to connect with the container...");

        // The container will need some time to start up, so keep trying to connect and check
        // that its ready to receive data.
        IOException error = null;
        for (int i = 0; i < MAX_TRIES; i++) {
            try {
                // Connect to the container
                final Socket clientSocket = new Socket(LOCALHOST, port);
                LOGGER.info("Connected to container port at {}", clientSocket.getRemoteSocketAddress());

                // Check the container is ready
                inputStream = getInputStream(clientSocket);
                LOGGER.info("Container ready status: {}", inputStream.readBoolean());

                // Send the data
                OutputStream outToContainer = clientSocket.getOutputStream();
                final DataOutputStream outputStream = new DataOutputStream(outToContainer);
                boolean firstObject = true;
                for (final Object current : data) {
                    if (firstObject) {
                        outputStream.writeUTF("[" + new String(JSONSerialiser.serialise(current)));
                        firstObject = false;
                    } else {
                        outputStream.writeUTF(", " + new String(JSONSerialiser.serialise(current)));
                    }
                }
                outputStream.writeUTF("]");
                LOGGER.info("Sending data to docker container from {}", clientSocket.getLocalSocketAddress() + "...");
                outputStream.flush();
                error = null;
                break;
            } catch (final IOException e) {
                error = e;
                sleep();

            }
        }
        // Only print an error if it still fails after many tries
        if (error != null) {
            RandomPortGenerator.getInstance().releasePort(port);
            DockerClientSingleton.close();
            LOGGER.error(error.toString());
            LOGGER.error("Failed to send the data to the container");
            throw error;
        }
    }

    /**
     * Retrieves the length of the data being received from the container, then retrieves the data itself as a StringBuilder
     *
     * @return StringBuilder dataReceived
     */
    @Override
    public DataInputStream receiveData() {
        return inputStream;
    }

    @Override
    public String getContainerId() {
        return containerId;
    }

    @Override
    public int getPort() {
        return port;
    }

    private DataInputStream getInputStream(final Socket clientSocket) throws IOException {
        return new DataInputStream(clientSocket.getInputStream());
    }

    private void sleep() {
        try {
            Thread.sleep(LocalDockerContainer.TIMEOUT_100);
        } catch (final InterruptedException e) {
            LOGGER.error(e.toString());
        }
    }
}
