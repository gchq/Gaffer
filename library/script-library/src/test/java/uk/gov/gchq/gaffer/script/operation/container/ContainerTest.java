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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.script.operation.ScriptTestConstants;
import uk.gov.gchq.gaffer.script.operation.handler.RunScriptHandler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class ContainerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerTest.class);

    @Test
    public void shouldCreateContainer() {
        // Given
        setupTestServer();

        LocalDockerContainer localDockerContainer = new LocalDockerContainer("", ScriptTestConstants.TEST_SERVER_PORT_3);
        ArrayList<String> inputData = new ArrayList<>();
        inputData.add("Test Data");

        // When
        StringBuilder result = null;
        try {
            localDockerContainer.sendData(inputData);
            result = (StringBuilder) new RunScriptHandler().receiveData(localDockerContainer.receiveData());
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }

        // Then
        assert result != null;
        Assert.assertEquals("Test Complete", result.toString());
    }

    private void setupTestServer() {
        Runnable serverTask = () -> {
            try (ServerSocket serverSocket = new ServerSocket(ScriptTestConstants.TEST_SERVER_PORT_3)) {
                System.out.println("Waiting for clients to connect...");
                System.out.println("Client connected.");
                try (Socket clientSocket = serverSocket.accept();
                     DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
                     DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream())) {
                    dos.writeBoolean(true);
                    dos.flush();
                    dis.readUTF();
                    dis.readUTF();
                    dos.writeInt(0);
                    dos.writeUTF("Test Complete");
                    System.out.println("Closing Socket.");
                    dos.flush();
                } catch (IOException e) {
                    System.err.println("Unable to process client request");
                    System.out.println("Unable to process client request");
                    e.printStackTrace();
                }
            } catch (IOException e) {
                LOGGER.error("Unable to process client request");
                LOGGER.info("Unable to process client request");
                LOGGER.error(e.getMessage());
            }
        };
        final Thread serverThread = new Thread(serverTask);
        serverThread.start();
    }
}
