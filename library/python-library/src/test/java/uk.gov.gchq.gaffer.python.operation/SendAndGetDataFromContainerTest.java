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

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class SendAndGetDataFromContainerTest {

    @Test
    public void shouldCreateContainer() {
        // Given
        setupTestServer();
        SendAndGetDataFromContainer sUACC = new SendAndGetDataFromContainer();
        final RunPythonScript<String, String> runPythonScript =
                new RunPythonScript.Builder<String, String>()
                        .build();
        ArrayList<String> inputData = new ArrayList<>();
        inputData.add("Test Data");
        runPythonScript.setInput(inputData);

        // When
        StringBuilder result = null;
        try {
            result = sUACC.setUpAndCloseContainer(runPythonScript, "7790");
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

        // Then
        assert result != null;
        Assert.assertEquals("Test Complete", result.toString());
    }

    private void setupTestServer() {
        Runnable serverTask = () -> {
            ServerSocket serverSocket;
            try {
                serverSocket = new ServerSocket(7790);
                System.out.println("Waiting for clients to connect...");
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected.");
                DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
                dos.writeBoolean(true);
                dis.readUTF();
                dos.writeInt(1);
                dos.writeUTF("Test Complete");
                serverSocket.close();
                System.out.println("Closing Socket.");
            } catch (IOException e) {
                System.err.println("Unable to process client request");
                e.printStackTrace();
            }
        };
        final Thread serverThread = new Thread(serverTask);
        serverThread.start();
    }
}
