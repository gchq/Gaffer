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
    public void shouldRetrieveInputStream() {
        // Given
        setupTestServer();
        Socket testSocket = null;
        try {
            testSocket = new Socket("localhost", 7789);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // When
        DataInputStream dis = null;
        try {
            assert testSocket != null;
            dis = SendAndGetDataFromContainer.getInputStream(testSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Then
        Assert.assertNotNull(dis);
    }

    @Test
    public void shouldSendDataToServer() {
        // Given
        setupTestServer();
        final RunPythonScript<String, String> runPythonScript =
                new RunPythonScript.Builder<String, String>()
                        .build();
        ArrayList<String> inputData = new ArrayList<>();
        inputData.add("Test Data 1");
        inputData.add("Test Data 2");
        runPythonScript.setInput(inputData);
        Socket testSocket = null;
        try {
            testSocket = new Socket("localhost", 7789);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // When
        try {
            SendAndGetDataFromContainer.sendData(runPythonScript, testSocket);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // Then
        try {
            DataInputStream dis = new DataInputStream(testSocket.getInputStream());
            Assert.assertEquals("[\"Test Data 1\"", dis.readUTF());
            Assert.assertEquals(", \"Test Data 2\"", dis.readUTF());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void setupTestServer() {
        Runnable serverTask = () -> {
            try {
                ServerSocket serverSocket = new ServerSocket(7789);
                System.out.println("Waiting for clients to connect...");
                Socket clientSocket = serverSocket.accept();
                DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
                while(true) {
                    dos.writeUTF(dis.readUTF());
                }
            } catch (IOException e) {
                System.err.println("Unable to process client request");
                e.printStackTrace();
            }
        };
        Thread serverThread = new Thread(serverTask);
        serverThread.start();
    }
}
