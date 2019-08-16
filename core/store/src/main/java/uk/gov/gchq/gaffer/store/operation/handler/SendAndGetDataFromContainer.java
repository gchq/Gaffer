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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.RunPythonScript;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class SendAndGetDataFromContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendAndGetDataFromContainer.class);

    public SendAndGetDataFromContainer() {
    }

    /**
     * Sends data to and gets data from container
     */
    static DataInputStream sendAndGetData(final RunPythonScript operation, final Socket clientSocket) throws IOException {
        // Send the data
        LOGGER.info("Sending data to docker container from {}", clientSocket.getLocalSocketAddress() + "...");
        OutputStream outToContainer = clientSocket.getOutputStream();
        DataOutputStream out = new DataOutputStream(outToContainer);
        boolean firstObject = true;
        for (final Object current : operation.getInput()) {
            if (firstObject) {
                out.writeUTF("[" + new String(JSONSerialiser.serialise(current)));
                firstObject = false;
            } else {
                out.writeUTF(", " + new String(JSONSerialiser.serialise(current)));
            }
        }
        out.writeUTF("]");
        out.flush();
        //out.writeUTF(dataToSend);
        LOGGER.info("Waiting for response from Container...");
        // Get the data from the container
        InputStream inFromContainer = clientSocket.getInputStream();
        return new DataInputStream(inFromContainer);
    }
}
