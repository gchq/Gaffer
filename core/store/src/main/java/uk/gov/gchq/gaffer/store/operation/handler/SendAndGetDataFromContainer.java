package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.PythonOperation;

import java.io.*;
import java.net.Socket;

public class SendAndGetDataFromContainer {
    public SendAndGetDataFromContainer() {
    }

    /**
     * Sends data to and gets data from container
     */
    static DataInputStream sendAndGetData(PythonOperation operation, Socket clientSocket) throws IOException {
        // Send the data
        System.out.println("Sending data to docker container from " + clientSocket.getLocalSocketAddress() + "...");
        OutputStream outToContainer = clientSocket.getOutputStream();
        DataOutputStream out = new DataOutputStream(outToContainer);
        boolean firstObject = true;
        for (Object current : operation.getInput()) {
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
        System.out.println("Waiting for response from Container...");
        // Get the data from the container
        InputStream inFromContainer = clientSocket.getInputStream();
        return new DataInputStream(inFromContainer);
    }
}