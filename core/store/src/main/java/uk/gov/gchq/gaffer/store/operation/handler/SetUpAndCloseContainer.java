package uk.gov.gchq.gaffer.store.operation.handler;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import uk.gov.gchq.gaffer.operation.PythonOperation;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

public class SetUpAndCloseContainer {
    private final PythonOperationHandler pythonOperationHandler;

    public SetUpAndCloseContainer(PythonOperationHandler pythonOperationHandler) {
        this.pythonOperationHandler = pythonOperationHandler;
    }

    /**
     * Sets up and closes container
     */
    StringBuilder setUpAndCloseContainer(PythonOperation operation, DockerClient docker, String port, String containerId) throws InterruptedException, DockerException, IOException {
        // Keep trying to connect to container and give the container some time to load up
        boolean failedToConnect = true;
        IOException error = null;
        Socket clientSocket = null;
        DataInputStream in = null;
        System.out.println("Attempting to send data to container...");
        for (int i = 0; i < 100; i++) {
            try {
                clientSocket = new Socket("127.0.0.1", Integer.parseInt(port));
                System.out.println("Connected to container port at " + clientSocket.getRemoteSocketAddress());
                in = pythonOperationHandler.sendAndGetData(operation, clientSocket);

                System.out.println("Container ready status: " + in.readBoolean());
                break;
            } catch (final IOException e) {
                System.out.println("Failed to send data.");
                error = e;
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }
        System.out.println("In is: " + in);
        System.out.println("clientSocket is: " + clientSocket);
        int incomingDataLength = 0;
        if (clientSocket != null && in != null) {
            int timeout = 0;
            while (timeout < 100) {
                try {
                    // Get the data from the container
                    incomingDataLength = in.readInt();
                    System.out.println("Length of container..." + incomingDataLength);
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
            System.out.println("Connection failed, stopping the container...");
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