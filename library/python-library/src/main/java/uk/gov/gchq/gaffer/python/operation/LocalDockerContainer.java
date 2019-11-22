package uk.gov.gchq.gaffer.python.operation;

public class LocalDockerContainer implements Container {
    private String containerId;

    LocalDockerContainer(String containerId) {
        this.containerId = containerId;
    }

    @Override
    public void start(LocalDockerPlatform docker) {
        docker.startContainer(this.containerId);
    }

    @Override
    public void sendData(String data, String port) {

    }

    @Override
    public String receiveData() {
        return null;
    }

    @Override
    public void close() {

    }
}
