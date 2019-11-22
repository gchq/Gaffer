package uk.gov.gchq.gaffer.python.operation;

public interface Container {

    void start(? extends Platform platform);

    void sendData(String data, String port);

    String receiveData();

    void close();

}