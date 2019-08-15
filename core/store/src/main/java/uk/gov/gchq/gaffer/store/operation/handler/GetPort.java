package uk.gov.gchq.gaffer.store.operation.handler;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GetPort {
    public GetPort() {
    }

    /**
     * Get a random port number
     */
    String getPort() {
        List<Integer> portsList = IntStream.rangeClosed(50000, 65535).boxed().collect(Collectors.toList());
        Random rand = new Random();
        Integer portNum = portsList.get(rand.nextInt(portsList.size()));
        return String.valueOf(portNum);
    }
}