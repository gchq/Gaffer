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

package uk.gov.gchq.gaffer.script.operation.generator;

import uk.gov.gchq.koryphe.Summary;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * <p>
 * A {@code RandomPortGenerator} gets a random port number from a predefined range.
 * </p>
 */
@Summary("A port generator that returns a random port number")
public final class RandomPortGenerator implements PortGenerator {
    private static volatile RandomPortGenerator portGenerator;
    private static final Integer MIN_PORT_NUM = 50000;
    private static final Integer MAX_PORT_NUM = 65535;
    private static ArrayList<Integer> usedPorts = new ArrayList<>();

    private RandomPortGenerator() { }

    public static RandomPortGenerator getInstance() {
        // Don't wait for other threads if the instance is available
        if (portGenerator == null) {
            // Synchronize the creation of the port generator
            synchronized (RandomPortGenerator.class) {
                // Only create a port generator if one already exists
                if (portGenerator == null) {
                    portGenerator = new RandomPortGenerator();
                }
            }
        }
        return portGenerator;
    }

    public synchronized Integer generatePort() {
        List<Integer> portsList = IntStream.rangeClosed(MIN_PORT_NUM, MAX_PORT_NUM)
                .filter(num -> !usedPorts.contains(num))
                .boxed()
                .collect(Collectors.toList());
        Random rand = new Random();
        Integer portNum = portsList.get(rand.nextInt(portsList.size()));
        usedPorts.add(portNum);
        return portNum;
    }

    public synchronized void freePort(final Integer port) {
        usedPorts.remove(port);
    }
}
