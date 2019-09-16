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

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GetPort {
    public GetPort() {
    }

    /**
     * @return a random port number
     */
    public String getPort() {
        List<Integer> portsList = IntStream.rangeClosed(50000, 65535).boxed().collect(Collectors.toList());
        Random rand = new Random();
        Integer portNum = portsList.get(rand.nextInt(portsList.size()));
        return String.valueOf(portNum);
    }
}
