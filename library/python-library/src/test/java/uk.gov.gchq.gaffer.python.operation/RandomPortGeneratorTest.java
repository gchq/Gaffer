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

import java.util.ArrayList;

public class RandomPortGeneratorTest {

    @Test
    public void shouldGetAPortWithinRange() {
        // Given
        int portResult = 0;

        // When
        portResult = RandomPortGenerator.getInstance().generatePort();

        // Then
        Assert.assertTrue(portResult < PythonTestConstants.MAX_PORT && portResult > PythonTestConstants.MIN_PORT);
    }

    @Test
    public void shouldReturnMultiplePortsWithinRange() {
        // Given
        ArrayList<Integer> portResults = new ArrayList<>();

        // When
        for (int i = 0; i < 100; i++) {
            portResults.add(RandomPortGenerator.getInstance().generatePort());
        }

        // Then
        for (int i = 0; i < 100; i++) {
            int currentPort = portResults.get(i);
            Assert.assertTrue(currentPort < PythonTestConstants.MAX_PORT && currentPort > PythonTestConstants.MIN_PORT);
        }
    }
}
