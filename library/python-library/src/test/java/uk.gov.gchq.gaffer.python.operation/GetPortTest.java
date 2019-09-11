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

public class GetPortTest {

    @Test
    public void shouldGetAPortWithinRange() {
        //Given
        GetPort gp = new GetPort();

        //When
        int portResult = Integer.parseInt(gp.getPort());

        //Then
        Assert.assertTrue(portResult < 65535 && portResult > 50000);
    }

    @Test
    public void shouldReturnMultiplePortsWithinRange() {
        //Given
        GetPort gp = new GetPort();

        //When
        ArrayList<Integer> portResults = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            portResults.add(Integer.parseInt(gp.getPort()));
        }

        //Then
        for (int i = 0; i < 100; i++) {
            int currentPort = portResults.get(i);
            Assert.assertTrue(currentPort < 65535 && currentPort > 50000);
        }
    }
}
