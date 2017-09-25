/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.util;

import org.junit.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AggregatePairTest extends JSONSerialisationTest<AggregatePair> {
    @Override
    protected AggregatePair getTestObject() {
        return new AggregatePair();
    }

    @Test
    public void shouldSetAndGetProperties() {
        // Given
        final AggregatePair pair = new AggregatePair();
        pair.setGroupBys(new String[] {"timestamp"});
        pair.setElementAggregator(new ElementAggregator());

        // When / Then
        assertArrayEquals(new String[]{"timestamp"}, pair.getGroupBys());
        assertNotNull(pair.getElementAggregator());
    }

    @Test
    public void shouldCreateObjectFromConstructorsCorrectly() {
        // Given
        final AggregatePair pair = new AggregatePair(new String[]{"timestamp"});
        final AggregatePair pair1 = new AggregatePair(new ElementAggregator());

        // When / Then
        assertTrue(null == pair.getElementAggregator());
        assertTrue(null == pair1.getGroupBys());
    }
}
