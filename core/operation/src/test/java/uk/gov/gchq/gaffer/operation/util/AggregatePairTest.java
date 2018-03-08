/*
 * Copyright 2017-2018 Crown Copyright
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AggregatePairTest extends JSONSerialisationTest<AggregatePair> {
    @Override
    protected AggregatePair getTestObject() {
        return new AggregatePair();
    }

    @Test
    public void shouldSetAndGetProperties() {
        // Given
        final AggregatePair pair = new AggregatePair();
        pair.setGroupBy(new String[] {"timestamp"});
        pair.setElementAggregator(new ElementAggregator());

        // When / Then
        assertArrayEquals(new String[]{"timestamp"}, pair.getGroupBy());
        assertNotNull(pair.getElementAggregator());
    }

    @Test
    public void shouldCreateObjectFromConstructorsCorrectly() {
        // Given
        final String[] groupBy = new String[] {"timestamp"};
        final ElementAggregator elementAggregator = new ElementAggregator();

        final AggregatePair pair = new AggregatePair(groupBy);
        final AggregatePair pair1 = new AggregatePair(elementAggregator);

        // When / Then

        assertNull(pair.getElementAggregator());
        assertNull(pair1.getGroupBy());
        assertArrayEquals(pair.getGroupBy(), groupBy);
        assertEquals(pair1.getElementAggregator(), elementAggregator);
    }
}
