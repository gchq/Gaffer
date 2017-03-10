/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.function.aggregate;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.bifunction.BiFunctionTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MinTest extends BiFunctionTest {
    private Comparable state;

    @Before
    public void before() {
        state = null;
    }

    @Test
    public void testAggregateInIntMode() {
        // Given
        final Min min = new Min();

        // When 1
        state = min.apply(state, 1);

        // Then 1
        assertTrue(state instanceof Integer);
        assertEquals(1, state);

        // When 2
        state = min.apply(state, 2);

        // Then 2
        assertTrue(state instanceof Integer);
        assertEquals(1, state);

        // When 3
        state = min.apply(state, 3);

        // Then 3
        assertTrue(state instanceof Integer);
        assertEquals(1, state);
    }

    @Test
    public void testAggregateInLongMode() {
        // Given
        final Min min = new Min();

        // When 1
        state = min.apply(state, 1l);

        // Then 1
        assertTrue(state instanceof Long);
        assertEquals(1l, state);

        // When 2
        state = min.apply(state, 2l);

        // Then 2
        assertTrue(state instanceof Long);
        assertEquals(1l, state);

        // When 3
        state = min.apply(state, 3l);

        // Then 3
        assertTrue(state instanceof Long);
        assertEquals(1l, state);
    }

    @Test
    public void testAggregateInDoubleMode() {
        // Given
        final Min min = new Min();


        // When 1
        state = min.apply(state, 2.1d);

        // Then 1
        assertTrue(state instanceof Double);
        assertEquals(2.1d, state);

        // When 2
        state = min.apply(state, 1.1d);

        // Then 2
        assertTrue(state instanceof Double);
        assertEquals(1.1d, state);

        // When 3
        state = min.apply(state, 3.1d);

        // Then 3
        assertTrue(state instanceof Double);
        assertEquals(1.1d, state);
    }

    @Test
    public void testAggregateMixedInput() {
        // Given
        final Min min = new Min();

        // When 1
        state = min.apply(state, 5);

        // When 2
        try {
            state = min.apply(state, 2l);
            fail();
        } catch (ClassCastException cce) {
        }

        // When 3
        try {
            state = min.apply(state, 2.1d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(state instanceof Integer);
        assertEquals(5, state);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Min aggregator = new Min();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.Min\"%n" +
                "}"), json);

        // When 2
        final Min deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), Min.class);

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Min getInstance() {
        return new Min();
    }

    @Override
    protected Class<Min> getFunctionClass() {
        return Min.class;
    }
}
