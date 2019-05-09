/*
 * Copyright 2016-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.binaryoperator;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HyperLogLogPlusAggregatorTest extends BinaryOperatorTest {
    private HyperLogLogPlus hyperLogLogPlus1;
    private HyperLogLogPlus hyperLogLogPlus2;

    @Before
    public void setup() {
        setupHllp(5, 5);
    }

    @Test
    public void shouldAggregateHyperLogLogPlusWithVariousPAndSpValues() {
        setupHllp(5, 5);
        shouldAggregateHyperLogLogPlus();

        setupHllp(5, 6);
        shouldAggregateHyperLogLogPlus();

        setupHllp(6, 6);
        shouldAggregateHyperLogLogPlus();
    }

    private void shouldAggregateHyperLogLogPlus() {
        HyperLogLogPlusAggregator hyperLogLogPlusAggregator = new HyperLogLogPlusAggregator();
        HyperLogLogPlus currentState = hyperLogLogPlus1;
        assertEquals(2L, currentState.cardinality());
        currentState = hyperLogLogPlusAggregator.apply(currentState, hyperLogLogPlus2);
        assertEquals(4L, currentState.cardinality());
    }

    @Test
    public void testClone() {
        assertEquals(new HyperLogLogPlusAggregator(), new HyperLogLogPlusAggregator());
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final HyperLogLogPlusAggregator aggregator = new HyperLogLogPlusAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.clearspring.cardinality.binaryoperator.HyperLogLogPlusAggregator\"%n" +
                "}"), json);

        // When 2
        final HyperLogLogPlusAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), HyperLogLogPlusAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return HyperLogLogPlusAggregator.class;
    }

    @Override
    protected HyperLogLogPlusAggregator getInstance() {
        return new HyperLogLogPlusAggregator();
    }

    private void setupHllp(final int p, final int sp) {
        hyperLogLogPlus1 = new HyperLogLogPlus(p, sp);
        hyperLogLogPlus1.offer("A");
        hyperLogLogPlus1.offer("B");

        hyperLogLogPlus2 = new HyperLogLogPlus(p, sp);
        hyperLogLogPlus2.offer("C");
        hyperLogLogPlus2.offer("D");
    }
}
