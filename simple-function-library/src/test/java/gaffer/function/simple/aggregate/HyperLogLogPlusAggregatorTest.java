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
package gaffer.function.simple.aggregate;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import gaffer.exception.SerialisationException;
import gaffer.function.ConsumerProducerFunction;
import gaffer.function.ConsumerProducerFunctionTest;
import gaffer.function.Function;
import gaffer.jsonserialisation.JSONSerialiser;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class HyperLogLogPlusAggregatorTest extends ConsumerProducerFunctionTest {
    private HyperLogLogPlus hyperLogLogPlus1;
    private HyperLogLogPlus hyperLogLogPlus2;

    @Before
    public void setup() {
        hyperLogLogPlus1 = new HyperLogLogPlus(5, 5);
        hyperLogLogPlus1.offer("A");
        hyperLogLogPlus1.offer("B");

        hyperLogLogPlus2 = new HyperLogLogPlus(5, 5);
        hyperLogLogPlus2.offer("C");
        hyperLogLogPlus2.offer("D");
    }

    @Test
    public void testExecute() {
        HyperLogLogPlusAggregator hyperLogLogPlusAggregator = new HyperLogLogPlusAggregator();
        hyperLogLogPlusAggregator.init();
        assertEquals(0l, ((HyperLogLogPlus) hyperLogLogPlusAggregator.state()[0]).cardinality());
        hyperLogLogPlusAggregator._aggregate(hyperLogLogPlus1);
        assertEquals(2l, ((HyperLogLogPlus) hyperLogLogPlusAggregator.state()[0]).cardinality());
        hyperLogLogPlusAggregator._aggregate(hyperLogLogPlus2);
        assertEquals(4l, ((HyperLogLogPlus) hyperLogLogPlusAggregator.state()[0]).cardinality());
    }

    @Test
    public void testFailedExecuteDueToNullInput() {
        HyperLogLogPlusAggregator hyperLogLogPlusAggregator = new HyperLogLogPlusAggregator();
        hyperLogLogPlusAggregator.init();
        hyperLogLogPlusAggregator._aggregate(hyperLogLogPlus1);
        try {
            hyperLogLogPlusAggregator.aggregate(null);
        } catch (IllegalArgumentException exception) {
            assertEquals("Expected an input array of length 1", exception.getMessage());
        }
    }

    @Test
    public void testFailedExecuteDueToEmptyInput() {
        HyperLogLogPlusAggregator hyperLogLogPlusAggregator = new HyperLogLogPlusAggregator();
        hyperLogLogPlusAggregator.init();
        hyperLogLogPlusAggregator._aggregate(hyperLogLogPlus1);
        try {
            hyperLogLogPlusAggregator.aggregate(new Object[0]);
        } catch (IllegalArgumentException exception) {
            assertEquals("Expected an input array of length 1", exception.getMessage());
        }
    }

    @Test
    public void testClone() {
        HyperLogLogPlusAggregator hyperLogLogPlusAggregator = new HyperLogLogPlusAggregator();
        hyperLogLogPlusAggregator.init();
        hyperLogLogPlusAggregator._aggregate(hyperLogLogPlus1);
        HyperLogLogPlusAggregator clone = hyperLogLogPlusAggregator.statelessClone();
        assertNotSame(hyperLogLogPlusAggregator, clone);
        assertEquals(0l, ((HyperLogLogPlus) clone.state()[0]).cardinality());
        clone._aggregate(hyperLogLogPlus2);
        assertEquals(hyperLogLogPlus2.cardinality(), ((HyperLogLogPlus) clone.state()[0]).cardinality());
    }

    @Test
    public void testCloneWhenEmpty() {
        HyperLogLogPlusAggregator hyperLogLogPlusAggregator = new HyperLogLogPlusAggregator();
        hyperLogLogPlusAggregator.init();
        HyperLogLogPlusAggregator clone = hyperLogLogPlusAggregator.statelessClone();
        assertNotSame(hyperLogLogPlusAggregator, clone);
        assertEquals(0l, ((HyperLogLogPlus) clone.state()[0]).cardinality());
        clone._aggregate(hyperLogLogPlus1);
        assertEquals(hyperLogLogPlus1.cardinality(), ((HyperLogLogPlus) clone.state()[0]).cardinality());
    }

    @Test
    public void testCloneOfBusySketch() {
        HyperLogLogPlusAggregator hyperLogLogPlusAggregator = new HyperLogLogPlusAggregator();
        hyperLogLogPlusAggregator.init();
        for (int i = 0; i < 100; i++) {
            HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(5, 5);
            for (int j = 0; j < 100; j++) {
                hyperLogLogPlus.offer(getRandomLetter());
            }
            hyperLogLogPlusAggregator._aggregate(hyperLogLogPlus);
        }
        HyperLogLogPlusAggregator clone = hyperLogLogPlusAggregator.statelessClone();
        assertNotSame(hyperLogLogPlusAggregator, clone);
        assertEquals(0l, ((HyperLogLogPlus) clone.state()[0]).cardinality());
        clone._aggregate(hyperLogLogPlus1);
        assertEquals(hyperLogLogPlus1.cardinality(), ((HyperLogLogPlus) clone.state()[0]).cardinality());
    }

    private static String getRandomLetter() {
        String[] letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("");
        return letters[(int) (Math.random() * letters.length)];
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final HyperLogLogPlusAggregator aggregator = new HyperLogLogPlusAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));
        // Then 1
        assertEquals("{\n" +
                "  \"class\" : \"gaffer.function.simple.aggregate.HyperLogLogPlusAggregator\"\n" +
                "}", json);

        // When 2
        final HyperLogLogPlusAggregator deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), HyperLogLogPlusAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return HyperLogLogPlusAggregator.class;
    }

    @Override
    protected ConsumerProducerFunction getInstance() {
        return new HyperLogLogPlusAggregator();
    }
}
