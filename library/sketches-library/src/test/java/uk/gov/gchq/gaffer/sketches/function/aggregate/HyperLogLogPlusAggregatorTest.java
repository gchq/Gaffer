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
package uk.gov.gchq.gaffer.sketches.function.aggregate;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.AggregateFunctionTest;
import uk.gov.gchq.gaffer.function.Function;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class HyperLogLogPlusAggregatorTest extends AggregateFunctionTest {
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
        hyperLogLogPlusAggregator.init();
        assertNull((hyperLogLogPlusAggregator.state()[0]));
        hyperLogLogPlusAggregator._aggregate(hyperLogLogPlus1);
        assertEquals(2l, ((HyperLogLogPlus) hyperLogLogPlusAggregator.state()[0]).cardinality());
        hyperLogLogPlusAggregator._aggregate(hyperLogLogPlus2);
        assertEquals(4l, ((HyperLogLogPlus) hyperLogLogPlusAggregator.state()[0]).cardinality());

        assertNotSame(hyperLogLogPlus1, hyperLogLogPlusAggregator.state());
        assertNotSame(hyperLogLogPlus2, hyperLogLogPlusAggregator.state());
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
        assertNull((clone.state()[0]));
        clone._aggregate(hyperLogLogPlus2);
        assertEquals(hyperLogLogPlus2.cardinality(), ((HyperLogLogPlus) clone.state()[0]).cardinality());
    }

    @Test
    public void testCloneWhenEmpty() {
        HyperLogLogPlusAggregator hyperLogLogPlusAggregator = new HyperLogLogPlusAggregator();
        hyperLogLogPlusAggregator.init();
        HyperLogLogPlusAggregator clone = hyperLogLogPlusAggregator.statelessClone();
        assertNotSame(hyperLogLogPlusAggregator, clone);
        assertNull((clone.state()[0]));
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
        assertNull((clone.state()[0]));
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
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.function.aggregate.HyperLogLogPlusAggregator\"%n" +
                "}"), json);

        // When 2
        final HyperLogLogPlusAggregator deserialisedAggregator = new JSONSerialiser()
                .deserialise(json.getBytes(), HyperLogLogPlusAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Test
    public void shouldBeEqualWhenBothAggregatorsHaveSameSketches() throws IOException {
        // Given
        final HyperLogLogPlusAggregator aggregator1 = new HyperLogLogPlusAggregator();
        final HyperLogLogPlusAggregator aggregator2 = new HyperLogLogPlusAggregator();
        final HyperLogLogPlus hllp1 = new HyperLogLogPlus(5, 5);
        hllp1.offer("A");
        hllp1.offer("B");

        final HyperLogLogPlus hllp2 = new HyperLogLogPlus(5, 5);
        hllp2.offer("A");
        hllp2.offer("B");

        aggregator1._aggregate(hllp1);
        aggregator2._aggregate(hllp2);

        // Then
        assertEquals(aggregator1, aggregator2);
    }

    @Test
    public void shouldBeNotEqualWhenBothAggregatorsHaveDifferentSketches() throws IOException {
        // Given
        final HyperLogLogPlusAggregator aggregator1 = new HyperLogLogPlusAggregator();
        final HyperLogLogPlusAggregator aggregator2 = new HyperLogLogPlusAggregator();
        final HyperLogLogPlus hllp1 = new HyperLogLogPlus(5, 5);
        hllp1.offer("A");
        hllp1.offer("B");

        final HyperLogLogPlus hllp2 = new HyperLogLogPlus(5, 5);
        hllp2.offer("A");
        hllp2.offer("C");

        aggregator1._aggregate(hllp1);
        aggregator2._aggregate(hllp2);

        // Then
        assertNotEquals(aggregator1, aggregator2);
    }

    @Test
    public void shouldBeNotEqualWhenFirstAggregatorsHasNullHllp() throws IOException {
        // Given
        final HyperLogLogPlusAggregator aggregator1 = new HyperLogLogPlusAggregator();
        final HyperLogLogPlusAggregator aggregator2 = new HyperLogLogPlusAggregator();
        final HyperLogLogPlus hllp2 = new HyperLogLogPlus(5, 5);
        hllp2.offer("A");
        hllp2.offer("C");

        aggregator2._aggregate(hllp2);

        // Then
        assertNotEquals(aggregator1, aggregator2);
    }

    @Test
    public void shouldBeNotEqualWhenSecondAggregatorsHasNullHllp() throws IOException {
        // Given
        final HyperLogLogPlusAggregator aggregator1 = new HyperLogLogPlusAggregator();
        final HyperLogLogPlusAggregator aggregator2 = new HyperLogLogPlusAggregator();
        final HyperLogLogPlus hllp1 = new HyperLogLogPlus(5, 5);
        hllp1.offer("A");
        hllp1.offer("B");

        aggregator1._aggregate(hllp1);

        // Then
        assertNotEquals(aggregator1, aggregator2);
    }

    @Test
    public void shouldBeNotEqualWhenBothAggregatorsHaveSketchesWithDifferentPAndSpValues() throws IOException {
        // Given
        final HyperLogLogPlusAggregator aggregator1 = new HyperLogLogPlusAggregator();
        final HyperLogLogPlusAggregator aggregator2 = new HyperLogLogPlusAggregator();
        final HyperLogLogPlus hllp1 = new HyperLogLogPlus(5, 5);
        hllp1.offer("A");
        hllp1.offer("B");

        final HyperLogLogPlus hllp2 = new HyperLogLogPlus(6, 6);
        hllp2.offer("A");
        hllp2.offer("B");

        aggregator1._aggregate(hllp1);
        aggregator2._aggregate(hllp2);

        // Then
        assertNotEquals(aggregator1, aggregator2);
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
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
