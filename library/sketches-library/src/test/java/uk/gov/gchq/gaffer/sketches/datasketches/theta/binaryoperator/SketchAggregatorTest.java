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
package uk.gov.gchq.gaffer.sketches.datasketches.theta.binaryoperator;

import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;
import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SketchAggregatorTest extends BinaryOperatorTest {
    private static final double DELTA = 0.01D;
    private UpdateSketch sketch1;
    private UpdateSketch sketch2;

    @Before
    public void setup() {
        sketch1 = UpdateSketch.builder().build();
        sketch1.update("A");
        sketch1.update("B");

        sketch2 = UpdateSketch.builder().build();
        sketch2.update("C");
        sketch2.update("D");
    }

    @Test
    public void testAggregate() {
        final SketchAggregator unionAggregator = new SketchAggregator();
        Sketch currentState = sketch1;
        assertEquals(2.0D, currentState.getEstimate(), DELTA);
        currentState = unionAggregator.apply(currentState, sketch2);
        assertEquals(4.0D, currentState.getEstimate(), DELTA);
    }

    @Test
    public void testEquals() {
        assertEquals(new SketchAggregator(), new SketchAggregator());
    }

    @Override
    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final SketchAggregator aggregator = new SketchAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));
        // Then 1
        JsonUtil.equals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.theta.binaryoperator.SketchAggregator\"%n" +
                "}"), json);

        // When 2
        final SketchAggregator deserialisedAggregator = new JSONSerialiser()
                .deserialise(json.getBytes(), SketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return SketchAggregator.class;
    }

    @Override
    protected SketchAggregator getInstance() {
        return new SketchAggregator();
    }
}
