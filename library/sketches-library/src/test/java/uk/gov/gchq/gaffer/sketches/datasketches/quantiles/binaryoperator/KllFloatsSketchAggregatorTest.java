/*
 * Copyright 2017-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.binaryoperator;

import com.yahoo.sketches.kll.KllFloatsSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.function.BinaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KllFloatsSketchAggregatorTest extends BinaryOperatorTest {

    private static final double DELTA = 0.01D;

    @Test
    public void testAggregate() {
        final KllFloatsSketchAggregator sketchAggregator = new KllFloatsSketchAggregator();

        KllFloatsSketch currentSketch = new KllFloatsSketch();
        currentSketch.update(1.0F);
        currentSketch.update(2.0F);
        currentSketch.update(3.0F);

        assertEquals(3L, currentSketch.getN());
        assertEquals(2.0D, currentSketch.getQuantile(0.5D), DELTA);

        KllFloatsSketch newSketch = new KllFloatsSketch();
        newSketch.update(4.0F);
        newSketch.update(5.0F);
        newSketch.update(6.0F);
        newSketch.update(7.0F);

        currentSketch = sketchAggregator.apply(currentSketch, newSketch);
        assertEquals(7L, currentSketch.getN());
        assertEquals(4.0D, currentSketch.getQuantile(0.5D), DELTA);
    }

    @Test
    public void testEquals() {
        assertEquals(new KllFloatsSketchAggregator(), new KllFloatsSketchAggregator());
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final KllFloatsSketchAggregator aggregator = new KllFloatsSketchAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.quantiles.binaryoperator.KllFloatsSketchAggregator\"%n" +
                "}"), json);

        // When 2
        final KllFloatsSketchAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), KllFloatsSketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return KllFloatsSketchAggregator.class;
    }

    @Override
    protected KllFloatsSketchAggregator getInstance() {
        return new KllFloatsSketchAggregator();
    }

    @Override
    protected Iterable<KllFloatsSketchAggregator> getDifferentInstancesOrNull() {
        return null;
    }
}
