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

import com.yahoo.sketches.quantiles.DoublesUnion;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.function.BinaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DoublesUnionAggregatorTest extends BinaryOperatorTest {

    private static final double DELTA = 0.01D;

    @Test
    public void testAggregate() {
        final DoublesUnionAggregator unionAggregator = new DoublesUnionAggregator();

        DoublesUnion currentUnion = DoublesUnion.builder().build();
        currentUnion.update(1.0D);
        currentUnion.update(2.0D);
        currentUnion.update(3.0D);

        assertEquals(3L, currentUnion.getResult().getN());
        assertEquals(2.0D, currentUnion.getResult().getQuantile(0.5D), DELTA);

        DoublesUnion newUnion = DoublesUnion.builder().build();
        newUnion.update(4.0D);
        newUnion.update(5.0D);
        newUnion.update(6.0D);
        newUnion.update(7.0D);

        currentUnion = unionAggregator.apply(currentUnion, newUnion);
        assertEquals(7L, currentUnion.getResult().getN());
        assertEquals(4.0D, currentUnion.getResult().getQuantile(0.5D), DELTA);
    }

    @Test
    public void testEquals() {
        assertEquals(new DoublesUnionAggregator(), new DoublesUnionAggregator());
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final DoublesUnionAggregator aggregator = new DoublesUnionAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.quantiles.binaryoperator.DoublesUnionAggregator\"%n" +
                "}"), json);

        // When 2
        final DoublesUnionAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), DoublesUnionAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return DoublesUnionAggregator.class;
    }

    @Override
    protected DoublesUnionAggregator getInstance() {
        return new DoublesUnionAggregator();
    }

    @Override
    protected Iterable<DoublesUnionAggregator> getDifferentInstancesOrNull() {
        return null;
    }
}
