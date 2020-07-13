/*
 * Copyright 2017-2020 Crown Copyright
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

import com.google.common.collect.Ordering;
import com.yahoo.sketches.quantiles.ItemsUnion;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.function.BinaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StringsUnionAggregatorTest extends BinaryOperatorTest {

    @Test
    public void testAggregate() {
        final StringsUnionAggregator unionAggregator = new StringsUnionAggregator();

        ItemsUnion<String> currentSketch = ItemsUnion.getInstance(Ordering.<String>natural());
        currentSketch.update("1");
        currentSketch.update("2");
        currentSketch.update("3");

        assertEquals(3L, currentSketch.getResult().getN());
        assertEquals("2", currentSketch.getResult().getQuantile(0.5D));

        ItemsUnion<String> newSketch = ItemsUnion.getInstance(Ordering.<String>natural());
        newSketch.update("4");
        newSketch.update("5");
        newSketch.update("6");
        newSketch.update("7");

        currentSketch = unionAggregator.apply(currentSketch, newSketch);
        assertEquals(7L, currentSketch.getResult().getN());
        assertEquals("4", currentSketch.getResult().getQuantile(0.5D));
    }

    @Test
    public void testEquals() {
        assertEquals(new StringsUnionAggregator(), new StringsUnionAggregator());
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final StringsUnionAggregator aggregator = new StringsUnionAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.quantiles.binaryoperator.StringsUnionAggregator\"%n" +
                "}"), json);

        // When 2
        final StringsUnionAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), StringsUnionAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return StringsUnionAggregator.class;
    }

    @Override
    protected StringsUnionAggregator getInstance() {
        return new StringsUnionAggregator();
    }
}
