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
package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.binaryoperator;

import com.google.common.collect.Ordering;
import com.yahoo.sketches.quantiles.ItemsUnion;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;
import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StringsUnionAggregatorTest extends BinaryOperatorTest {
    private ItemsUnion<String> union1;
    private ItemsUnion<String> union2;

    @Before
    public void setup() {
        union1 = ItemsUnion.getInstance(Ordering.<String>natural());
        union1.update("1");
        union1.update("2");
        union1.update("3");

        union2 = ItemsUnion.getInstance(Ordering.<String>natural());
        union2.update("4");
        union2.update("5");
        union2.update("6");
        union2.update("7");
    }

    @Test
    public void testAggregate() {
        final StringsUnionAggregator unionAggregator = new StringsUnionAggregator();
        ItemsUnion<String> currentState = union1;
        assertEquals(3L, currentState.getResult().getN());
        assertEquals("2", currentState.getResult().getQuantile(0.5D));

        currentState = unionAggregator.apply(currentState, union2);
        assertEquals(7L, currentState.getResult().getN());
        assertEquals("4", currentState.getResult().getQuantile(0.5D));
    }

    @Test
    public void testEquals() {
        assertEquals(new StringsUnionAggregator(), new StringsUnionAggregator());
    }

    @Override
    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final StringsUnionAggregator aggregator = new StringsUnionAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.quantiles.binaryoperator.StringsUnionAggregator\"%n" +
                "}"), json);

        // When 2
        final StringsUnionAggregator deserialisedAggregator = new JSONSerialiser()
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
