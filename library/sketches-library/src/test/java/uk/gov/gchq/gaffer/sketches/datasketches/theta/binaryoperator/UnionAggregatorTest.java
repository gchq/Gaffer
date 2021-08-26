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
package uk.gov.gchq.gaffer.sketches.datasketches.theta.binaryoperator;

import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.function.BinaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class UnionAggregatorTest extends BinaryOperatorTest {

    private static final double DELTA = 0.01D;

    @Test
    public void testAggregate() {
        final UnionAggregator unionAggregator = new UnionAggregator();

        Union currentUnion = Sketches.setOperationBuilder().buildUnion();
        currentUnion.update("A");
        currentUnion.update("B");

        assertEquals(2.0D, currentUnion.getResult().getEstimate(), DELTA);

        Union newUnion = Sketches.setOperationBuilder().buildUnion();
        newUnion.update("C");
        newUnion.update("D");

        currentUnion = unionAggregator.apply(currentUnion, newUnion);
        assertEquals(4.0D, currentUnion.getResult().getEstimate(), DELTA);
    }

    @Test
    public void testEquals() {
        assertEquals(new UnionAggregator(), new UnionAggregator());
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final UnionAggregator aggregator = new UnionAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.theta.binaryoperator.UnionAggregator\"%n" +
                "}"), json);

        // When 2
        final UnionAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), UnionAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return UnionAggregator.class;
    }

    @Override
    protected UnionAggregator getInstance() {
        return new UnionAggregator();
    }

    @Override
    protected Iterable<UnionAggregator> getDifferentInstancesOrNull() {
        return null;
    }
}
