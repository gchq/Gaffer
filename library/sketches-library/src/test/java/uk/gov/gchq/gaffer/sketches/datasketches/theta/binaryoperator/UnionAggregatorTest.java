/*
 * Copyright 2017-2018 Crown Copyright
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
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class UnionAggregatorTest extends BinaryOperatorTest {
    private static final double DELTA = 0.01D;
    private Union union1;
    private Union union2;

    @Before
    public void setup() {
        union1 = Sketches.setOperationBuilder().buildUnion();
        union1.update("A");
        union1.update("B");

        union2 = Sketches.setOperationBuilder().buildUnion();
        union2.update("C");
        union2.update("D");
    }

    @Test
    public void testAggregate() {
        final UnionAggregator unionAggregator = new UnionAggregator();
        Union currentState = union1;
        assertEquals(2.0D, currentState.getResult().getEstimate(), DELTA);
        currentState = unionAggregator.apply(currentState, union2);
        assertEquals(4.0D, currentState.getResult().getEstimate(), DELTA);
    }

    @Test
    public void testEquals() {
        assertEquals(new UnionAggregator(), new UnionAggregator());
    }

    @Override
    @Test
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
}
