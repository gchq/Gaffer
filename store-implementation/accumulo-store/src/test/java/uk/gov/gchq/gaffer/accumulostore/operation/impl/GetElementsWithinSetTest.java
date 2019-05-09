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

package uk.gov.gchq.gaffer.accumulostore.operation.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloTestData;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class GetElementsWithinSetTest extends OperationTest<GetElementsWithinSet> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .input(AccumuloTestData.SEED_SOURCE_1,
                        AccumuloTestData.SEED_DESTINATION_1,
                        AccumuloTestData.SEED_SOURCE_2,
                        AccumuloTestData.SEED_DESTINATION_2)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);

        final GetElementsWithinSet deserialisedOp = JSONSerialiser.deserialise(json, GetElementsWithinSet.class);

        // Then
        final Iterator itrSeedsA = deserialisedOp.getInput().iterator();
        assertEquals(AccumuloTestData.SEED_SOURCE_1, itrSeedsA.next());
        assertEquals(AccumuloTestData.SEED_DESTINATION_1, itrSeedsA.next());
        assertEquals(AccumuloTestData.SEED_SOURCE_2, itrSeedsA.next());
        assertEquals(AccumuloTestData.SEED_DESTINATION_2, itrSeedsA.next());
        assertFalse(itrSeedsA.hasNext());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final GetElementsWithinSet getElementsWithinSet = new GetElementsWithinSet.Builder()
                .input(AccumuloTestData.SEED_A)
                .directedType(DirectedType.DIRECTED)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(new View.Builder()
                        .edge("testEdgegroup")
                        .build())
                .build();
        assertEquals("true", getElementsWithinSet.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(DirectedType.DIRECTED, getElementsWithinSet.getDirectedType());
        assertEquals(AccumuloTestData.SEED_A, getElementsWithinSet.getInput().iterator().next());
        assertNotNull(getElementsWithinSet.getView());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final View view = new View.Builder()
                .edge("testEdgegroup")
                .build();
        final GetElementsWithinSet getElementsWithinSet = new GetElementsWithinSet.Builder()
                .input(AccumuloTestData.SEED_A)
                .directedType(DirectedType.DIRECTED)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(view)
                .build();

        // When
        final GetElementsWithinSet clone = getElementsWithinSet.shallowClone();

        // Then
        assertNotSame(getElementsWithinSet, clone);
        assertEquals("true", clone.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(DirectedType.DIRECTED, clone.getDirectedType());
        assertEquals(AccumuloTestData.SEED_A, clone.getInput().iterator().next());
        assertEquals(view, clone.getView());
    }

    @Test
    public void shouldCreateInputFromVertices() {
        // When
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .input(AccumuloTestData.SEED_B, AccumuloTestData.SEED_B1.getVertex())
                .build();

        // Then
        assertEquals(
                Lists.newArrayList(AccumuloTestData.SEED_B, AccumuloTestData.SEED_B1),
                Lists.newArrayList(op.getInput())
        );
    }

    @Override
    protected GetElementsWithinSet getTestObject() {
        return new GetElementsWithinSet();
    }
}
