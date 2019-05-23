/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SampleElementsForSplitPointsTest extends OperationTest<SampleElementsForSplitPoints> {
    @Test
    public void shouldFailValidationIfNumSplitsIsLessThan1() {
        // Given
        final SampleElementsForSplitPoints op = new SampleElementsForSplitPoints.Builder<>()
                .numSplits(0)
                .build();

        // When
        final ValidationResult result = op.validate();

        // Then
        assertFalse(result.isValid());
        assertTrue(result.getErrorString(), result.getErrorString().contains("numSplits must be null or greater than 0"));
    }

    @Test
    public void shouldFailValidationIfProportionToSampleIsNotIn0_1Range() {
        // Given
        final SampleElementsForSplitPoints op = new SampleElementsForSplitPoints.Builder<>()
                .proportionToSample(1.1f)
                .build();

        // When
        final ValidationResult result = op.validate();

        // Then
        assertFalse(result.isValid());
        assertTrue(result.getErrorString(), result.getErrorString().contains("proportionToSample must within range: [0, 1]"));
    }

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final SampleElementsForSplitPoints op = new SampleElementsForSplitPoints.Builder<>()
                .numSplits(10)
                .proportionToSample(0.5f)
                .input(new Entity(TestGroups.ENTITY, "vertex"))
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);

        final SampleElementsForSplitPoints deserialisedOp = JSONSerialiser.deserialise(json, SampleElementsForSplitPoints.class);

        // Then
        assertEquals(10, (int) deserialisedOp.getNumSplits());
        assertEquals(0.5f, deserialisedOp.getProportionToSample(), 0.1);
        assertEquals(Collections.singletonList(new Entity(TestGroups.ENTITY, "vertex")), deserialisedOp.getInput());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // When
        final SampleElementsForSplitPoints op = new SampleElementsForSplitPoints.Builder<>()
                .numSplits(10)
                .proportionToSample(0.5f)
                .input(new Entity(TestGroups.ENTITY, "vertex"))
                .build();

        // Then
        assertEquals(10, (int) op.getNumSplits());
        assertEquals(0.5f, op.getProportionToSample(), 0.1);
        assertEquals(Collections.singletonList(new Entity(TestGroups.ENTITY, "vertex")), op.getInput());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final SampleElementsForSplitPoints op = new SampleElementsForSplitPoints.Builder<>()
                .numSplits(10)
                .proportionToSample(0.5f)
                .input(new Entity(TestGroups.ENTITY, "vertex"))
                .build();

        // When
        final SampleElementsForSplitPoints clone = op.shallowClone();

        // Then
        assertEquals(10, (int) clone.getNumSplits());
        assertEquals(0.5f, clone.getProportionToSample(), 0.1);
        assertEquals(Collections.singletonList(new Entity(TestGroups.ENTITY, "vertex")), clone.getInput());
    }

    @Override
    protected SampleElementsForSplitPoints getTestObject() {
        return new SampleElementsForSplitPoints();
    }
}
