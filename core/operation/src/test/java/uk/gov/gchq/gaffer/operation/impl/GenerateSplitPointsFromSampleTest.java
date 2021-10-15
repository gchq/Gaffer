/*
 * Copyright 2020 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GenerateSplitPointsFromSampleTest extends OperationTest<GenerateSplitPointsFromSample> {

    private static final List<String> TEST_INPUT = asList("one", "two", "three");
    private static final int TEST_NUM_SPLITS = 10;

    @Test
    public void shouldFailValidationIfNumSplitsIsLessThan1() {
        // Given
        final GenerateSplitPointsFromSample op = new GenerateSplitPointsFromSample.Builder<>()
                .numSplits(0)
                .build();

        // When
        final ValidationResult result = op.validate();

        // Then
        assertFalse(result.isValid());
        assertTrue(result.getErrorString().contains("numSplits must be null or greater than 0"), result.getErrorString());
    }

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final GenerateSplitPointsFromSample op = getTestObject();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final GenerateSplitPointsFromSample deserialisedOp = JSONSerialiser.deserialise(json, GenerateSplitPointsFromSample.class);

        // Then
        assertExpected(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given / When
        final GenerateSplitPointsFromSample op = getTestObject();

        // Then
        assertExpected(op);
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final GenerateSplitPointsFromSample op = getTestObject();

        // When
        final GenerateSplitPointsFromSample clone = op.shallowClone();

        // Then
        assertExpected(clone);
    }

    @Override
    protected GenerateSplitPointsFromSample getTestObject() {
        return new GenerateSplitPointsFromSample.Builder<>()
                .numSplits(TEST_NUM_SPLITS)
                .input(TEST_INPUT)
                .build();
    }

    private void assertExpected(final GenerateSplitPointsFromSample operation) {
        assertEquals(TEST_NUM_SPLITS, (int) operation.getNumSplits());
        assertEquals(TEST_INPUT, operation.getInput());
    }

}
