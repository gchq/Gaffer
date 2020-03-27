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
package uk.gov.gchq.gaffer.operation.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SampleToSplitPointsTest extends OperationTest<SampleToSplitPoints> {

    private static final List<String> TEST_INPUT = asList("one", "two", "three");
    private static final int TEST_NUM_SPLITS = 10;

    @Test
    public void shouldFailValidationIfNumSplitsIsLessThan1() {

        final SampleToSplitPoints op = new SampleToSplitPoints.Builder<>()
                .numSplits(0)
                .build();

        final ValidationResult result = op.validate();

        assertFalse(result.isValid());
        assertTrue(result.getErrorString(), result.getErrorString().contains("numSplits must be null or greater than 0"));
    }

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {

        final SampleToSplitPoints op = getTestObject();

        byte[] json = JSONSerialiser.serialise(op, true);

        final SampleToSplitPoints deserialisedOp = JSONSerialiser.deserialise(json, SampleToSplitPoints.class);

        assertExpected(deserialisedOp);
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {

        final SampleToSplitPoints op = getTestObject();

        assertExpected(op);
    }

    @Override
    public void shouldShallowCloneOperation() {

        final SampleToSplitPoints op = getTestObject();

        final SampleToSplitPoints clone = op.shallowClone();

        assertExpected(clone);
    }

    @Override
    protected SampleToSplitPoints getTestObject() {

        return new SampleToSplitPoints.Builder<>()
                .numSplits(TEST_NUM_SPLITS)
                .input(TEST_INPUT)
                .build();
    }

    private void assertExpected(final SampleToSplitPoints operation) {

        assertEquals(TEST_NUM_SPLITS, (int) operation.getNumSplits());
        assertEquals(TEST_INPUT, operation.getInput());
    }

}
