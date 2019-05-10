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

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class SplitStoreTest extends OperationTest<SplitStore> {
    private static final String INPUT_DIRECTORY = "/input";
    private static final String TEST_OPTION_KEY = "testOption";

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("inputPath");
    }

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final SplitStore op = new SplitStore();
        op.setInputPath(INPUT_DIRECTORY);

        // When
        byte[] json = JSONSerialiser.serialise(op, true);

        final SplitStore deserialisedOp = JSONSerialiser.deserialise(json, SplitStore.class);

        // Then
        assertEquals(INPUT_DIRECTORY, deserialisedOp.getInputPath());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final SplitStore splitTable = new SplitStore.Builder().inputPath(INPUT_DIRECTORY).option(TEST_OPTION_KEY, "true").build();
        assertEquals(INPUT_DIRECTORY, splitTable.getInputPath());
        assertEquals("true", splitTable.getOption(TEST_OPTION_KEY));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final SplitStore splitStore = new SplitStore.Builder()
                .inputPath(INPUT_DIRECTORY)
                .option(TEST_OPTION_KEY, "false")
                .build();

        // When
        final SplitStore clone = splitStore.shallowClone();

        // Then
        assertNotSame(splitStore, clone);
        assertEquals(INPUT_DIRECTORY, clone.getInputPath());
        assertEquals("false", clone.getOptions().get(TEST_OPTION_KEY));
    }

    @Override
    protected SplitStore getTestObject() {
        return new SplitStore();
    }
}
