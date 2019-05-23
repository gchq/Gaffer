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

package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.impl;

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class ImportAccumuloKeyValueFilesTest extends OperationTest<ImportAccumuloKeyValueFiles> {
    private static final String INPUT_DIRECTORY = "/input";
    private static final String FAIL_DIRECTORY = "/fail";
    private static final String TEST_OPTION_KEY = "testOption";

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("failurePath", "inputPath");
    }

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ImportAccumuloKeyValueFiles op = new ImportAccumuloKeyValueFiles();
        op.setInputPath(INPUT_DIRECTORY);
        op.setFailurePath(FAIL_DIRECTORY);

        // When
        byte[] json = JSONSerialiser.serialise(op, true);

        final ImportAccumuloKeyValueFiles deserialisedOp = JSONSerialiser.deserialise(json, ImportAccumuloKeyValueFiles.class);

        // Then
        assertEquals(INPUT_DIRECTORY, deserialisedOp.getInputPath());
        assertEquals(FAIL_DIRECTORY, deserialisedOp.getFailurePath());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // When
        final ImportAccumuloKeyValueFiles importAccumuloKeyValueFiles = new ImportAccumuloKeyValueFiles.Builder()
                .inputPath(INPUT_DIRECTORY)
                .failurePath(FAIL_DIRECTORY)
                .option(TEST_OPTION_KEY, "true")
                .build();

        // Then
        assertEquals(INPUT_DIRECTORY, importAccumuloKeyValueFiles.getInputPath());
        assertEquals(FAIL_DIRECTORY, importAccumuloKeyValueFiles.getFailurePath());
        assertEquals("true", importAccumuloKeyValueFiles.getOption(TEST_OPTION_KEY));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final ImportAccumuloKeyValueFiles importAccumuloKeyValueFiles = new ImportAccumuloKeyValueFiles.Builder()
                .inputPath(INPUT_DIRECTORY)
                .failurePath(FAIL_DIRECTORY)
                .option("testOption", "true")
                .build();

        // When
        final ImportAccumuloKeyValueFiles clone = importAccumuloKeyValueFiles.shallowClone();

        // Then
        assertNotSame(importAccumuloKeyValueFiles, clone);
        assertEquals("true", clone.getOption("testOption"));
        assertEquals(INPUT_DIRECTORY, clone.getInputPath());
        assertEquals(FAIL_DIRECTORY, clone.getFailurePath());
    }

    @Override
    protected ImportAccumuloKeyValueFiles getTestObject() {
        return new ImportAccumuloKeyValueFiles();
    }
}
