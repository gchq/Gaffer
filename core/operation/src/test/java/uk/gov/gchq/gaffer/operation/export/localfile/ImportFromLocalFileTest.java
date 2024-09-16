/*
 * Copyright 2022-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.export.localfile;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.export.localfile.ImportFromLocalFile;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class ImportFromLocalFileTest extends OperationTest<ImportFromLocalFile>  {

    @Required
    public static final String FILE_PATH = "path/to/file.csv";

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        ImportFromLocalFile op = getPopulatedObject();

        // When
        final byte[] json = JSONSerialiser.serialise(op);
        final ImportFromLocalFile deserialisedOp = JSONSerialiser.deserialise(json, op.getClass());

        // Then
        assertThat(FILE_PATH).isEqualTo(deserialisedOp.getKey());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        ImportFromLocalFile op = getPopulatedObject();

        // Then
        assertThat(FILE_PATH).isEqualTo(op.getKey());

    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        ImportFromLocalFile op = getPopulatedObject();

        // When
        ImportFromLocalFile clone = op.shallowClone();

        // Then
        assertThat(op).isNotEqualTo(clone);
        assertThat(FILE_PATH).isEqualTo(clone.getKey());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertThat(Iterable.class).isEqualTo(outputClass);
    }

    @Override
    protected Set<String> getRequiredFields() {
        return new HashSet(Collections.singleton("filePath"));
    }

    protected ImportFromLocalFile getPopulatedObject() {
        return new ImportFromLocalFile.Builder()
                .key(FILE_PATH)
                .build();
    }

    @Override
    protected ImportFromLocalFile getTestObject() {
        return new ImportFromLocalFile();
    }
}
