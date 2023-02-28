/*
 * Copyright 2022 Crown Copyright
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

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.export.localfile.ExportToLocalFile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class ExportToLocalFileTest extends OperationTest<ExportToLocalFile>  {
    public static final String FILE_PATH = "path/to/file.csv";
    public static final ArrayList<String> INPUT = Lists.newArrayList("header", "line1", "line2");

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        ExportToLocalFile op = getPopulatedObject();

        // When
        final byte[] json = JSONSerialiser.serialise(op);
        final ExportToLocalFile deserialisedOp = JSONSerialiser.deserialise(json, op.getClass());

        // Then
        assertThat(FILE_PATH).isEqualTo(deserialisedOp.getFilePath());
        assertThat(INPUT).containsAll(deserialisedOp.getInput());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        ExportToLocalFile op = getPopulatedObject();

        // Then
        assertThat(FILE_PATH).isEqualTo(op.getFilePath());
        assertThat(INPUT).containsAll(op.getInput());
    }

    @Test
    @Override
    public void shouldShallowCloneOperationREVIEWMAYBEDELETE() {
        // Given
        ExportToLocalFile op = getPopulatedObject();

        // When
        ExportToLocalFile clone = op.shallowClone();

        // Then
        assertThat(op).isNotEqualTo(clone);
        assertThat(FILE_PATH).isEqualTo(clone.getFilePath());
        assertThat(INPUT).isEqualTo(clone.getInput());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObjectOld().getOutputClass();

        // Then
        assertThat(Iterable.class).isEqualTo(outputClass);
    }

    @Override
    protected Set<String> getNonRequiredFields() {
        return new HashSet(Collections.singleton("filePath"));
    }

    protected ExportToLocalFile getPopulatedObject() {
        return new ExportToLocalFile.Builder()
                .filePath(FILE_PATH)
                .input(INPUT)
                .build();
    }

    @Override
    protected ExportToLocalFile getTestObjectOld() {
        return new ExportToLocalFile();
    }
}
