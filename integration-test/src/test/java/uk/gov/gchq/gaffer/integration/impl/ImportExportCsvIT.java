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

package uk.gov.gchq.gaffer.integration.impl;


import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.generator.NeptuneCsvElementGenerator;
import uk.gov.gchq.gaffer.data.generator.NeptuneCsvGenerator;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.localfile.ExportToLocalFile;
import uk.gov.gchq.gaffer.operation.impl.export.localfile.ImportFromLocalFile;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE", justification = "Intentional part of test")
public class ImportExportCsvIT extends AbstractStoreIT {
    private static final String INPUT_FILE_PATH = "NeptuneEntitiesAndEdgesWithProperties.csv";
    private static final String OUTPUT_FILE_PATH = "outputFile.csv";

    private String absoluteOutputPath;

    @TempDir
    Path tempDir;

    @Override
    public void _setup() throws Exception {
        absoluteOutputPath = tempDir.resolve(OUTPUT_FILE_PATH).toAbsolutePath().toString();
    }

    @Test
    public void shouldImportFromFileThenCorrectlyExportToFile() throws OperationException, IOException {
        // Given
        final OperationChain<Iterable<? extends String>> importExportOpChain = new OperationChain.Builder()
                .first(new ImportFromLocalFile.Builder()
                        .filePath(INPUT_FILE_PATH)
                        .build())
                .then(new GenerateElements.Builder<String>()
                        .generator(new NeptuneCsvElementGenerator())
                        .build())
                .then(new AddElements.Builder()
                        .build())
                .then(new GetAllElements())
                .then(new ToCsv.Builder()
                        .generator(new NeptuneCsvGenerator())
                        .build())
                .then(new ExportToLocalFile.Builder()
                        .filePath(absoluteOutputPath)
                        .build())
                .build();

        // When
        final Iterable<String> exportedData = (Iterable<String>) graph.execute(importExportOpChain, getUser());
        final List<String> expectedData = Arrays.asList(
            ":ID,:LABEL,:TYPE,:START_ID,:END_ID,count:Int,DIRECTED:Boolean",
            "A,BasicEntity,,,,1,",
            "B,BasicEntity,,,,2,",
            ",,BasicEdge,A,B,1,true"
        );

        final Iterable<String> outputFileData = readFile(absoluteOutputPath);
        final Iterable<String> inputFileData = readFile(INPUT_FILE_PATH);

        // Then
        assertThat(exportedData).containsExactlyInAnyOrderElementsOf(expectedData);
        assertThat(outputFileData).containsExactlyInAnyOrderElementsOf(inputFileData);
    }

    private Iterable<String> readFile(final String filePath) throws OperationException {
        final OperationChain<Iterable<String>> importOpChain = new OperationChain.Builder()
                .first(new ImportFromLocalFile.Builder()
                        .filePath(filePath)
                        .build())
                .build();

        return graph.execute(importOpChain, getUser());
    }

    @Override
    protected Schema createSchema() {
        return new Schema.Builder()
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("DIRECTED", new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .build())
                .type("int", new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Max())
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .property(TestPropertyNames.COUNT, "int")
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("DIRECTED")
                        .property(TestPropertyNames.COUNT, "int")
                        .build())
                .build();
    }
}
