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

package uk.gov.gchq.gaffer.integration.impl;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.generator.NeptuneFormat;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.add.CsvToElements;
import uk.gov.gchq.gaffer.operation.impl.export.localfile.ExportToLocalFile;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.imprt.localfile.ImportFromLocalFile;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;

import java.io.File;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ImportExportIT extends AbstractStoreIT {

    private static final String INCOMING_FILE_PATH = "NeptuneEntitiesAndEdgesWithProperties.csv";
    private Path path;
    private File outgoingFile;
    @TempDir
    Path tempDir;

    //@Override
    public void _setup() throws Exception {

        try {
            path = tempDir.resolve("outputFile.csv");
        } catch (final InvalidPathException ipe) {
            throw new InvalidPathException(ipe.getInput(), "error creating temporary test file in " + this.getClass().getSimpleName());
        }

        outgoingFile = path.toFile();
    }
    @Test
    public void shouldImportFromFileThenExportBackOutToDifferentFile() throws OperationException, IOException {
        // Given
        List<String> expectedData = Arrays.asList(
                ":ID,:LABEL,:TYPE,:START_ID,:END_ID,count:Int,DIRECTED:Boolean",
                "A,BasicEntity,,,,1,",
                "B,BasicEntity,,,,2,",
                ",,BasicEdge,A,B,1,true"
        );


        final OperationChain<Iterable<? extends String>> importExportOpChain = new OperationChain.Builder()
                .first(new ImportFromLocalFile.Builder()
                        .filePath(INCOMING_FILE_PATH)
                        .build())
                .then(new CsvToElements.Builder()
                        .csvFormat(new NeptuneFormat())
                        .build())
                .then(new AddElements.Builder()
                        .build())
                .then(new GetAllElements())
                .then(new ToCsv.Builder()
                        .csvFormat(new NeptuneFormat())
                        .build())
                .then(new ExportToLocalFile.Builder()
                        .filePath(outgoingFile.getAbsolutePath())
                        .build())
                .build();

        final Iterable<? extends String> exportedData = graph.execute(importExportOpChain, getUser());

        //   When

        Iterable<String> incomingFile = readFile(INCOMING_FILE_PATH);
        Iterable<String> outgoingFile = readFile(path.toFile().getAbsolutePath());


        //  Then
        assertThat(outgoingFile).containsExactlyInAnyOrderElementsOf(incomingFile);
    }


    public Iterable<String> readFile(final String filePath) throws OperationException {
        final OperationChain<Iterable<String>> importOpChain = new OperationChain.Builder()
                .first(new ImportFromLocalFile.Builder()
                        .filePath(INCOMING_FILE_PATH)
                        .build())
                .build();

        return graph.execute(importOpChain, getUser());
    }

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