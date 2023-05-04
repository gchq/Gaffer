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

package uk.gov.gchq.gaffer.store.operation.handler.export.localfile;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.operation.impl.export.localfile.ExportToLocalFile;
import uk.gov.gchq.gaffer.operation.impl.export.localfile.LocalFileExporter;

import uk.gov.gchq.gaffer.store.Context;


import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class ExportToLocalFileHandlerTest {
    public static final ArrayList<String> INPUT = Lists.newArrayList("header", "line1", "line2");
    private File file;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        file = tempDir.resolve("testfile.csv").toFile();
    }

    @Test
    public void shouldWriteToLocalFile() throws Exception {
        // Given
        final ExportToLocalFile exportToLocalFile = new ExportToLocalFile.Builder()
                .input(INPUT)
                .filePath(file.getAbsolutePath())
                .build();

        final Context context = new Context();
        context.addExporter(new LocalFileExporter());

        final ExportToLocalFileHandler handler = new ExportToLocalFileHandler();

        // When
        final Object result = handler.doOperation(exportToLocalFile, context, null);

        List<String> fileOutput;
        try (Stream<String> lines = Files.lines(Paths.get(file.getAbsolutePath()))) {
            fileOutput = lines.collect(Collectors.toList());
        }

        // Then
        assertThat(INPUT).isEqualTo(fileOutput);
        assertThat(result).isEqualTo(INPUT);
    }
}
