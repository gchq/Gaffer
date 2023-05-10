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

package uk.gov.gchq.gaffer.export;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.operation.impl.export.localfile.LocalFileExporter;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalFileExporterTest {
    public static final ArrayList<String> INPUT = Lists.newArrayList("header", "line1", "line2");
    private File file;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) {
        file = tempDir.resolve("testfile.csv").toFile();
    }

    @Test
    public void shouldWriteToLocalFile() throws Exception {
        // Given
        final LocalFileExporter exporter = new LocalFileExporter();

        // When
        exporter.add(file.getAbsolutePath(), INPUT);

        List<String> fileOutput;
        try (Stream<String> lines = Files.lines(Paths.get(file.getAbsolutePath()))) {
            fileOutput = lines.collect(Collectors.toList());
        }

        // Then
        assertThat(fileOutput).isEqualTo(INPUT);
    }
}
