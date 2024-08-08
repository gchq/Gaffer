/*
 * Copyright 2018-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.hdfs.integration.loader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.TextJobInitialiser;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.JsonMapperGenerator;
import uk.gov.gchq.gaffer.integration.impl.loader.UserLoaderIT;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.AggregationSchemaLoader;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.BasicSchemaLoader;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.FullSchemaLoader;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.VisibilitySchemaLoader;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.store.schema.TestSchema.AGGREGATION_SCHEMA;
import static uk.gov.gchq.gaffer.store.schema.TestSchema.BASIC_SCHEMA;
import static uk.gov.gchq.gaffer.store.schema.TestSchema.FULL_SCHEMA;
import static uk.gov.gchq.gaffer.store.schema.TestSchema.VISIBILITY_SCHEMA;

public class AddElementsFromHdfsLoaderIT {

    @Nested
    public class FullSchemaAddElementsLoaderIT extends AddElementsFromHdfsLoader {
        public FullSchemaAddElementsLoaderIT() {
            super();
            this.schema = FULL_SCHEMA.getSchema();
            this.loader = new FullSchemaLoader();
        }
    }
    @Nested
    public class VisibilitySchemaAddElementsLoaderIT extends AddElementsFromHdfsLoader {
        public VisibilitySchemaAddElementsLoaderIT() {
            super();
            this.schema = VISIBILITY_SCHEMA.getSchema();
            this.loader = new VisibilitySchemaLoader();
        }
    }

    @Nested
    public class AggregationSchemaAddElementsLoaderIT extends AddElementsFromHdfsLoader {
        public AggregationSchemaAddElementsLoaderIT() {
            super();
            this.schema = AGGREGATION_SCHEMA.getSchema();
            this.loader = new AggregationSchemaLoader();
        }
    }

    @Nested
    public class BasicSchemaAddElementsLoaderIT extends AddElementsFromHdfsLoader {
        public BasicSchemaAddElementsLoaderIT() {
            super();
            this.schema = BASIC_SCHEMA.getSchema();
            this.loader = new BasicSchemaLoader();
        }
    }

    static class AddElementsFromHdfsLoader extends UserLoaderIT {
        @TempDir
        public File testFolder;

        private final Logger logger = LoggerFactory.getLogger(AddElementsFromHdfsLoader.class);

        private FileSystem fs;

        private String inputDir1;
        private String inputDir2;
        private String inputDir3;
        private String outputDir;
        private String failureDir;
        private String splitsFile;
        private String workingDir;

        @AfterEach
        void afterEach() {
            tearDown();
        }

        @Override
        public void _setup() throws Exception {
            fs = createFileSystem();

            final String root = fs.resolvePath(new Path("/")).toString()
                    .replaceFirst("/$", "")
                    + testFolder.getAbsolutePath();

            logger.info("using root dir: {}", root);

            inputDir1 = root + "/inputDir1";
            inputDir2 = root + "/inputDir2";
            inputDir3 = root + "/inputDir3";

            outputDir = root + "/outputDir";
            failureDir = root + "/failureDir";

            splitsFile = root + "/splitsDir/splits";
            workingDir = root + "/workingDir";
            super._setup();
        }

        @Test
        void shouldThrowExceptionWhenAddElementsFromHdfsWhenFailureDirectoryContainsFiles(final TestInfo testInfo) throws Exception {
            tearDown();
            fs.mkdirs(new Path(failureDir));
            try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(failureDir + "/someFile.txt"), true)))) {
                writer.write("Some content");
            }

            assertThatExceptionOfType(Exception.class)
                    .isThrownBy(() -> setup(testInfo))
                    .withStackTraceContaining("Failure directory is not empty: " + failureDir);
        }

        @Test
        void shouldAddElementsFromHdfsWhenDirectoriesAlreadyExist(TestInfo testInfo) throws Exception {
            // Given
            tearDown();
            fs.mkdirs(new Path(outputDir));
            fs.mkdirs(new Path(failureDir));

            // When
            setup(testInfo);

            // Then
            shouldGetAllElements();
        }

        @Test
        void shouldThrowExceptionWhenAddElementsFromHdfsWhenOutputDirectoryContainsFiles(final TestInfo testInfo) throws Exception {
            // Given
            tearDown();
            fs.mkdirs(new Path(outputDir));
            try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputDir + "/someFile.txt"), true)))) {
                writer.write("Some content");
            }

            // When
            assertThatExceptionOfType(Exception.class)
                    .isThrownBy(() -> setup(testInfo))
                    .withStackTraceContaining("Output directory exists and is not empty: " + outputDir);
        }

        @Override
        protected void addElements(final Iterable<? extends Element> elements) throws OperationException {
            createInputFile(elements);

            graph.execute(new AddElementsFromHdfs.Builder()
                    .addInputMapperPair(inputDir1, JsonMapperGenerator.class)
                    .addInputMapperPair(inputDir2, JsonMapperGenerator.class)
                    .addInputMapperPair(inputDir3, JsonMapperGenerator.class)
                    .outputPath(outputDir)
                    .failurePath(failureDir)
                    .jobInitialiser(new TextJobInitialiser())
                    .useProvidedSplits(false)
                    .splitsFilePath(splitsFile)
                    .workingPath(workingDir)
                    .build(), user);
        }

        private void createInputFile(final Iterable<? extends Element> elements) {
            final Path inputPath1 = new Path(inputDir1);
            final Path inputFilePath1 = new Path(inputDir1 + "/file.txt");
            final Path inputPath2 = new Path(inputDir2);
            final Path inputFilePath2 = new Path(inputDir2 + "/file.txt");
            final Path inputPath3 = new Path(inputDir3);
            final Path inputFilePath3 = new Path(inputDir3 + "/file.txt");

            try {
                fs.mkdirs(inputPath1);
                fs.mkdirs(inputPath2);
                fs.mkdirs(inputPath3);

                if (fs.exists(inputFilePath1)) {
                    fs.delete(inputFilePath1, false);
                }
                if (fs.exists(inputFilePath2)) {
                    fs.delete(inputFilePath2, false);
                }
                if (fs.exists(inputFilePath3)) {
                    fs.delete(inputFilePath3, false);
                }

                try (final BufferedWriter writer1 = new BufferedWriter(new OutputStreamWriter(fs.create(inputFilePath1, true)));
                     final BufferedWriter writer2 = new BufferedWriter(new OutputStreamWriter(fs.create(inputFilePath2, true)));
                     final BufferedWriter writer3 = new BufferedWriter(new OutputStreamWriter(fs.create(inputFilePath3, true)))) {
                    final Random rand = new Random();
                    for (final Element element : elements) {
                        final int randVal = rand.nextInt(3);
                        final BufferedWriter writer;
                        if (randVal == 0) {
                            writer = writer1;
                        } else if (randVal == 1) {
                            writer = writer2;
                        } else {
                            writer = writer3;
                        }
                        writer.write(StringUtil.toString(JSONSerialiser.serialise(element)) + "\n");
                    }
                }
            } catch (final IOException e) {
                throw new RuntimeException("Unable to create input files: " + e.getMessage(), e);
            }
        }

        private FileSystem createFileSystem() throws IOException {
            return FileSystem.get(new Configuration());
        }
    }
}
