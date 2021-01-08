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

package uk.gov.gchq.gaffer.hdfs.integration.loader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.TextJobInitialiser;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.JsonMapperGenerator;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.extensions.LoaderTestCase;
import uk.gov.gchq.gaffer.integration.template.loader.AbstractLoaderIT;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.user.User;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AddElementsFromHdfsLoaderIT should be extended for any store that wants to support AddElementsFromHDFS.
 */
public class AddElementsFromHdfsLoaderITTemplate extends AbstractLoaderIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsFromHdfsLoaderITTemplate.class);

    @TempDir
    public static File tempDir;

    private FileSystem fs;

    private String inputDir1;
    private String inputDir2;
    private String inputDir3;
    private String outputDir;
    private String failureDir;
    private String splitsFile;
    private String workingDir;

    @BeforeEach
    public void resetFileNames() throws IOException {
        fs = createFileSystem();

        final String root = fs.resolvePath(new Path(tempDir.getAbsolutePath())).toString();

        fs.delete(new Path(root), true);
        LOGGER.info("using root dir: " + root);

        inputDir1 = root + "/inputDir1";
        inputDir2 = root + "/inputDir2";
        inputDir3 = root + "/inputDir3";

        outputDir = root + "/outputDir";
        failureDir = root + "/failureDir";

        splitsFile = root + "/splitsDir/splits";
        workingDir = root + "/workingDir";
    }

    @GafferTest
    public void shouldThrowExceptionWhenAddElementsFromHdfsWhenFailureDirectoryContainsFiles(final LoaderTestCase testCase) throws Exception {
        fs.mkdirs(new Path(failureDir));
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(failureDir + "/someFile.txt"), true)))) {
            writer.write("Some content");
        }

        OperationException e = assertThrows(OperationException.class, () -> beforeEveryTest(testCase));
        assertEquals("Failure directory is not empty: " + failureDir, e.getCause().getMessage());
    }

    @GafferTest
    public void shouldAddElementsFromHdfsWhenDirectoriesAlreadyExist(final LoaderTestCase testCase) throws Exception {
        // Given
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(failureDir));

        // When beforeEveryTest is run

        // Then
        shouldGetAllElements(testCase);
    }

    @GafferTest
    public void shouldThrowExceptionWhenAddElementsFromHdfsWhenOutputDirectoryContainsFiles(final LoaderTestCase testCase) throws Exception {
        // Given
        fs.mkdirs(new Path(outputDir));
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputDir + "/someFile.txt"), true)))) {
            writer.write("Some content");
        }

        // When
        Exception e = assertThrows(Exception.class, () -> beforeEveryTest(testCase));
        assertTrue(e.getMessage().contains("Output directory exists and is not empty: " + outputDir), e.getMessage());
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

    @Override
    protected void addElements(final Graph graph, final Iterable<? extends Element> input) throws OperationException {
        createInputFile(input);

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
            .build(), new User());
    }
}
