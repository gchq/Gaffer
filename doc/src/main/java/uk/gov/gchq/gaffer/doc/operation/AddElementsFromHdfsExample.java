/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.doc.operation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.doc.operation.generator.TextMapperGeneratorImpl;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.TextJobInitialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.user.User;
import java.nio.file.Paths;

public class AddElementsFromHdfsExample extends OperationExample {
    private final String[] args = new String[5];

    public static void main(final String[] args) throws OperationException {
        new AddElementsFromHdfsExample().run();
    }

    public AddElementsFromHdfsExample() {
        super(AddElementsFromHdfs.class, "This operation must be run as a Hadoop Job. " +
                "So you will need to package up a shaded jar containing a main method " +
                "that creates an instance of Graph and executes the operation. " +
                "It can then be run with: \n\n"
                + "```bash\n"
                + "hadoop jar custom-shaded-jar.jar\n"
                + "```\n\n"
                + "When running an " + AddElementsFromHdfs.class.getSimpleName()
                + " on Accumulo, if you do not specify useProvidedSplits " +
                "and the Accumulo table does not have a full set of split points " +
                "then this operation will first sample the input data, generate " +
                "split points and set them on the Accumulo table. " +
                "It does this by delegating to " + SampleDataForSplitPoints.class.getSimpleName()
                + " and " + SplitStore.class + ".");
    }

    @Override
    public void runExamples() {
        addElementsFromHdfs();
        addElementsFromHdfsMainMethod();
    }

    @SuppressFBWarnings("REC_CATCH_EXCEPTION")
    private void addElementsFromHdfsMainMethod() {
        try {
            // ---------------------------------------------------------
            if (5 != args.length) {
                System.err.println("Usage: hadoop jar custom-hdfs-import-<version>-shaded.jar <inputPath> <outputPath> <failurePath> <schemaPath> <storePropertiesPath>");
                System.exit(1);
            }

            final String inputPath = args[0];
            final String outputPath = args[1];
            final String failurePath = args[2];
            final String schemaPath = args[3];
            final String storePropertiesPath = args[4];

            final Graph graph = new Graph.Builder()
                    .storeProperties(storePropertiesPath)
                    .addSchemas(Paths.get(schemaPath))
                    .build();

            final AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                    .addInputPath(inputPath)
                    .outputPath(outputPath)
                    .failurePath(failurePath)
                    .splitsFilePath("/tmp/splits")
                    .workingPath("/tmp/workingDir")
                    .useProvidedSplits(false)
                    .mapperGenerator(TextMapperGeneratorImpl.class)
                    .jobInitialiser(new TextJobInitialiser())
                    .minReducers(10)
                    .maxReducers(100)
                    .build();

            graph.execute(operation, new User());
            // ---------------------------------------------------------
        } catch (final Exception e) {
            // ignore error
        }

        showJavaExample("Example content for a main method that takes 5 arguments and runs an " + AddElementsFromHdfs.class.getSimpleName());
    }

    @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
    public void addElementsFromHdfs() {
        // ---------------------------------------------------------
        final AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .addInputPath("/path/to/input/fileOrFolder")
                .outputPath("/path/to/output/folder")
                .failurePath("/path/to/failure/folder")
                .splitsFilePath("/path/to/splits/file")
                .workingPath("/tmp/workingDir")
                .useProvidedSplits(false)
                .mapperGenerator(TextMapperGeneratorImpl.class)
                .jobInitialiser(new TextJobInitialiser())
                .minReducers(10)
                .maxReducers(100)
                .build();
        // ---------------------------------------------------------

        showJavaExample(null);
    }
}
