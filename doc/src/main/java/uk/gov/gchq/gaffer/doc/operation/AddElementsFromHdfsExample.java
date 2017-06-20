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

import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.AccumuloAddElementsFromHdfs;
import uk.gov.gchq.gaffer.doc.operation.generator.TextMapperGeneratorImpl;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.TextJobInitialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;

public class AddElementsFromHdfsExample extends OperationExample {
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
                + "```\n");
    }

    @Override
    public void runExamples() {
        addElementsFromHdfs();
        accumuloAddElementsFromHdfs();
    }

    public void addElementsFromHdfs() {
        // ---------------------------------------------------------
        final AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .addInputPath("/path/to/input/file")
                .outputPath("/path/to/output/folder")
                .failurePath("/path/to/failure/folder")
                .mapperGenerator(TextMapperGeneratorImpl.class)
                .jobInitialiser(new TextJobInitialiser())
                .build();
        // ---------------------------------------------------------

        showJavaExample(operation, null);
    }

    public void accumuloAddElementsFromHdfs() {
        // ---------------------------------------------------------
        final AddElementsFromHdfs operation = new AccumuloAddElementsFromHdfs.Builder()
                .addInputPath("/path/to/input/file")
                .addInputPath("/path/to/another/input/file")
                .addInputPath("/path/to/input/folder")
                .outputPath("/path/to/output/folder")
                .failurePath("/path/to/failure/folder")
                .mapperGenerator(TextMapperGeneratorImpl.class)
                .jobInitialiser(new TextJobInitialiser())
                .useProvidedSplits(false)
                .splitsFilePath("/path/to/splits/file")
                .maxReducers(100)
                .minReducers(10)
                .skipImport(true)
                .useAccumuloPartitioner(true)
                .build();
        // ---------------------------------------------------------

        showJavaExample(operation,
                "When running an " + AddElementsFromHdfs.class.getSimpleName()
                        + " on Accumulo, there are  additional options. "
                        + "There is a special Builder to help with these: AccumuloAddElementsFromHdfs.Builder().\n\n" +
                        "If you do not provide split points and the Accumulo table does not have a full set of split points then this operation will first sample the input data, generate split points and set them on the Accumulo table. " +
                        "It does this by delegating to " + SampleDataForSplitPoints.class.getSimpleName()
                        + " and " + SplitStore.class + "."
        );
    }
}
