/*
 * Copyright 2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.IntegerParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.StringParquetSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.junit.Assert.assertTrue;

public class WriteDataTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void testTwoWritesToSamePartitionDoesntThrowException() throws Exception {
        // Given
        final Schema schema = new Schema.Builder()
                .type("int", new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .serialiser(new IntegerParquetSerialiser())
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new StringParquetSerialiser())
                        .build())
                .entity("entity", new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .property("property1", "int")
                        .aggregate(false)
                        .build())
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .property("property2", "int")
                        .aggregate(false)
                        .build())
                .vertexSerialiser(new StringParquetSerialiser())
                .build();
        testFolder.create();
        final File tmpDir = testFolder.newFolder();
        final Function<String, String> groupToDirectory = group -> tmpDir.getAbsolutePath() + "/" + group;
        final List<Element> elements = new ArrayList<>();
        elements.add(new Entity.Builder()
                .group("entity")
                .vertex("A")
                .property("property1", 1)
                .build());
        elements.add(new Edge.Builder()
                .group("edge")
                .source("B")
                .dest("C")
                .property("property2", 100)
                .build());
        final WriteData writeData = new WriteData(groupToDirectory, schema, CompressionCodecName.GZIP);
        final FileSystem fileSystem = FileSystem.get(new Configuration());

        // When
        final ExecutorService executorService = Executors.newFixedThreadPool(3);
        final List<Callable<Void>> tasks = new ArrayList<>();
        LongStream.range(1000L, 1003L)
                .forEach(l -> {
                    tasks.add(() -> {
                        writeData.call(elements.iterator(), 1, l);
                        return null;
                    });
                });
        executorService.invokeAll(tasks);

        // Then
        // - Check that a file named with the partition id has been created
        assertTrue(fileSystem.exists(new Path(groupToDirectory.apply("entity") + "/" + "input-1.parquet")));
        assertTrue(fileSystem.exists(new Path(groupToDirectory.apply("edge") + "/" + "input-1.parquet")));
    }
}
