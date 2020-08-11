/*
 * Copyright 2020 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.javardd;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfAllElements;
import uk.gov.gchq.gaffer.spark.operation.javardd.ImportJavaRDDOfElements;
import uk.gov.gchq.gaffer.spark.operation.javardd.SplitStoreFromJavaRDDOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.AbstractPropertiesDrivenTest;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.gchq.gaffer.sparkaccumulo.AbstractPropertiesDrivenTest.setUpBeforeClass;
import static uk.gov.gchq.gaffer.sparkaccumulo.AbstractPropertiesDrivenTest.tearDownAfterClass;

public class SplitStoreFromJavaRDDOfElementsHandlerTest extends AbstractPropertiesDrivenTest {

    private final User user = new User();

    private String outputPath;
    private String failurePath;
    private Graph graph;
    private List<Element> elements;
    private JavaRDD<Element> javaRDD;
    private String configurationString;

    @TempDir
    static Path tempDir;

    @BeforeAll
    public static void setup() throws IOException {
        setUpBeforeClass("/store.properties", Files.createDirectories(tempDir.resolve("accumulo_temp_dir")));
    }

    @AfterAll
    public static void teardown() {
        tearDownAfterClass();
    }

    @BeforeEach
    public void setUp() throws IOException {
        graph = createGraph();
        elements = createElements();
        javaRDD = createJavaRDDContaining(elements);
        configurationString = createConfigurationString();
        outputPath = tempDir.resolve("output").toAbsolutePath().toString();
        failurePath = tempDir.resolve("failure").toAbsolutePath().toString();
    }

    private Graph createGraph() {

        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getClass().getResourceAsStream("/schema/elements.json"))
                .addSchema(getClass().getResourceAsStream("/schema/types.json"))
                .addSchema(getClass().getResourceAsStream("/schema/serialisation.json"))
                .storeProperties(getStoreProperties())
                .build();
    }

    private List<Element> createElements() {

        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("" + i)
                    .build();

            final Edge edge1 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("B")
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 2)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 4)
                    .build();

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity);
        }

        return elements;
    }

    private JavaRDD<Element> createJavaRDDContaining(final List<Element> elements) {

        return JavaSparkContext.fromSparkContext(SparkSessionProvider.getSparkSession().sparkContext()).parallelize(elements);
    }

    private String createConfigurationString() throws IOException {

        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        return AbstractGetRDDHandler.convertConfigurationToString(configuration);
    }

    @Test
    public void throwsExceptionWhenNumSplitPointsIsLessThanOne() throws OperationException {
        final SplitStoreFromJavaRDDOfElements splitStoreHandler = new SplitStoreFromJavaRDDOfElements.Builder()
                .input(javaRDD)
                .numSplits(-1)
                .build();
        IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                () -> graph.execute(splitStoreHandler, user));
        assertTrue(actual.getMessage().contains("numSplits must be null or greater than 0"));
    }

    @Test
    public void throwsExceptionWhenMaxSampleSizeIsLessThanOne() throws OperationException {
        final SplitStoreFromJavaRDDOfElements splitStoreHandler = new SplitStoreFromJavaRDDOfElements.Builder()
                .input(javaRDD)
                .maxSampleSize(-1)
                .build();
        IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                () -> graph.execute(splitStoreHandler, user));
        assertTrue(actual.getMessage().contains("maxSampleSize must be null or greater than 0"));
    }

    @Test
    public void throwsExceptionWhenFractionToSampleIsGreaterThanOne() throws OperationException {
        final SplitStoreFromJavaRDDOfElements splitStoreHandler = new SplitStoreFromJavaRDDOfElements.Builder()
                .input(javaRDD)
                .fractionToSample(1.000000001d)
                .build();
        IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                () -> graph.execute(splitStoreHandler, user));
        assertTrue(actual.getMessage().contains("fractionToSample must be null or between 0 exclusive and 1 inclusive"));
    }

    @Test
    public void throwsExceptionWhenFractionToSampleIsZero() throws OperationException {
        final SplitStoreFromJavaRDDOfElements splitStoreHandler = new SplitStoreFromJavaRDDOfElements.Builder()
                .input(javaRDD)
                .fractionToSample(0d)
                .build();
        IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                () -> graph.execute(splitStoreHandler, user));
        assertTrue(actual.getMessage().contains("fractionToSample must be null or between 0 exclusive and 1 inclusive"));
    }

    @Test
    public void throwsExceptionWhenFractionToSampleLessThanZero() throws OperationException {
        final SplitStoreFromJavaRDDOfElements splitStoreHandler = new SplitStoreFromJavaRDDOfElements.Builder()
                .input(javaRDD)
                .fractionToSample(-0.00000001d)
                .build();
        IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                () -> graph.execute(splitStoreHandler, user));
        assertTrue(actual.getMessage().contains("fractionToSample must be null or between 0 exclusive and 1 inclusive"));
    }

    @Test
    public void canBeSuccessfullyChainedWithImport() throws Exception {

        graph.execute(new OperationChain.Builder()
                .first(new SplitStoreFromJavaRDDOfElements.Builder()
                        .input(javaRDD)
                        .build())
                .then(new ImportJavaRDDOfElements.Builder()
                        .input(javaRDD)
                        .option("outputPath", outputPath)
                        .option("failurePath", failurePath)
                        .build()).build(), user);

        // Check all elements were added
        final GetJavaRDDOfAllElements rddQuery = new GetJavaRDDOfAllElements.Builder()
                .option(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString)
                .build();

        final JavaRDD<Element> rdd = graph.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>(rdd.collect());
        assertEquals(elements.size(), results.size());
    }
}
