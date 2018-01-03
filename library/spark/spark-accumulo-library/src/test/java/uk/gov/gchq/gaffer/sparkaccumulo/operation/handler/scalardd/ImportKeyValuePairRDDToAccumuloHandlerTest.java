/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Tuple2;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.spark.operation.dataframe.ClassTagConstants;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.scalardd.ImportKeyValuePairRDDToAccumulo;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.scala.ElementConverterFunction;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ImportKeyValuePairRDDToAccumuloHandlerTest {
    private static final ClassTag<Element> ELEMENT_CLASS_TAG = ClassTagConstants.ELEMENT_CLASS_TAG;
    private static final ClassTag<Tuple2<Key, Value>> TUPLE2_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);
    private static final ClassTag<AccumuloElementConverter> ACCUMULO_ELEMENT_CONVERTER_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(AccumuloElementConverter.class);
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void checkImportRDDOfElements() throws OperationException, IOException {
        final Graph graph1 = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getClass().getResourceAsStream("/schema/elements.json"))
                .addSchema(getClass().getResourceAsStream("/schema/types.json"))
                .addSchema(getClass().getResourceAsStream("/schema/serialisation.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final ArrayBuffer<Element> elements = new ArrayBuffer<>();
        for (int i = 0; i < 10; i++) {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("" + i)
                    .build();

            final Edge edge1 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("B").directed(false)
                    .property(TestPropertyNames.COUNT, 2)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 4)
                    .build();

            elements.$plus$eq(edge1);
            elements.$plus$eq(edge2);
            elements.$plus$eq(entity);
        }
        final User user = new User();

        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        final String configurationString = AbstractGetRDDHandler
                .convertConfigurationToString(configuration);

        final String outputPath = testFolder.getRoot().getAbsolutePath() + "/output";
        final String failurePath = testFolder.getRoot().getAbsolutePath() + "/failure";

        final ElementConverterFunction func = new ElementConverterFunction(sparkSession.sparkContext().broadcast(new ByteEntityAccumuloElementConverter(graph1.getSchema()), ACCUMULO_ELEMENT_CONVERTER_CLASS_TAG));
        final RDD<Tuple2<Key, Value>> elementRDD = sparkSession.sparkContext().parallelize(elements, 1, ELEMENT_CLASS_TAG).flatMap(func, TUPLE2_CLASS_TAG);
        final ImportKeyValuePairRDDToAccumulo addRdd = new ImportKeyValuePairRDDToAccumulo.Builder()
                .input(elementRDD)
                .outputPath(outputPath)
                .failurePath(failurePath)
                .build();
        graph1.execute(addRdd, user);

        // Check all elements were added
        final GetRDDOfAllElements rddQuery = new GetRDDOfAllElements.Builder()
                .option(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString)
                .build();

        final RDD<Element> rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>();
        final Element[] returnedElements = (Element[]) rdd.collect();
        Collections.addAll(results, returnedElements);
        assertEquals(elements.size(), results.size());
    }
}