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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.SparkConstants;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.scalardd.ImportKeyValuePairRDDToAccumulo;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.scala.ElementConverterFunction;
import uk.gov.gchq.gaffer.user.User;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ImportKeyValuePairRDDToAccumuloHandlerTest {

    private static final ClassTag<Element> ELEMENT_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(Element.class);
    private static final ClassTag<Tuple2<Key, Value>> TUPLE2_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);
    private static final ClassTag<AccumuloElementConverter> ACCUMULO_ELEMENT_CONVERTER_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(AccumuloElementConverter.class);

    @Test
    public void checkImportRDDOfElements() throws OperationException, IOException {
        final Graph graph1 = new Graph.Builder()
                .graphId("graphId")
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

        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("tests")
                .set(SparkConstants.SERIALIZER, SparkConstants.DEFAULT_SERIALIZER)
                .set(SparkConstants.KRYO_REGISTRATOR, SparkConstants.DEFAULT_KRYO_REGISTRATOR)
                .set(SparkConstants.DRIVER_ALLOW_MULTIPLE_CONTEXTS, "true");
        final SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        configuration.write(new DataOutputStream(baos));
        final String configurationString = new String(baos.toByteArray(), CommonConstants.UTF_8);
        final String outputPath = this.getClass().getResource("/").getPath().toString() + "load" + Math.random();
        final String failurePath = this.getClass().getResource("/").getPath().toString() + "failure" + Math.random();
        final File file = new File(outputPath);
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }

        final ElementConverterFunction func = new ElementConverterFunction(sparkSession.sparkContext().broadcast(new ByteEntityAccumuloElementConverter(graph1.getSchema()), ACCUMULO_ELEMENT_CONVERTER_CLASS_TAG));
        final RDD<Tuple2<Key, Value>> elementRDD = sparkSession.sparkContext().parallelize(elements, 1, ELEMENT_CLASS_TAG).flatMap(func, TUPLE2_CLASS_TAG);
        final ImportKeyValuePairRDDToAccumulo addRdd = new ImportKeyValuePairRDDToAccumulo.Builder()
                .input(elementRDD)
                .outputPath(outputPath)
                .failurePath(failurePath)
                .build();
        graph1.execute(addRdd, user);
        FileUtils.forceDeleteOnExit(file);

        // Check all elements were added
        final GetRDDOfAllElements rddQuery = new GetRDDOfAllElements.Builder()
                .sparkSession(sparkSession)
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
        sparkSession.stop();
    }
}