/*
 * Copyright 2016-2017 Crown Copyright
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

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfAllElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.user.User;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GetJavaRDDOfAllElementsHandlerTest {

    @Test
    public void checkGetAllElementsInJavaRDD() throws OperationException, IOException {
        final Graph graph1 = new Graph.Builder()
                .addSchema(getClass().getResourceAsStream("/schema/dataSchema.json"))
                .addSchema(getClass().getResourceAsStream("/schema/dataTypes.json"))
                .addSchema(getClass().getResourceAsStream("/schema/storeTypes.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final List<Element> elements = new ArrayList<>();
        final Set<Element> expectedElements = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            final Entity entity = new Entity(TestGroups.ENTITY);
            entity.setVertex("" + i);

            final Edge edge1 = new Edge(TestGroups.EDGE);
            edge1.setSource("" + i);
            edge1.setDestination("B");
            edge1.setDirected(false);
            edge1.putProperty(TestPropertyNames.COUNT, 2);

            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("" + i);
            edge2.setDestination("C");
            edge2.setDirected(false);
            edge2.putProperty(TestPropertyNames.COUNT, 4);

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity);

            expectedElements.add(edge1);
            expectedElements.add(edge2);
            expectedElements.add(entity);
        }
        final User user = new User();
        graph1.execute(new AddElements(elements), user);

        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("testCheckGetCorrectElementsInJavaRDDForEntitySeed")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        configuration.write(new DataOutputStream(baos));
        final String configurationString = new String(baos.toByteArray(), CommonConstants.UTF_8);

        // Check get correct edges for "1"
        final GetJavaRDDOfAllElements rddQuery = new GetJavaRDDOfAllElements.Builder()
                .javaSparkContext(sparkContext)
                .build();

        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        final JavaRDD<Element> rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>(rdd.collect());
        assertEquals(expectedElements, results);

        sparkContext.stop();
    }

    @Test
    public void checkGetAllElementsInJavaRDDWithVisibility() throws OperationException, IOException {
        final Graph graph1 = new Graph.Builder()
                .addSchema(getClass().getResourceAsStream("/schema/dataSchemaWithVisibility.json"))
                .addSchema(getClass().getResourceAsStream("/schema/dataTypes.json"))
                .addSchema(getClass().getResourceAsStream("/schema/storeTypes.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            final Entity entity = new Entity(TestGroups.ENTITY);
            entity.setVertex("" + i);
            entity.putProperty("visibility", "public");

            final Edge edge1 = new Edge(TestGroups.EDGE);
            edge1.setSource("" + i);
            edge1.setDestination("B");
            edge1.setDirected(false);
            edge1.putProperty(TestPropertyNames.COUNT, 2);
            edge1.putProperty("visibility", "private");

            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("" + i);
            edge2.setDestination("C");
            edge2.setDirected(false);
            edge2.putProperty(TestPropertyNames.COUNT, 4);
            edge2.putProperty("visibility", "public");

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity);
        }
        final User user = new User("user", Collections.singleton("public"));
        graph1.execute(new AddElements(elements), user);

        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("testCheckGetCorrectElementsInJavaRDDForEntitySeed")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        configuration.write(new DataOutputStream(baos));
        final String configurationString = new String(baos.toByteArray(), CommonConstants.UTF_8);

        // Create user with just public auth, and user with both private and public
        final Set<String> publicNotPrivate = new HashSet<>();
        publicNotPrivate.add("public");
        final User userWithPublicNotPrivate = new User("user1", publicNotPrivate);
        final Set<String> privateAuth = new HashSet<>();
        privateAuth.add("public");
        privateAuth.add("private");
        final User userWithPrivate = new User("user2", privateAuth);

        // Calculate correct results for 2 users
        final Set<Element> expectedElementsPublicNotPrivate = new HashSet<>();
        final Set<Element> expectedElementsPrivate = new HashSet<>();
        for (final Element element : elements) {
            expectedElementsPrivate.add(element);
            if (element.getProperty("visibility").equals("public")) {
                expectedElementsPublicNotPrivate.add(element);
            }
        }

        // Check get correct edges for user with just public
        GetJavaRDDOfAllElements rddQuery = new GetJavaRDDOfAllElements.Builder()
                .javaSparkContext(sparkContext)
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        JavaRDD<Element> rdd = graph1.execute(rddQuery, userWithPublicNotPrivate);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>(rdd.collect());
        assertEquals(expectedElementsPublicNotPrivate, results);

        // Check get correct edges for user with both private and public
        rddQuery = new GetJavaRDDOfAllElements.Builder()
                .javaSparkContext(sparkContext)
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        rdd = graph1.execute(rddQuery, userWithPrivate);
        if (rdd == null) {
            fail("No RDD returned");
        }
        results.clear();
        results.addAll(rdd.collect());
        assertEquals(expectedElementsPrivate, results);

        sparkContext.stop();
    }
}
