/*
 * Copyright 2016-2018 Crown Copyright
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
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfAllElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.user.User;

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
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getClass().getResourceAsStream("/schema/elements.json"))
                .addSchema(getClass().getResourceAsStream("/schema/types.json"))
                .addSchema(getClass().getResourceAsStream("/schema/serialisation.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final List<Element> elements = new ArrayList<>();
        final Set<Element> expectedElements = new HashSet<>();
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

            expectedElements.add(edge1);
            expectedElements.add(edge2);
            expectedElements.add(entity);
        }
        final User user = new User();
        graph1.execute(new AddElements.Builder().input(elements).build(), user);


        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        final String configurationString = AbstractGetRDDHandler
                .convertConfigurationToString(configuration);

        // Check get correct edges for "1"
        final GetJavaRDDOfAllElements rddQuery = new GetJavaRDDOfAllElements.Builder()
                .build();

        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        final JavaRDD<Element> rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>(rdd.collect());
        assertEquals(expectedElements, results);
    }

    @Test
    public void checkGetAllElementsInJavaRDDWithVisibility() throws OperationException, IOException {
        final Graph graph1 = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getClass().getResourceAsStream("/schema/elementsWithVisibility.json"))
                .addSchema(getClass().getResourceAsStream("/schema/types.json"))
                .addSchema(getClass().getResourceAsStream("/schema/serialisation.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("" + i)
                    .property("visibility", "public")
                    .build();

            final Edge edge1 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("B")
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 2)
                    .property("visibility", "private")
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 4)
                    .property("visibility", "public").build();

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity);
        }
        final User user = new User("user", Collections.singleton("public"));
        graph1.execute(new AddElements.Builder().input(elements).build(), user);


        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        final String configurationString = AbstractGetRDDHandler
                .convertConfigurationToString(configuration);

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
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        rdd = graph1.execute(rddQuery, userWithPrivate);
        if (rdd == null) {
            fail("No RDD returned");
        }
        results.clear();
        results.addAll(rdd.collect());
        assertEquals(expectedElementsPrivate, results);
    }
}
