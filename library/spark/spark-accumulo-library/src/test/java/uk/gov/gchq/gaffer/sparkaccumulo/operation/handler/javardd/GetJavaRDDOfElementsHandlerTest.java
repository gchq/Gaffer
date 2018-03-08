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
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfElements;
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

public class GetJavaRDDOfElementsHandlerTest {

    private static final String ENTITY_GROUP = "BasicEntity";
    private static final String EDGE_GROUP = "BasicEdge";

    @Test
    public void checkGetCorrectElementsInJavaRDDForEntityId() throws OperationException, IOException {
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
                    .property("count", 2)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(false)
                    .property("count", 4)
                    .build();

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity);
        }
        final User user = new User();
        graph1.execute(new AddElements.Builder().input(elements).build(), user);

        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        final String configurationString = AbstractGetRDDHandler
                .convertConfigurationToString(configuration);

        // Check get correct edges for "1"
        GetJavaRDDOfElements rddQuery = new GetJavaRDDOfElements.Builder()
                .input(Collections.singleton(new EntitySeed("1")))
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        JavaRDD<Element> rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>(rdd.collect());
        final Set<Element> expectedElements = new HashSet<>();
        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("1")
                .build();
        final Edge edge1B = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("1")
                .dest("B")
                .directed(false)
                .property("count", 2).build();
        final Edge edge1C = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("1")
                .dest("C")
                .directed(false)
                .property("count", 4).build();
        expectedElements.add(entity1);
        expectedElements.add(edge1B);
        expectedElements.add(edge1C);
        assertEquals(expectedElements, results);

        // Check get correct edges for "1" when specify entities only
        rddQuery = new GetJavaRDDOfElements.Builder()
                .input(new EntitySeed("1"))
                .view(new View.Builder()
                        .entity(ENTITY_GROUP)
                        .build())
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        results.clear();
        results.addAll(rdd.collect());
        expectedElements.clear();
        expectedElements.add(entity1);
        assertEquals(expectedElements, results);

        // Check get correct edges for "1" when specify edges only
        rddQuery = new GetJavaRDDOfElements.Builder()
                .input(new EntitySeed("1"))
                .view(new View.Builder()
                        .edge(EDGE_GROUP)
                        .build())
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        results.clear();
        results.addAll(rdd.collect());
        expectedElements.clear();
        expectedElements.add(edge1B);
        expectedElements.add(edge1C);
        assertEquals(expectedElements, results);

        // Check get correct edges for "1" and "5"
        rddQuery = new GetJavaRDDOfElements.Builder()
                .input(new EntitySeed("1"), new EntitySeed("5"))
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        results.clear();
        results.addAll(rdd.collect());
        final Entity entity5 = new Entity.Builder()
                .group(TestGroups.ENTITY).
                        vertex("5").build();
        final Edge edge5B = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("5")
                .dest("B")
                .directed(false)
                .property("count", 2).build();
        final Edge edge5C = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("5")
                .dest("C")
                .directed(false)
                .property("count", 4).build();
        expectedElements.clear();
        expectedElements.add(entity1);
        expectedElements.add(edge1B);
        expectedElements.add(edge1C);
        expectedElements.add(entity5);
        expectedElements.add(edge5B);
        expectedElements.add(edge5C);
        assertEquals(expectedElements, results);
    }

    @Test
    public void checkGetCorrectElementsInRDDForEdgeId() throws OperationException, IOException {
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
                    .property("count", 2)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(false)
                    .property("count", 4)
                    .build();

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity);
        }
        final User user = new User();

        graph1.execute(new AddElements.Builder().input(elements).build(), user);


        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        final String configurationString = AbstractGetRDDHandler
                .convertConfigurationToString(configuration);

        // Check get correct edges for EdgeSeed 1 -> B
        GetJavaRDDOfElements rddQuery = new GetJavaRDDOfElements.Builder()
                .input(new EdgeSeed("1", "B", false))
                .view(new View.Builder()
                        .edge(EDGE_GROUP)
                        .build())
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        JavaRDD<Element> rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>();
        results.addAll(rdd.collect());
        final Set<Element> expectedElements = new HashSet<>();
        final Edge edge1B = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("1")
                .dest("B")
                .directed(false)
                .property("count", 2)
                .build();
        expectedElements.add(edge1B);
        assertEquals(expectedElements, results);

        // Check get entity for 1 when query for 1 -> B and specify entities only
        rddQuery = new GetJavaRDDOfElements.Builder()
                .input(new EdgeSeed("1", "B", false))
                .view(new View.Builder()
                        .entity(ENTITY_GROUP)
                        .build())
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        results.clear();
        results.addAll(rdd.collect());
        expectedElements.clear();
        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("1").build();
        expectedElements.add(entity1);
        assertEquals(expectedElements, results);

        // Check get correct edges for 1 -> B when specify edges only
        rddQuery = new GetJavaRDDOfElements.Builder()
                .input(new EdgeSeed("1", "B", false))
                .view(new View.Builder()
                        .edge(EDGE_GROUP)
                        .build())
                .directedType(DirectedType.EITHER)
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        results.clear();
        results.addAll(rdd.collect());
        expectedElements.clear();
        expectedElements.add(edge1B);
        assertEquals(expectedElements, results);

        // Check get correct edges for 1 -> B and 5 -> C
        rddQuery = new GetJavaRDDOfElements.Builder()
                .view(new View.Builder()
                        .edge(EDGE_GROUP)
                        .build())
                .input(new EdgeSeed("1", "B", false), new EdgeSeed("5", "C", false))
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        results.clear();
        results.addAll(rdd.collect());
        final Edge edge5C = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("5")
                .dest("C")
                .directed(false)
                .property("count", 4)
                .build();
        expectedElements.clear();
        expectedElements.add(edge1B);
        expectedElements.add(edge5C);
        assertEquals(expectedElements, results);
    }
}
