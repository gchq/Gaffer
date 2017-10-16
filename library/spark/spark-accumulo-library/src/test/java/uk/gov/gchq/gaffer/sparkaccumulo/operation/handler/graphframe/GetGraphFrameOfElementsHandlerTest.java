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

package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.graphframe;

import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.operation.graphframe.GetGraphFrameOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.SparkSessionProvider;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;

public class GetGraphFrameOfElementsHandlerTest {

    private static final String ENTITY_GROUP = "BasicEntity";
    private static final String EDGE_GROUP = "BasicEdge";
    private static final String EDGE_GROUP2 = "BasicEdge2";
    private static final int NUM_ELEMENTS = 10;

    @Test
    public void checkGetCorrectElementsInGraphFrame() throws OperationException {
        final Graph graph = getGraph("/schema-DataFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        // Edges group - check get correct edges
        GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .sparkSession(sparkSession)
                .view(new View.Builder().edge(EDGE_GROUP).entity(ENTITY_GROUP).build())
                .build();
        GraphFrame graphFrame = graph.execute(gfOperation, new User());

        graphFrame.vertices().show();
        graphFrame.edges().show();
    }

    private Graph getGraph(final String elementsSchema, final List<Element> elements) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getClass().getResourceAsStream(elementsSchema))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/types.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();
        graph.execute(new AddElements.Builder().input(elements).build(), new User());
        return graph;
    }

    static List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final Entity entity = new Entity.Builder().group(TestGroups.ENTITY)
                    .vertex("" + i)
                    .property("columnQualifier", 1)
                    .property("property1", i)
                    .property("property2", 3.0F)
                    .property("property3", 4.0D)
                    .property("property4", 5L)
                    .property("count", 6L)
                    .build();

            final Edge edge1 = new Edge.Builder().group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("B")
                    .directed(true)
                    .property("columnQualifier", 1)
                    .property("property1", 2)
                    .property("property2", 3.0F)
                    .property("property3", 4.0D)
                    .property("property4", 5L)
                    .property("count", 100L)
                    .build();

            final Edge edge2 = new Edge.Builder().group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(true)
                    .property("columnQualifier", 6)
                    .property("property1", 7)
                    .property("property2", 8.0F)
                    .property("property3", 9.0D)
                    .property("property4", 10L)
                    .property("count", i * 200L)
                    .build();

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity);
        }
        return elements;
    }





}
