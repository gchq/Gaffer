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

package uk.gov.gchq.gaffer.sparkaccumulo.generator;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.spark.data.generator.RowToElementGenerator;
import uk.gov.gchq.gaffer.spark.function.GraphFrameToIterableRow;
import uk.gov.gchq.gaffer.spark.operation.graphframe.GetGraphFrameOfElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RowToElementGeneratorTest {
    @Before
    public void before() {
        SparkSessionProvider.getSparkSession();
    }

    @Test
    public void checkGetCorrectElementsInGraphFrame() throws OperationException {
        // Given
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());

        final GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(gfOperation)
                .then(new Map.Builder<>()
                        .first(new GraphFrameToIterableRow())
                        .build())
                .then(new GenerateElements.Builder<Row>()
                        .generator(new RowToElementGenerator())
                        .build())
                .build();

        // When
        final Iterable<? extends Element> result = graph.execute(opChain, new User());

        // Then
        ElementUtil.assertElementEquals(getElements(), result);
    }

    private List<Element> getElements() {
        final List<String> names = Lists.newArrayList("Alice", "Bob", "Charlie", "David");
        final List<Element> elements = new ArrayList<>();

        final List<Entity> entities = names.stream().map(n -> {
            return new Entity.Builder().vertex(n.substring(0, 1).toLowerCase()).group(TestGroups.ENTITY).property("fullname", n).build();
        }).collect(Collectors.toList());

        final Edge edge1 = new Edge.Builder().source("a").dest("b").directed(true).group(TestGroups.EDGE)
                .property("type", "friend").build();
        final Edge edge2 = new Edge.Builder().source("b").dest("c").directed(false).group(TestGroups.EDGE)
                .property("type", "follow").build();
        final Edge edge3 = new Edge.Builder().source("a").dest("c").directed(true).group(TestGroups.EDGE)
                .property("type", "follow").build();
        final Edge edge4 = new Edge.Builder().source("c").dest("a").directed(true).group(TestGroups.EDGE)
                .property("type", "follow").build();
        final Edge edge5 = new Edge.Builder().source("d").dest("c").directed(true).group(TestGroups.EDGE)
                .property("type", "follow").build();

        final List<Edge> edges = Lists.newArrayList(edge1, edge2, edge3, edge4, edge5);

        elements.addAll(entities);
        elements.addAll(edges);

        return elements;
    }


    private Graph getGraph(final String elementsSchema, final List<Element> elements) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getClass().getResourceAsStream(elementsSchema))
                .addSchema(getClass().getResourceAsStream("/schema-GraphFrame/types.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();
        graph.execute(new AddElements.Builder().input(elements).build(), new User());
        return graph;
    }
}
