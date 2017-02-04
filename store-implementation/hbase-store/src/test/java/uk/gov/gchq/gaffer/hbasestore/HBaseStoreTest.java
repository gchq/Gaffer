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

package uk.gov.gchq.gaffer.hbasestore;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.function.transform.Concat;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

public class HBaseStoreTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStore.class);

    @Test
    public void shouldGetAllElementsWithFilters() throws StoreException, OperationException {
        final Graph graph = new Graph.Builder()
                .addSchemas(StreamUtil.schemas(HBaseStoreTest.class))
                .storeProperties(StreamUtil.storeProps(HBaseStoreTest.class))
                .build();
        addElements(graph);

        final User user = new User.Builder()
                .userId("publicUser")
                .dataAuth("public")
                .build();
        final CloseableIterable<Element> elements = graph.execute(new GetAllElements.Builder<>()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("property1")
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build(), user);
        for (Element element : elements) {
            LOGGER.info("Element: " + element);
        }
    }

    @Test
    public void shouldGetEntity() throws StoreException, OperationException {
        final Graph graph = new Graph.Builder()
                .addSchemas(StreamUtil.schemas(HBaseStoreTest.class))
                .storeProperties(StreamUtil.storeProps(HBaseStoreTest.class))
                .build();
        addElements(graph);

        final User user = new User.Builder()
                .userId("user")
                .dataAuth("public")
                .build();
        final CloseableIterable<Element> elements = graph.execute(new GetElements.Builder<>()
                .addSeed(new EntitySeed("vertex1"))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                        .transientProperty("transientProp", String.class)
                                        .transformer(new ElementTransformer.Builder()
                                                .select(IdentifierType.VERTEX.name(), "property1")
                                                .execute(new Concat())
                                                .project("transientProp")
                                                .build())
                                        .build()
                        )
                        .build())
                .build(), user);
        for (Element element : elements) {
            LOGGER.info("Element: " + element);
        }
    }

    private void addElements(final Graph graph) throws OperationException {
        graph.execute(new AddElements.Builder()
                .elements(new Edge.Builder()
                                .source("source1")
                                .dest("dest1")
                                .group(TestGroups.EDGE)
                                .property("property1", 1)
                                .property("columnQualifier", 10)
                                .property("visibility", "private")
                                .build(),
                        new Edge.Builder()
                                .source("source2")
                                .dest("dest2")
                                .group(TestGroups.EDGE_2)
                                .property("property1", 5)
                                .property("columnQualifier", 10)
                                .property("visibility", "public")
                                .build(),
                        new Edge.Builder()
                                .source("source3")
                                .dest("dest3")
                                .group(TestGroups.EDGE_2)
                                .property("property1", 10)
                                .property("columnQualifier", 10)
                                .property("visibility", "public")
                                .build(),
                        new Entity.Builder()
                                .vertex("vertex1")
                                .group(TestGroups.ENTITY)
                                .property("property1", 2)
                                .property("columnQualifier", 20)
                                .property("visibility", "public")
                                .build(),
                        new Entity.Builder()
                                .vertex("vertex2")
                                .group(TestGroups.ENTITY)
                                .property("property1", 2)
                                .property("columnQualifier", 20)
                                .property("visibility", "public")
                                .build())
                .build(), new User());
    }
}
