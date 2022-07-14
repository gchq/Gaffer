/*
 * Copyright 2019-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.integration;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO_WITH_EDGES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO_WITH_ENTITIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_EDGE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;

/**
 * In all of theses tests the Federated graph contains two graphs, one containing
 * a schema with only edges the other with only entities.
 */
public class FederatedViewsIT extends AbstractStandaloneFederatedStoreIT {

    private static final AccumuloProperties ACCUMULO_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(FederatedViewsIT.class, "properties/singleUseAccumuloStore.properties"));

    @Override
    protected Schema createSchema() {
        final Schema.Builder schemaBuilder = new Schema.Builder(AbstractStoreIT.createDefaultSchema());
        schemaBuilder.edges(Collections.emptyMap());
        schemaBuilder.entities(Collections.emptyMap());
        schemaBuilder.json(StreamUtil.openStream(FederatedViewsIT.class, "schema/basicEdgeSchema.json"));
        schemaBuilder.json(StreamUtil.openStream(FederatedViewsIT.class, "schema/basicEntitySchema.json"));
        return schemaBuilder.build();
    }

    @Test
    public void shouldBeEmptyAtStart() throws OperationException {

        final Iterable<? extends Element> edges = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE)
                        .build())
                .build(), user);

        assertThat(edges.iterator().hasNext()).isFalse();

        final Iterable<? extends Element> entities = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(GROUP_BASIC_ENTITY)
                        .build())
                .build(), user);

        assertThat(entities.iterator().hasNext()).isFalse();

    }

    /**
     * Federation acts as a Edge/Entity graph with a view of Edge
     *
     * @throws OperationException any
     */
    @Test
    public void shouldAddAndGetEdge() throws OperationException {

        addBasicEdge();

        final Iterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE)
                        .build())
                .build(), user);

        assertThat(rtn.iterator().hasNext()).isTrue();

    }

    /**
     * Federation acts as a Edge/Entity graph with a view of Entity
     *
     * @throws OperationException any
     */
    @Test
    public void shouldAddAndGetEntity() throws OperationException {

        addBasicEntity();

        final Iterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(GROUP_BASIC_ENTITY)
                        .build())
                .build(), user);

        assertThat(rtn.iterator().hasNext()).isTrue();

    }

    /**
     * Federation acts as a Edge graph with a view of Edge
     *
     * @throws OperationException any
     */
    @Test
    public void shouldAddAndGetEdgeWithEdgeGraph() throws OperationException {

        addBasicEdge();

        final Iterable<? extends Element> rtn = graph.execute(getFederatedOperation(new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE)
                        .build())
                .build())
                .graphIdsCSV(GRAPH_ID_ACCUMULO_WITH_EDGES), user);

        assertThat(rtn.iterator().hasNext()).isTrue();

    }

    /**
     * Federation acts as a Entity graph with a view of Entity
     *
     * @throws OperationException any
     */
    @Test
    public void shouldAddAndGetEntityWithEntityGraph() throws OperationException {

        addBasicEntity();

        final Iterable<? extends Element> rtn = graph.execute(getFederatedOperation(new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(GROUP_BASIC_ENTITY)
                        .build())
                .build())
                .graphIdsCSV(GRAPH_ID_ACCUMULO_WITH_ENTITIES), user);

        assertThat(rtn.iterator().hasNext()).isTrue();

    }

    /**
     * Federation acts as a Entity graph with a view of Edge
     *
     * @throws OperationException any
     */
    @Test
    public void shouldNotAddAndGetEdgeWithEntityGraph() throws OperationException {

        addBasicEdge();

        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> graph.execute(getFederatedOperation(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .edge(GROUP_BASIC_EDGE)
                                .build())
                        .build())
                        .graphIdsCSV(GRAPH_ID_ACCUMULO_WITH_ENTITIES), user))
                .withMessage("Operation chain is invalid. Validation errors: \n" +
                        "View is not valid for graphIds:[" + GRAPH_ID_ACCUMULO_WITH_ENTITIES + "]\n" +
                        "(graphId: " + GRAPH_ID_ACCUMULO_WITH_ENTITIES + ") View for operation uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation is not valid. \n" +
                        "(graphId: " + GRAPH_ID_ACCUMULO_WITH_ENTITIES + ") Edge group BasicEdge does not exist in the schema");
    }


    /**
     * Federation acts as a Edge graph with a view of Entity
     *
     * @throws OperationException any
     */
    @Test
    public void shouldNotAddAndGetEntityWithEdgeGraph() throws OperationException {

        addBasicEntity();

        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> graph.execute(FederatedStoreUtil.<GetAllElements>getFederatedOperation(new GetAllElements.Builder()
                                .view(new View.Builder()
                                        .entity(GROUP_BASIC_ENTITY)
                                        .build())
                                .build())
                        .graphIdsCSV(GRAPH_ID_ACCUMULO_WITH_EDGES), user))

                .withMessage("Operation chain is invalid. Validation errors: \n" +
                        "View is not valid for graphIds:[" + GRAPH_ID_ACCUMULO_WITH_EDGES + "]\n" +
                        "(graphId: " + GRAPH_ID_ACCUMULO_WITH_EDGES + ") View for operation uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation is not valid. \n" +
                        "(graphId: " + GRAPH_ID_ACCUMULO_WITH_EDGES + ") Entity group BasicEntity does not exist in the schema");
    }

    /**
     * Federation acts as a Edge/Entity graph with a view of Edge and Entity
     *
     * @throws OperationException any
     */
    @Test
    public void shouldGetEntitiesAndEdgesFromAnEntityAndAnEdgeGraph() throws OperationException {
        addBasicEntity();
        addBasicEdge();

        final Iterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE)
                        .entity(GROUP_BASIC_ENTITY)
                        .build())
                .build(), user);

        final ArrayList<? extends Element> elements = Lists.newArrayList(rtn.iterator());
        assertThat(elements).hasSize(2);
    }

    @Test
    public void shouldGetDoubleEdgesFromADoubleEdgeGraph() throws OperationException {
        graph.execute(new RemoveGraph.Builder()
                .graphId(GRAPH_ID_ACCUMULO_WITH_ENTITIES)
                .build(), user);

        graph.execute(new AddGraph.Builder()
                .graphId(GRAPH_ID_ACCUMULO_WITH_EDGES + 2)
                .storeProperties(ACCUMULO_PROPERTIES)
                .schema(Schema.fromJson(StreamUtil.openStream(FederatedViewsIT.class, "schema/basicEdge2Schema.json")))
                .build(), user);

        addBasicEdge();

        graph.execute(new AddElements.Builder()
                .input(Lists.newArrayList(new Edge.Builder()
                        .group(GROUP_BASIC_EDGE + 2)
                        .source("a")
                        .dest("b")
                        .build()))
                .build(), user);

        final Iterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE)
                        .edge(GROUP_BASIC_EDGE + 2)
                        .build())
                .build(), user);

        final ArrayList<? extends Element> elements = Lists.newArrayList(rtn.iterator());

        assertThat(elements).hasSize(2);
    }

    @Test
    public void shouldGetDoubleEntitiesFromADoubleEntityGraph() throws OperationException {
        graph.execute(new RemoveGraph.Builder()
                .graphId(GRAPH_ID_ACCUMULO_WITH_EDGES)
                .build(), user);

        graph.execute(new AddGraph.Builder()
                .graphId(GRAPH_ID_ACCUMULO_WITH_ENTITIES + 2)
                .storeProperties(ACCUMULO_PROPERTIES)
                .schema(Schema.fromJson(StreamUtil.openStream(FederatedViewsIT.class, "schema/basicEntity2Schema.json")))
                .build(), user);

        addBasicEntity();

        graph.execute(new AddElements.Builder()
                .input(Lists.newArrayList(new Entity.Builder()
                        .group(GROUP_BASIC_ENTITY + 2)
                        .vertex("a")
                        .build()))
                .build(), user);

        final Iterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(GROUP_BASIC_ENTITY)
                        .entity(GROUP_BASIC_ENTITY + 2)
                        .build())
                .build(), user);

        final ArrayList<? extends Element> elements = Lists.newArrayList(rtn.iterator());

        assertThat(elements).hasSize(2);
    }

    protected void addBasicEdge() throws OperationException {
        graph.execute(new AddElements.Builder()
                .input(Lists.newArrayList(new Edge.Builder()
                        .group(GROUP_BASIC_EDGE)
                        .source("a")
                        .dest("b")
                        .build()))
                .build(), user);
    }

    protected void addBasicEntity() throws OperationException {
        graph.execute(new AddElements.Builder()
                .input(Lists.newArrayList(new Entity.Builder()
                        .group(GROUP_BASIC_ENTITY)
                        .vertex("a")
                        .build()))
                .build(), user);
    }
}


