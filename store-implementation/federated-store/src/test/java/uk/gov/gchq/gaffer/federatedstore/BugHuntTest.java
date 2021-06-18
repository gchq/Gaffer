/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.Schema.Builder;
import uk.gov.gchq.gaffer.user.User;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;

public class BugHuntTest {
    private static final String FEDERATED_STORE_ID = "testFederatedStoreId";
    private static final String ACC_ID_2 = "miniAccGraphId2";
    private static final String PATH_ACC_STORE_PROPERTIES_ALT = "properties/singleUseAccumuloStoreAlt.properties";
    private static final String PATH_BASIC_EDGE_SCHEMA_JSON = "schema/basicEdgeSchema.json";
    private FederatedStore store;
    private FederatedStoreProperties federatedProperties;
    private HashMapGraphLibrary library;
    private Context userContext;
    private User blankUser;

    private static final Class CURRENT_CLASS = new Object() {
    }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES_ALT = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(CURRENT_CLASS, PATH_ACC_STORE_PROPERTIES_ALT));

    @BeforeEach
    public void setUp() throws Exception {
        federatedProperties = new FederatedStoreProperties();

        store = new FederatedStore();
        store.setGraphLibrary(library);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        userContext = new Context(blankUser());
        blankUser = blankUser();
    }


    @Test
    public void shouldAddEdgesToOneGraph() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_EDGE_SCHEMA_JSON);

        Edge expected = new Edge.Builder()
                .group("BasicEdge")
                .source("testSource")
                .dest("testDest")
                .property("property1", 12)
                .build();
        AddElements op = new AddElements.Builder()
                .input(expected)
                .build();

        // When
        store.execute(op, userContext);

        // Then
        Set<Element> elements = getElements();
        assertEquals(1, elements.size());
        assertEquals(expected, elements.iterator().next());
    }


    private Set<Element> getElements() throws OperationException {
        CloseableIterable<? extends Element> elements = store
                .execute(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .edges(store.getSchema().getEdgeGroups())
                                .entities(store.getSchema().getEntityGroups())
                                .build())
                        .build(), new Context(blankUser));

        return (null == elements) ? Sets.newHashSet() : Sets.newHashSet(elements);
    }

    private void addGraphWithPaths(final String graphId, final StoreProperties properties, final String... schemaPath) throws OperationException {
        Builder schema = new Builder();
        for (String path : schemaPath) {
            schema.merge(getSchemaFromPath(path));
        }

        store.execute(new AddGraph.Builder()
                .graphId(graphId)
                .storeProperties(properties)
                .isPublic(true)
                .schema(schema.build())
                .build(), userContext);
    }


    private Schema getSchemaFromPath(final String path) {
        return Schema.fromJson(StreamUtil.openStream(Schema.class, path));

    }
}
