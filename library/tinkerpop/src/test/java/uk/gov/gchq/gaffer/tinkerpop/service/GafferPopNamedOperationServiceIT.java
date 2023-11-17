/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.service;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphTest;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphTest.AUTH_1;
import static uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphTest.AUTH_2;
import static uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphTest.USER_ID;

public class GafferPopNamedOperationServiceIT {
    private static final MapStoreProperties PROPERTIES = MapStoreProperties.loadStoreProperties(StreamUtil.openStream(GafferPopGraphTest.class, "/gaffer/map-store.properties"));
    private static final Configuration TEST_CONFIGURATION = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.GRAPH, GafferPopGraph.class.getName());
            this.setProperty(GafferPopGraph.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
            this.setProperty(GafferPopGraph.USER_ID, USER_ID);
            this.setProperty(GafferPopGraph.DATA_AUTHS, new String[]{AUTH_1, AUTH_2});
        }
    };

    @Test
    void shouldHaveNamedOperationService() {
        // Given
        final GafferPopGraph gafferPopGraph = GafferPopGraph.open(TEST_CONFIGURATION, getGafferGraph());
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        GraphTraversal<Object, Object> traversal = g.call().with("verbose").with("service", "namedoperation");
        List<Object> results = traversal.toList();

        // Then
        assertThat(results).hasSize(1).asString()
                .contains("{\"name\":\"namedoperation\",\"type:[requirements]:\":{\"Start\":[]},\"params\":{}}");
    }

    @Test
    void shouldThrowExceptionForMissingParameter() {
        // Given
        final GafferPopGraph gafferPopGraph = GafferPopGraph.open(TEST_CONFIGURATION, getGafferGraph());
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        Map<String, String> params = Collections.singletonMap("missing", "missing");

        // Then
        assertThatThrownBy(() -> g.call("namedoperation", params).toList())
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Missing parameter");
    }

    @Test
    void shouldThrowExceptionWhenTryingToExecuteMissingNamedOperation() {
        // Given
        GafferPopGraph gafferPopGraph = GafferPopGraph.open(TEST_CONFIGURATION, getGafferGraph());
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        Map<String, String> params = Collections.singletonMap("execute", "missingNamedOp");

        // Then
        assertThatThrownBy(() -> g.call("namedoperation", params).toList())
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Named Operation not found");
    }

    @Test
    void shouldAddNamedOperation() {
        // Given
        final GafferPopGraph gafferPopGraph = GafferPopGraph.open(TEST_CONFIGURATION, getGafferGraph());
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        Map<String, Object> addParams = new HashMap<>();
        addParams.put("name", "testNamedOp");
        addParams.put("opChain", getOpChainElementCount(null).getOperationChainAsString());
        Map<String, Object> params = Collections.singletonMap("add", addParams);

        // Then
        assertThatNoException().isThrownBy(() -> g.call("namedoperation", params).toList());
    }

    @Test
    void shouldExecuteNonIterableReturningNamedOperation() throws OperationException {
        // Given
        String opName = "testNamedOpNonIterableReturn";
        Graph gafferGraph = getGafferGraph();
        gafferGraph.execute(getOpChainElementCount(opName), new User(USER_ID));
        GafferPopGraph gafferPopGraph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        Map<String, String> params = Collections.singletonMap("execute", opName);

        // Then
        assertThat(g.call("namedoperation", params).next())
                .isInstanceOf(Long.class)
                .isEqualTo(0L);
    }

    @Test
    void shouldExecuteIterableReturningNamedOperation() throws OperationException {
        // Given
        String opName = "testNamedOpIterableReturn";
        Graph gafferGraph = getGafferGraph();
        gafferGraph.execute(getOpChainElements(opName), new User(USER_ID));
        GafferPopGraph gafferPopGraph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        Map<String, String> params = Collections.singletonMap("execute", opName);

        // Then
        assertThat(g.call("namedoperation", params).toList())
                .isEmpty();
    }

    private Graph getGafferGraph() {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .storeProperties(PROPERTIES)
                .addSchemas(StreamUtil.openStreams(this.getClass(), "/gaffer/schema"))
                .build();
    }

    private AddNamedOperation getOpChainElementCount(String name) {
        return new AddNamedOperation.Builder()
                .operationChain(new OperationChain.Builder()
                        .first(new GetAllElements())
                        .then(new Count<>())
                        .build())
                .name(name)
                .build();
    }

    private AddNamedOperation getOpChainElements(String name) {
        return new AddNamedOperation.Builder()
                .operationChain(new OperationChain.Builder()
                        .first(new GetAllElements())
                        .build())
                .name(name)
                .build();
    }
}
