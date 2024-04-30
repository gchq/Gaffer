/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.server.auth;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verification.VertexProgramRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@TestInstance(Lifecycle.PER_CLASS)
class GafferPopAuthoriserTest {

    private static final MapStoreProperties PROPERTIES = MapStoreProperties.loadStoreProperties(StreamUtil.openStream(
            GafferPopAuthoriserTest.class, "/gaffer/map-store.properties"));
    private static final String TEST_USERNAME = "testUser";
    private GafferPopAuthoriser authoriser;

    @BeforeAll
    void setup() {
        authoriser = new GafferPopAuthoriser();
        authoriser.setup(new HashMap<String, Object>());
    }

    @Test
    void shouldNotAuthoriseLambdaBytecodeRequest() {
        // Given
        final GafferPopGraph graph = GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph());
        final GraphTraversalSource g = graph.traversal();
        final Bytecode bytecode = g.V().map(Lambda.function("it.get()")).asAdmin().getBytecode();

        // Then
        assertThatExceptionOfType(AuthorizationException.class)
            .isThrownBy(() ->
                authoriser.authorize(new AuthenticatedUser(TEST_USERNAME), bytecode, new HashMap<String, String>()))
            .withMessageContaining(GafferPopAuthoriser.REJECT_LAMBDA);
    }

    @Test
    void shouldNotAuthoriseMutatingBytecodeRequest() {
        // Given
        final GafferPopGraph graph = GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph());
        final GraphTraversalSource g = graph.traversal();
        final Bytecode bytecode = g.withoutStrategies(ReadOnlyStrategy.class).V().addV().asAdmin().getBytecode();

        // Then
        assertThatExceptionOfType(AuthorizationException.class)
            .isThrownBy(() ->
                authoriser.authorize(new AuthenticatedUser(TEST_USERNAME), bytecode, new HashMap<String, String>()))
            .withMessageContaining(GafferPopAuthoriser.REJECT_MUTATE);
    }


    @Test
    void shouldNotAuthoriseOLAPBytecodeRequest() {
        // Given
        final GafferPopGraph graph = GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph());
        final GraphTraversalSource g = graph.traversal();
        final Bytecode bytecode = g.withoutStrategies(VertexProgramRestrictionStrategy.class).withComputer().V().asAdmin().getBytecode();

        // Then
        assertThatExceptionOfType(AuthorizationException.class)
                .isThrownBy(() ->
                    authoriser.authorize(new AuthenticatedUser(TEST_USERNAME), bytecode, new HashMap<String, String>()))
                .withMessageContaining(GafferPopAuthoriser.REJECT_OLAP);
    }

    @Test
    void shouldNotAuthoriseSettingUserInRequest() {
        // Given
        final GafferPopGraph graph = GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph());
        final GraphTraversalSource g = graph.traversal();
        final Bytecode bytecode = g.V().asAdmin().getBytecode();
        // Add the with() step separately otherwise it will get converted to the OptionsStrategy before we can compare
        bytecode.addStep("with", GafferPopGraphVariables.USER_ID, "notAllowed");

        // Then
        assertThatExceptionOfType(AuthorizationException.class)
            .isThrownBy(() ->
                authoriser.authorize(new AuthenticatedUser(TEST_USERNAME), bytecode, new HashMap<String, String>()));
    }

    @Test
    void shouldInjectUserIdIntoRequest() throws AuthorizationException {
        // Given
        final GafferPopGraph graph = GafferPopGraph.open(GafferPopTestUtil.TEST_CONFIGURATION_1, getGafferGraph());
        final GraphTraversalSource g = graph.traversal();
        final Bytecode bytecode = g.V().asAdmin().getBytecode();
        final Bytecode expectedBytecode = g.V().asAdmin().getBytecode();
        // Add the with() step separately otherwise it will get converted to the OptionsStrategy before we can compare
        expectedBytecode.addSource("with", GafferPopGraphVariables.USER_ID, TEST_USERNAME);

        // Then
        assertThat(authoriser.authorize(new AuthenticatedUser(TEST_USERNAME), bytecode, new HashMap<String, String>())).isEqualTo(expectedBytecode);
    }

    private Graph getGafferGraph() {
        return GafferPopTestUtil.getGafferGraph(this.getClass(), PROPERTIES);
    }
}
