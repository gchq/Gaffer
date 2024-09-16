/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.koryphe.impl.function.Concat;
import uk.gov.gchq.koryphe.impl.function.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TransformationIT extends AbstractStoreIT {
    private static final String VERTEX = "vertexWithTransientProperty";

    @Override
    public void _setup() throws Exception {
        addDefaultElements();
        addAdditionalElements();
    }

    /**
     * Tests that the entity stored does not contain any transient properties not stored in the Schemas.
     *
     * @throws OperationException should never be thrown.
     */
    @Test
    public void shouldNotStoreEntityPropertiesThatAreNotInSchema() throws OperationException {
        // Given
        final GetElements getEntities = new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .entity(TestGroups.ENTITY_2)
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getEntities, getUser()));


        assertThat(results)
                .hasSize(1);
        for (final Element result : results) {
            assertThat(result.getProperty(TestPropertyNames.TRANSIENT_1)).isNull();
        }
    }

    /**
     * Tests that the edge stored does not contain any transient properties not stored in the Schemas.
     *
     * @throws OperationException should never be thrown.
     */
    @Test
    public void shouldNotStoreEdgePropertiesThatAreNotInSchema() throws OperationException {
        // Given
        final GetElements getEdges = new GetElements.Builder()
                .input(new EdgeSeed(VERTEX + SOURCE, VERTEX + DEST, true))
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getEdges, getUser()));

        // Then
        assertThat(results)
                .hasSize(1);
        for (final Element result : results) {
            assertThat(result.getProperty(TestPropertyNames.COUNT)).isEqualTo(1L);
            assertThat(result.getProperty(TestPropertyNames.TRANSIENT_1)).isNull();
        }
    }

    @Test
    @TraitRequirement(StoreTrait.TRANSFORMATION)
    public void shouldCreateTransientEntityProperty() throws OperationException {
        // Given
        final GetElements getEntities = new GetElements.Builder()
                .input(new EntitySeed("A1"))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .transientProperty(TestPropertyNames.TRANSIENT_1, String.class)
                                .transformer(new ElementTransformer.Builder()
                                        .select(IdentifierType.VERTEX.name(), TestPropertyNames.SET)
                                        .execute(new Concat())
                                        .project(TestPropertyNames.TRANSIENT_1)
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getEntities, getUser()));


        assertThat(results)
                .hasSize(1);
        for (final Element result : results) {
            assertThat(result.getProperty(TestPropertyNames.TRANSIENT_1)).isEqualTo("A1,[3]");
        }
    }

    @Test
    @TraitRequirement(StoreTrait.TRANSFORMATION)
    public void shouldCreateTransientEdgeProperty() throws OperationException {
        // Given
        final GetElements getEdges = new GetElements.Builder()
                .input(new EdgeSeed(SOURCE_1, DEST_1, false))
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .transientProperty(TestPropertyNames.TRANSIENT_1, String.class)
                                .transformer(new ElementTransformer.Builder()
                                        .select(IdentifierType.SOURCE.name(), TestPropertyNames.INT)
                                        .execute(new Concat())
                                        .project(TestPropertyNames.TRANSIENT_1)
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getEdges, getUser()));

        assertThat(results).isNotNull();
        for (final Element result : results) {
            assertThat(result.getProperty(TestPropertyNames.TRANSIENT_1)).isEqualTo(SOURCE_1 + "," + result.getProperty(TestPropertyNames.INT));
        }
    }

    @Test
    @TraitRequirement(StoreTrait.TRANSFORMATION)
    public void shouldTransformVertex() throws OperationException {
        // Given
        final GetElements getEntities = new GetElements.Builder()
                .input(new EntitySeed("A1"))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .transformer(new ElementTransformer.Builder()
                                        .select(IdentifierType.VERTEX.name(), TestPropertyNames.SET)
                                        .execute(new Concat())
                                        .project(IdentifierType.VERTEX.name())
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getEntities, getUser()));


        assertThat(results)
                .hasSize(1);
        for (final Element result : results) {
            assertThat(((Entity) result).getVertex()).isEqualTo("A1,[3]");
        }
    }

    private void addAdditionalElements() throws OperationException {
        final Collection<Element> elements = Arrays.asList(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX + SOURCE)
                        .dest(VERTEX + DEST)
                        .directed(true)
                        .property(TestPropertyNames.COUNT, 1L)
                        .property(TestPropertyNames.TRANSIENT_1, "test")
                        .build(),

                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(VERTEX)
                        .property(TestPropertyNames.TRANSIENT_1, "test")
                        .build()
        );

        graph.execute(new AddElements.Builder()
                .input(elements)
                .build(), getUser());
    }

    @Test
    public void shouldNotErrorWhenEdgeTransformReceivesEntities() throws OperationException {
        final Iterable<? extends Element> result = graph.execute(new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .build())
                .then(new Transform.Builder()
                        .edge(TestGroups.EDGE, new ElementTransformer.Builder()
                                .select(TestPropertyNames.COUNT)
                                .execute(new ToString())
                                .project("propAlt")
                                .build())
                        .build())
                .build(), getUser());

        //Resolve the lazy iterator, by adding the contents to a list, No exception should have been thrown here.
        final ArrayList<Element> edges = Lists.newArrayList(result);
        final ArrayList<Element> entities = Lists.newArrayList(edges);

        edges.removeIf(e -> e instanceof Entity);
        assertThat(edges).hasSize(111);
        assertThat(edges.get(0).getProperties()).containsKey("propAlt");


        entities.removeIf(e -> e instanceof Edge);
        assertThat(entities).hasSize(89);
        assertThat(entities.get(0).getProperties()).doesNotContainKey("propAlt");
    }

    @Test
    public void shouldNotErrorWhenEntityTransformReceivesEdges() throws OperationException {
        final Iterable<? extends Element> result = graph.execute(new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .build())
                .then(new Transform.Builder()
                        .entity(TestGroups.ENTITY, new ElementTransformer.Builder()
                                .select(TestPropertyNames.COUNT)
                                .execute(new ToString())
                                .project("propAlt")
                                .build())
                        .build())
                .build(), getUser());

        //Resolve the lazy iterator, by adding the contents to a list, No exception should have been thrown here.
        final ArrayList<Element> edges = Lists.newArrayList(result);
        final ArrayList<Element> entities = Lists.newArrayList(edges);

        edges.removeIf(e -> e instanceof Entity);
        assertThat(edges).hasSize(111);
        assertThat(edges.get(0).getProperties()).doesNotContainKey("propAlt");

        entities.removeIf(e -> e instanceof Edge);
        assertThat(entities).hasSize(89);
        assertThat(entities.get(0).getProperties()).containsKey("propAlt");
    }
}
