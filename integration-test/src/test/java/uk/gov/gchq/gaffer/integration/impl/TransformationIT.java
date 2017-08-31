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
package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

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
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.koryphe.impl.function.Concat;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TransformationIT extends AbstractStoreIT {
    private static final String VERTEX = "vertexWithTransientProperty";

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();

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


        assertNotNull(results);
        assertEquals(1, results.size());
        for (final Element result : results) {
            assertNull(result.getProperty(TestPropertyNames.TRANSIENT_1));
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
        assertNotNull(results);
        assertEquals(1, results.size());
        for (final Element result : results) {
            assertEquals(1L, result.getProperty(TestPropertyNames.COUNT));
            assertNull(result.getProperty(TestPropertyNames.TRANSIENT_1));
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
                                        .select(IdentifierType.VERTEX.name(), TestPropertyNames.STRING)
                                        .execute(new Concat())
                                        .project(TestPropertyNames.TRANSIENT_1)
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getEntities, getUser()));


        assertNotNull(results);
        assertEquals(1, results.size());
        for (final Element result : results) {
            assertEquals("A1,3", result.getProperty(TestPropertyNames.TRANSIENT_1));
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

        assertNotNull(results);
        for (final Element result : results) {
            assertEquals(SOURCE_1 + "," + result.getProperty(TestPropertyNames.INT), result.getProperty(TestPropertyNames.TRANSIENT_1));
        }
    }

    @Test
    @TraitRequirement(StoreTrait.TRANSFORMATION)
    public void shouldTransformExistingProperty() throws OperationException {
        // Given
        final GetElements getEntities = new GetElements.Builder()
                .input(new EntitySeed("A1"))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .transformer(new ElementTransformer.Builder()
                                        .select(IdentifierType.VERTEX.name(), TestPropertyNames.STRING)
                                        .execute(new Concat())
                                        .project(TestPropertyNames.STRING)
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getEntities, getUser()));


        assertNotNull(results);
        assertEquals(1, results.size());
        for (final Element result : results) {
            assertEquals("A1,3", result.getProperty(TestPropertyNames.STRING));
        }
    }
}
