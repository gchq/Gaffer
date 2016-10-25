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
package gaffer.integration.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.clearspring.analytics.util.Lists;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.transform.Concat;
import gaffer.integration.AbstractStoreIT;
import gaffer.integration.TraitRequirement;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetEdgesBySeed;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.store.StoreTrait;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TransformationIT extends AbstractStoreIT {
    private static final String VERTEX = "vertexWithTransientProperty";

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();

        final Collection<Element> elements = new ArrayList<>(2);
        final Edge sampleEdgeWithTransientProperty = new Edge(TestGroups.EDGE, VERTEX + SOURCE, VERTEX + DEST, true);
        sampleEdgeWithTransientProperty.putProperty(TestPropertyNames.COUNT, 1L);
        sampleEdgeWithTransientProperty.putProperty(TestPropertyNames.TRANSIENT_1, "test");
        elements.add(sampleEdgeWithTransientProperty);

        final Entity sampleEntityWithTransientProperty = new Entity(TestGroups.ENTITY, VERTEX);
        sampleEntityWithTransientProperty.putProperty(TestPropertyNames.TRANSIENT_1, "test");
        elements.add(sampleEntityWithTransientProperty);

        graph.execute(new AddElements(elements), getUser());
    }

    /**
     * Tests that the entity stored does not contain any transient properties not stored in the Schemas.
     *
     * @throws OperationException should never be thrown.
     */
    @Test
    public void shouldNotStoreEntityPropertiesThatAreNotInSchema() throws OperationException {
        // Given
        final GetEntitiesBySeed getEntities = new GetEntitiesBySeed.Builder()
                .addSeed(new EntitySeed(VERTEX))
                .build();

        // When
        final List<Entity> results = Lists.newArrayList(graph.execute(getEntities, getUser()));


        assertNotNull(results);
        assertEquals(1, results.size());
        for (final Entity result : results) {
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
        final GetEdgesBySeed getEdges = new GetEdgesBySeed.Builder()
                .addSeed(new EdgeSeed(VERTEX + SOURCE, VERTEX + DEST, true))
                .build();

        // When
        final List<Edge> results = Lists.newArrayList(graph.execute(getEdges, getUser()));

        // Then
        assertNotNull(results);
        assertEquals(1, results.size());
        for (final Edge result : results) {
            assertEquals(1L, result.getProperty(TestPropertyNames.COUNT));
            assertNull(result.getProperty(TestPropertyNames.TRANSIENT_1));
        }
    }

    @Test
    @TraitRequirement(StoreTrait.TRANSFORMATION)
    public void shouldCreateTransientEntityProperty() throws OperationException {
        // Given
        final GetEntitiesBySeed getEntities = new GetEntitiesBySeed.Builder()
                .addSeed(new EntitySeed("A1"))
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
        final List<Entity> results = Lists.newArrayList(graph.execute(getEntities, getUser()));


        assertNotNull(results);
        assertEquals(1, results.size());
        for (final Entity result : results) {
            assertEquals("A1,3", result.getProperty(TestPropertyNames.TRANSIENT_1));
        }
    }

    @Test
    @TraitRequirement(StoreTrait.TRANSFORMATION)
    public void shouldCreateTransientEdgeProperty() throws OperationException {
        // Given
        final GetEdgesBySeed getEdges = new GetEdgesBySeed.Builder()
                .addSeed(new EdgeSeed(SOURCE_1, DEST_1, false))
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
        final List<Edge> results = Lists.newArrayList(graph.execute(getEdges, getUser()));

        assertNotNull(results);
        assertEquals(1, results.size());
        for (final Edge result : results) {
            assertEquals(SOURCE_1 + "," + 1, result.getProperty(TestPropertyNames.TRANSIENT_1));
        }
    }

    @Test
    @TraitRequirement(StoreTrait.TRANSFORMATION)
    public void shouldTransformExistingProperty() throws OperationException {
        // Given
        final GetEntitiesBySeed getEntities = new GetEntitiesBySeed.Builder()
                .addSeed(new EntitySeed("A1"))
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
        final List<Entity> results = Lists.newArrayList(graph.execute(getEntities, getUser()));


        assertNotNull(results);
        assertEquals(1, results.size());
        for (final Entity result : results) {
            assertEquals("A1,3", result.getProperty(TestPropertyNames.STRING));
        }
    }
}
