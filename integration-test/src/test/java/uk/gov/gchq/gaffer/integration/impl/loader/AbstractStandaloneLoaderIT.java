/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl.loader;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.StandaloneIT;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.HashMap;
import java.util.Map;

import static uk.gov.gchq.gaffer.data.util.ElementUtil.assertElementEquals;
import static uk.gov.gchq.gaffer.store.schema.TestSchema.BASIC_SCHEMA;
import static uk.gov.gchq.gaffer.store.schema.TestSchema.FULL_SCHEMA;

public abstract class AbstractStandaloneLoaderIT<T extends Operation> extends StandaloneIT {

    // Identifier prefixes
    public static final String SOURCE = "1-Source";
    public static final String DEST = "2-Dest";
    public static final String SOURCE_DIR = "1-SourceDir";
    public static final String DEST_DIR = "2-DestDir";
    public static final String A = "A";
    public static final String B = "B";
    public static final String C = "C";
    public static final String D = "D";
    public static final String[] VERTEX_PREFIXES = new String[]{A, B, C, D};

    protected final Map<EntityId, Entity> basicSchemaEntities = createBasicSchemaEntities();
    protected final Map<EdgeId, Edge> basicSchemaEdges = createBasicSchemaEdges();

    protected final Map<EntityId, Entity> fullSchemaEntities = createFullSchemaEntities();
    protected final Map<EdgeId, Edge> fullSchemaEdges = createFullSchemaEdges();

    protected final Iterable<? extends Element> basicSchemaInput = getBasicSchemaInputElements();

    protected final Iterable<? extends Element> fullSchemaInput = getFullSchemaInputElements();

    @Override
    protected Schema createSchema() {
        return BASIC_SCHEMA.getSchema();
    }

    @Override
    public StoreProperties createStoreProperties() {
        return new StoreProperties();
    }

    @Before
    public void setup() throws Exception {
        configure(fullSchemaInput);
    }

    @Test
    public void shouldAddElements_basicSchema() throws Exception {
        // Given
        final Graph graph = createGraph(BASIC_SCHEMA.getSchema());

        // When
        addElements(graph, basicSchemaInput);
        final Iterable<? extends Element> result = getAllElements(graph);

        // Then
        assertElementEquals(basicSchemaInput, result);
    }

    @Test
    public void shouldAddElements_fullSchema() throws Exception {
        // Given
        final Graph graph = createGraph(FULL_SCHEMA.getSchema());

        // When
        addElements(graph, fullSchemaInput);
        final Iterable<? extends Element> result = getAllElements(graph);

        // Then
        assertElementEquals(fullSchemaInput, result);
    }

    @Test
    @Ignore
    public void shouldAddElements_aggregationSchema() throws Exception {
        // testLoaderWithSchema(TestSchemas.getAggregationSchema());
    }

    @Test
    @Ignore
    public void shouldAddElements_visibilitySchema() throws Exception {
        // testLoaderWithSchema(TestSchemas.getVisibilitySchema());
    }

    protected void addElements(final Graph graph, final Iterable<? extends Element> elements) throws Exception {
        graph.execute(createOperation(elements), getUser());
    }

    private Iterable<? extends Element> getAllElements(final Graph graph) throws Exception {
        return graph.execute(new GetAllElements(), getUser());
    }

    protected Iterable<? extends Element> getBasicSchemaInputElements() {
        final Iterable<? extends Edge> edges = getBasicSchemaEdges().values();
        final Iterable<? extends Entity> entities = getBasicSchemaEntities().values();

        return Iterables.concat(edges, entities);
    }

    protected Iterable<? extends Element> getFullSchemaInputElements() {
        final Iterable<? extends Edge> edges = getFullSchemaEdges().values();
        final Iterable<? extends Entity> entities = getFullSchemaEntities().values();

        return Iterables.concat(edges, entities);
    }

    protected Map<EdgeId, Edge> createBasicSchemaEdges() {
        final Map<EdgeId, Edge> edges = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final Edge edge = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .directed(false)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build();
                addToMap(edge, edges);

                final Edge edgeDir = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .directed(true)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build();
                addToMap(edgeDir, edges);
            }

            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE + i)
                    .dest(DEST + i)
                    .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 1L)
                    .build();
            addToMap(edge, edges);

            final Edge edgeDir = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE_DIR + i)
                    .dest(DEST_DIR + i)
                    .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                    .directed(true)
                    .build();
            edgeDir.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(edgeDir, edges);
        }

        return edges;
    }

    protected Map<EdgeId, Edge> createFullSchemaEdges() {
        final Map<EdgeId, Edge> edges = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final Edge edge = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .directed(false)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build();
                addToMap(edge, edges);

                final Edge edgeDir = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .directed(true)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build();
                addToMap(edgeDir, edges);
            }

            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE + i)
                    .dest(DEST + i)
                    .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 1L)
                    .build();
            addToMap(edge, edges);

            final Edge edgeDir = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE_DIR + i)
                    .dest(DEST_DIR + i)
                    .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                    .directed(true)
                    .build();
            edgeDir.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(edgeDir, edges);
        }

        return edges;
    }

    protected Map<EntityId, Entity> createBasicSchemaEntities() {
        final Map<EntityId, Entity> entities = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final Entity entity = new Entity(TestGroups.ENTITY, VERTEX_PREFIXES[j] + i);
                entity.putProperty(TestPropertyNames.COUNT, 1L);
                addToMap(entity, entities);
            }

            final Entity secondEntity = new Entity(TestGroups.ENTITY, SOURCE + i);
            secondEntity.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(secondEntity, entities);

            final Entity thirdEntity = new Entity(TestGroups.ENTITY, DEST + i);
            thirdEntity.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(thirdEntity, entities);

            final Entity fourthEntity = new Entity(TestGroups.ENTITY, SOURCE_DIR + i);
            fourthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(fourthEntity, entities);

            final Entity fifthEntity = new Entity(TestGroups.ENTITY, DEST_DIR + i);
            fifthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(fifthEntity, entities);
        }

        return entities;
    }

    protected Map<EntityId, Entity> createFullSchemaEntities() {
        final Map<EntityId, Entity> entities = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final Entity entity = new Entity(TestGroups.ENTITY, VERTEX_PREFIXES[j] + i);
                entity.putProperty(TestPropertyNames.COUNT, 1L);
                addToMap(entity, entities);
            }

            final Entity secondEntity = new Entity(TestGroups.ENTITY, SOURCE + i);
            secondEntity.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(secondEntity, entities);

            final Entity thirdEntity = new Entity(TestGroups.ENTITY, DEST + i);
            thirdEntity.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(thirdEntity, entities);

            final Entity fourthEntity = new Entity(TestGroups.ENTITY, SOURCE_DIR + i);
            fourthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(fourthEntity, entities);

            final Entity fifthEntity = new Entity(TestGroups.ENTITY, DEST_DIR + i);
            fifthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(fifthEntity, entities);
        }

        return entities;
    }

    public Map<EntityId, Entity> getBasicSchemaEntities() {
        return basicSchemaEntities;
    }

    public Map<EdgeId, Edge> getBasicSchemaEdges() {
        return basicSchemaEdges;
    }

    public Map<EntityId, Entity> getFullSchemaEntities() {
        return fullSchemaEntities;
    }

    public Map<EdgeId, Edge> getFullSchemaEdges() {
        return fullSchemaEdges;
    }

    protected static void addToMap(final Edge element, final Map<EdgeId, Edge> edges) {
        edges.put(ElementSeed.createSeed(element), element);
    }

    protected static void addToMap(final Entity element, final Map<EntityId, Entity> entities) {
        entities.put(ElementSeed.createSeed(element), element);
    }

    protected abstract void configure(final Iterable<? extends Element> elements) throws Exception;

    protected abstract T createOperation(final Iterable<? extends Element> elements);
}
