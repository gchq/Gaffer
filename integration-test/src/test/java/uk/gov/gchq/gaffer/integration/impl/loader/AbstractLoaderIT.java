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

package uk.gov.gchq.gaffer.integration.impl.loader;

import com.google.common.collect.Iterables;
import org.junit.Ignore;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.TestSchemas;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static uk.gov.gchq.gaffer.data.util.ElementUtil.assertElementEquals;

public abstract class AbstractLoaderIT<T extends Operation> extends AbstractStoreIT {

    protected final Iterable<? extends Element> input = getInputElements();

    @Override
    public void setup() throws Exception {
        super.setup();
        configure(input);
    }

    @Test
    public void shouldAddElements_basicSchema() throws OperationException {
        // Given
        createGraph(TestSchemas.getBasicSchema());

        // When
        addElements();
        final Iterable<? extends Element> result = getAllElements();

        // Then
        assertEquals(Iterables.size(input), Iterables.size(result));
        assertElementEquals(input, result);
    }

    @Test
    public void shouldAddElements_fullSchema() throws OperationException {
        // Given
        createGraph(TestSchemas.getFullSchema());

        // When
        addElements();
        final Iterable<? extends Element> result = getAllElements();

        // Then
        assertEquals(Iterables.size(input), Iterables.size(result));
    }

    @Test
    @Ignore
    public void shouldAddElements_aggregationSchema() throws OperationException {
        // testLoaderWithSchema(TestSchemas.getAggregationSchema());
    }

    @Test
    @Ignore
    public void shouldAddElements_visibilitySchema() throws OperationException {
        // testLoaderWithSchema(TestSchemas.getVisibilitySchema());
    }

    protected void addElements() throws OperationException {
        graph.execute(createOperation(input), getUser());
    }

    private Iterable<? extends Element> getAllElements() throws OperationException {
        return graph.execute(new GetAllElements(), getUser());
    }

    private Iterable<? extends Element> getInputElements() {
        final Iterable<? extends Element> edges = getEdges().values();
        final Iterable<? extends Element> entities = getEntities().values();

        return Iterables.concat(edges, entities);
    }

    @Override
    protected Map<EdgeId, Edge> createEdges() {
        final Map<EdgeId, Edge> edges = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final Edge edge = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .directed(false)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build();
                addToMap(edge, edges);

                final Edge edgeDir = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .directed(true)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build();
                addToMap(edgeDir, edges);
            }

            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE + i)
                    .dest(DEST + i)
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 1L)
                    .build();
            addToMap(edge, edges);

            final Edge edgeDir = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE_DIR + i)
                    .dest(DEST_DIR + i)
                    .directed(true)
                    .build();
            edgeDir.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(edgeDir, edges);
        }

        return edges;
    }

    @Override
    protected Map<EntityId, Entity> createEntities() {
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

    protected abstract void configure(final Iterable<? extends Element> elements) throws Exception;

    protected abstract T createOperation(final Iterable<? extends Element> elements);
}
