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
import gaffer.integration.GafferIntegrationTests;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.operation.impl.get.GetRelatedEdges;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class TransientPropertiesIT extends GafferIntegrationTests {

    /**
     * Adds simple graph data required for testing.
     *
     * @throws OperationException should never be thrown.
     */
    @Before
    public void data() throws OperationException {
        //BUILD
        final Collection<Element> elements = new ArrayList<>(2);
        final Edge sampleEdgeWithTransientProperty = new Edge(TestGroups.EDGE, "A", "B", true);
        sampleEdgeWithTransientProperty.putProperty(TestPropertyNames.COUNT, 1L);
        sampleEdgeWithTransientProperty.putProperty(TestPropertyNames.TRANSIENT_1, "test");
        elements.add(sampleEdgeWithTransientProperty);

        final Entity sampleEntityWithTransientProperty = new Entity(TestGroups.ENTITY, "C");
        sampleEntityWithTransientProperty.putProperty(TestPropertyNames.TRANSIENT_1, "test");
        elements.add(sampleEntityWithTransientProperty);

        final AddElements add = new AddElements(elements);
        graph.execute(add);
    }

    /**
     * Tests that the edge stored does not contain any transient properties not stored in the Schemas.
     *
     * @throws OperationException should never be thrown.
     */
    @Test
    public void testEdgePropertiesNotInSchemaAreSkipped() throws OperationException {

        //OPERATE
        final List<ElementSeed> seeds = Collections.singletonList((ElementSeed) (new EntitySeed("A")));
        final GetRelatedEdges getEdges = new GetRelatedEdges(seeds);
        final List<Edge> results = Lists.newArrayList(graph.execute(getEdges));

        //CHECK
        assertNotNull(results);
        assertEquals(1, results.size());
        for (Element result : results) {
            assertEquals(1L, result.getProperty(TestPropertyNames.COUNT));
            assertNull(result.getProperty(TestPropertyNames.TRANSIENT_1));
        }
    }

    /**
     * Tests that the entity stored does not contain any transient properties not stored in the Schemas.
     *
     * @throws OperationException should never be thrown.
     */
    @Test
    public void testEntityPropertiesNotInSchemaAreSkipped() throws OperationException {

        //OPERATE
        final List<EntitySeed> seeds = Collections.singletonList(new EntitySeed("C"));
        final GetEntitiesBySeed getEdges = new GetEntitiesBySeed(seeds);
        final List<Entity> results = Lists.newArrayList(graph.execute(getEdges));

        //CHECK
        assertNotNull(results);
        assertEquals(1, results.size());
        for (Element result : results) {
            assertNull(result.getProperty(TestPropertyNames.TRANSIENT_1));
        }
    }
}
