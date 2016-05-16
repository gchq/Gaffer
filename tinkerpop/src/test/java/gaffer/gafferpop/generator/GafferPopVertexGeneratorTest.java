/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package gaffer.gafferpop.generator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Lists;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Entity;
import gaffer.gafferpop.GafferPopGraph;
import gaffer.gafferpop.GafferPopVertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.junit.Test;

public class GafferPopVertexGeneratorTest {
    @Test
    public void shouldConvertGafferEntityToGafferPopReadOnlyEntity() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        final String vertex = "vertex";
        final String propValue = "property value";
        final Entity entity = new Entity(TestGroups.ENTITY, vertex);
        entity.putProperty(TestPropertyNames.STRING, propValue);
        entity.putProperty(TestPropertyNames.INT, null); // should be skipped

        final GafferPopVertexGenerator generator = new GafferPopVertexGenerator(graph, true);

        // When
        final GafferPopVertex gafferPopVertex = generator.getObject(entity);

        // Then
        assertEquals(TestGroups.ENTITY, gafferPopVertex.label());
        assertEquals(vertex, gafferPopVertex.id());
        assertEquals(propValue, gafferPopVertex.property(TestPropertyNames.STRING).value());
        assertEquals(1, Lists.newArrayList(gafferPopVertex.properties()).size());
        assertSame(graph, gafferPopVertex.graph());
        assertTrue(gafferPopVertex.isReadOnly());
    }

    @Test
    public void shouldConvertGafferEntityToGafferPopReadWriteEntity() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        final String vertex = "vertex";
        final String propValue = "property value";
        final Entity entity = new Entity(TestGroups.ENTITY, vertex);
        entity.putProperty(TestPropertyNames.STRING, propValue);
        entity.putProperty(TestPropertyNames.INT, null); // should be skipped

        final GafferPopVertexGenerator generator = new GafferPopVertexGenerator(graph, false);

        // When
        final GafferPopVertex gafferPopVertex = generator.getObject(entity);

        // Then
        assertEquals(TestGroups.ENTITY, gafferPopVertex.label());
        assertEquals(vertex, gafferPopVertex.id());
        assertEquals(propValue, gafferPopVertex.property(TestPropertyNames.STRING).value());
        assertEquals(1, Lists.newArrayList(gafferPopVertex.properties()).size());
        assertSame(graph, gafferPopVertex.graph());
        assertFalse(gafferPopVertex.isReadOnly());
    }

    @Test
    public void shouldConvertGafferPopVertexToGafferEntity() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        final String vertex = "vertex";
        final String propValue = "property value";
        final GafferPopVertex gafferPopVertex = new GafferPopVertex(TestGroups.ENTITY, vertex, graph);
        gafferPopVertex.property(Cardinality.list, TestPropertyNames.STRING, propValue);

        final GafferPopVertexGenerator generator = new GafferPopVertexGenerator(graph);

        // When
        final Entity entity = generator.getElement(gafferPopVertex);

        // Then
        assertEquals(TestGroups.ENTITY, entity.getGroup());
        assertEquals(vertex, entity.getVertex());
        assertEquals(1, entity.getProperties().size());
        assertEquals(propValue, entity.getProperty(TestPropertyNames.STRING));
    }

}