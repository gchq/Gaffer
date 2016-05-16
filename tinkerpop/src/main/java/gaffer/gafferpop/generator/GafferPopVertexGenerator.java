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
package gaffer.gafferpop.generator;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.generator.OneToOneElementGenerator;
import gaffer.gafferpop.GafferPopGraph;
import gaffer.gafferpop.GafferPopVertex;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import java.util.Iterator;
import java.util.Map.Entry;

public class GafferPopVertexGenerator extends OneToOneElementGenerator<GafferPopVertex> {
    private final GafferPopGraph graph;
    private final boolean gafferPopReadOnly;

    public GafferPopVertexGenerator(final GafferPopGraph graph) {
        this(graph, true);
    }

    public GafferPopVertexGenerator(final GafferPopGraph graph, final boolean gafferPopReadOnly) {
        this.graph = graph;
        this.gafferPopReadOnly = gafferPopReadOnly;
    }

    @Override
    public Entity getElement(final GafferPopVertex vertex) {
        final Entity entity = new Entity(vertex.label(), vertex.id());
        final Iterator<VertexProperty<Object>> vertPropItr = vertex.properties();
        while (vertPropItr.hasNext()) {
            final VertexProperty<Object> vertProp = vertPropItr.next();
            if (null != vertProp.key()) {
                entity.putProperty(vertProp.key(), vertProp.value());
            }
            final Iterator<Property<Object>> propItr = vertProp.properties();
            while (propItr.hasNext()) {
                final Property<Object> prop = propItr.next();
                if (null != prop.key()) {
                    entity.putProperty(prop.key(), prop.value());
                }
            }
        }
        return entity;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Edge it must be an Entity")
    @Override
    public GafferPopVertex getObject(final Element element) {
        if (element instanceof Edge) {
            throw new IllegalArgumentException("An Edge cannot be converted into a GafferPopVertex");
        }

        final Entity entity = ((Entity) element);
        final GafferPopVertex vertex = new GafferPopVertex(entity.getGroup(), entity.getVertex(), graph);
        for (Entry<String, Object> entry : entity.getProperties().entrySet()) {
            if (null != entry.getValue()) {
                vertex.property(Cardinality.list, entry.getKey(), entry.getValue());
            }
        }
        if (gafferPopReadOnly) {
            vertex.setReadOnly();
        }

        return vertex;
    }
}
