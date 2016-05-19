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
import gaffer.gafferpop.GafferPopEdge;
import gaffer.gafferpop.GafferPopGraph;
import org.apache.tinkerpop.gremlin.structure.Property;
import java.util.Iterator;
import java.util.Map.Entry;

public class GafferPopEdgeGenerator extends OneToOneElementGenerator<GafferPopEdge> {
    private final GafferPopGraph graph;
    private final boolean gafferPopReadOnly;

    public GafferPopEdgeGenerator(final GafferPopGraph graph) {
        this(graph, true);
    }

    public GafferPopEdgeGenerator(final GafferPopGraph graph, final boolean gafferPopReadOnly) {
        this.graph = graph;
        this.gafferPopReadOnly = gafferPopReadOnly;
    }

    @Override
    public Edge getElement(final GafferPopEdge gafferPopEdge) {
        final Edge edge = new Edge(gafferPopEdge.label(), gafferPopEdge.id().getSource(),
                gafferPopEdge.id().getDest(), true);
        final Iterator<Property<Object>> propItr = gafferPopEdge.properties();
        while (propItr.hasNext()) {
            final Property<Object> prop = propItr.next();
            if (null != prop.key()) {
                edge.putProperty(prop.key(), prop.value());
            }
        }
        return edge;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public GafferPopEdge getObject(final Element element) {
        if (element instanceof Entity) {
            throw new IllegalArgumentException("An Entity cannot be converted into a GafferPopEdge");
        }

        final Edge edge = ((Edge) element);
        final GafferPopEdge gafferPopEdge = new GafferPopEdge(edge.getGroup(),
                edge.getSource(), edge.getDestination(), graph);

        for (Entry<String, Object> entry : edge.getProperties().entrySet()) {
            if (null != entry.getValue()) {
                gafferPopEdge.property(entry.getKey(), entry.getValue());
            }
        }
        if (gafferPopReadOnly) {
            gafferPopEdge.setReadOnly();
        }

        return gafferPopEdge;
    }
}
