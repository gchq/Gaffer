/*
 * Copyright 2016-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.generator;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneObjectGenerator;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopVertex;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.util.GafferCustomTypeFactory;


public class GafferPopVertexGenerator implements OneToOneObjectGenerator<GafferPopVertex> {
    private final GafferPopGraph graph;
    private final boolean gafferPopReadOnly;

    public GafferPopVertexGenerator(final GafferPopGraph graph) {
        this(graph, true);
    }

    public GafferPopVertexGenerator(final GafferPopGraph graph, final boolean gafferPopReadOnly) {
        this.graph = graph;
        this.gafferPopReadOnly = gafferPopReadOnly;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Edge it must be an Entity")
    @Override
    public GafferPopVertex _apply(final Element element) {
        if (element instanceof Edge) {
            throw new IllegalArgumentException("An Edge cannot be converted into a GafferPopVertex");
        }

        final Entity entity = ((Entity) element);
        final GafferPopVertex vertex = new GafferPopVertex(
            entity.getGroup(),
            GafferCustomTypeFactory.parseForGraphSONv3(entity.getVertex()),
            graph);

        // Add the properties
        entity.getProperties().forEach((k, v) -> {
            if (v != null) {
                vertex.propertyWithoutUpdate(Cardinality.list, k, GafferCustomTypeFactory.parseForGraphSONv3(v));
            }
        });

        if (gafferPopReadOnly) {
            vertex.setReadOnly();
        }

        return vertex;
    }
}
