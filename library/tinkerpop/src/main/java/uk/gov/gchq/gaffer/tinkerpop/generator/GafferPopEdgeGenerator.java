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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneObjectGenerator;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopEdge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.util.GafferCustomTypeFactory;

public class GafferPopEdgeGenerator implements OneToOneObjectGenerator<GafferPopEdge> {
    private final GafferPopGraph graph;
    private final boolean gafferPopReadOnly;

    public GafferPopEdgeGenerator(final GafferPopGraph graph) {
        this(graph, true);
    }

    public GafferPopEdgeGenerator(final GafferPopGraph graph, final boolean gafferPopReadOnly) {
        this.graph = graph;
        this.gafferPopReadOnly = gafferPopReadOnly;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public GafferPopEdge _apply(final Element element) {
        if (element instanceof Entity) {
            throw new IllegalArgumentException("An Entity cannot be converted into a GafferPopEdge");
        }

        final Edge edge = ((Edge) element);
        final GafferPopEdge gafferPopEdge = new GafferPopEdge(
            edge.getGroup(),
            GafferCustomTypeFactory.parseForGraphSONv3(edge.getSource()),
            GafferCustomTypeFactory.parseForGraphSONv3(edge.getDestination()),
            graph);

        // Add the properties
        edge.getProperties().forEach((k, v) -> {
            if (v != null) {
                gafferPopEdge.propertyWithoutUpdate(k, GafferCustomTypeFactory.parseForGraphSONv3(v));
            }
        });

        if (gafferPopReadOnly) {
            gafferPopEdge.setReadOnly();
        }

        return gafferPopEdge;
    }
}
