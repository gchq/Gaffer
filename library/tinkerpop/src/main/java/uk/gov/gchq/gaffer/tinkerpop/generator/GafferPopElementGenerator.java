/*
 * Copyright 2023 Crown Copyright
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
import uk.gov.gchq.gaffer.tinkerpop.GafferPopElement;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

public class GafferPopElementGenerator implements OneToOneObjectGenerator<GafferPopElement> {
    private final GafferPopGraph graph;
    private final boolean gafferPopReadOnly;

    public GafferPopElementGenerator(final GafferPopGraph graph) {
        this(graph, true);
    }

    public GafferPopElementGenerator(final GafferPopGraph graph, final boolean gafferPopReadOnly) {
        this.graph = graph;
        this.gafferPopReadOnly = gafferPopReadOnly;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public GafferPopElement _apply(final Element element) {
        if (element instanceof Entity) {
            return new GafferPopVertexGenerator(graph, gafferPopReadOnly)._apply(element);
        } else if (element instanceof Edge) {
            return new GafferPopEdgeGenerator(graph, gafferPopReadOnly)._apply(element);
        } else {
            throw new IllegalArgumentException("GafferPopElement has to be Edge or Entity");
        }
    }
}
