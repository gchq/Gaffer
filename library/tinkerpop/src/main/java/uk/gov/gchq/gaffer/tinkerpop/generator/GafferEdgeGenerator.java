/*
 * Copyright 2016-2023 Crown Copyright
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

import org.apache.tinkerpop.gremlin.structure.Property;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopEdge;

import java.util.Iterator;

public class GafferEdgeGenerator implements OneToOneElementGenerator<GafferPopEdge> {
    @Override
    public Edge _apply(final GafferPopEdge gafferPopEdge) {
        final Edge edge = new Edge(gafferPopEdge.label(), gafferPopEdge.outVertex().id(),
                gafferPopEdge.inVertex().id(), true);
        final Iterator<Property<Object>> propItr = gafferPopEdge.properties();
        while (propItr.hasNext()) {
            final Property<Object> prop = propItr.next();
            if (null != prop.key()) {
                edge.putProperty(prop.key(), prop.value());
            }
        }
        return edge;
    }
}
