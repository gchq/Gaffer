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
package uk.gov.gchq.gaffer.gafferpop.generator;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.gafferpop.GafferPopVertex;

import java.util.Iterator;

public class GafferEntityGenerator implements OneToOneElementGenerator<GafferPopVertex> {
    @Override
    public Entity _apply(final GafferPopVertex vertex) {
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
}
