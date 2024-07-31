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

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopVertex;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.util.GafferCustomTypeFactory;

public class GafferEntityGenerator implements OneToOneElementGenerator<GafferPopVertex> {
    @Override
    public Entity _apply(final GafferPopVertex vertex) {
        if (vertex == null) {
            throw new IllegalArgumentException("Unable to convert a null GafferPopVertex Object");
        }

        final Entity entity = new Entity(vertex.label(), GafferCustomTypeFactory.parseAsCustomTypeIfValid(vertex.id()));

        // Tinkerpop allows nested properties under a key for Gaffer we need to flatten these so only one property per key
        vertex.properties().forEachRemaining(vertProp -> {
            entity.putProperty(vertProp.key(), GafferCustomTypeFactory.parseAsCustomTypeIfValid(vertProp.value()));
            vertProp.properties().forEachRemaining(
                prop -> entity.putProperty(prop.key(), GafferCustomTypeFactory.parseAsCustomTypeIfValid(prop.value())));
        });

        return entity;
    }
}
