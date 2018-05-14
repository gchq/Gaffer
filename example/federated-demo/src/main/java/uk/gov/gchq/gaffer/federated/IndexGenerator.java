/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.federated;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class IndexGenerator implements OneToManyElementGenerator<Element> {
    @Override
    public Iterable<Element> _apply(final Element element) {
        final List<Element> elements = new ArrayList<>();
        for (final Entry<String, Object> entry : element.getProperties().entrySet()) {
            if (element instanceof Edge) {
                final Edge edge = (Edge) element;
                elements.add(new Entity.Builder()
                        .group(entry.getKey() + "|" + "Edge")
                        .vertex(entry.getValue().toString())
                        .property("source", edge.getSource())
                        .property("destination", edge.getDestination())
                        .property("directed", edge.isDirected())
                        .build());
            } else {
                final Entity entity = (Entity) element;
                elements.add(new Entity.Builder()
                        .group(entry.getKey() + "|" + "Entity")
                        .vertex(entry.getValue().toString())
                        .property("vertex", entity.getVertex())
                        .build());
            }
        }

        return elements;
    }
}
