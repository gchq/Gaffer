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

package uk.gov.gchq.gaffer.rest.example;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

public class ExampleDomainObjectGenerator extends OneToOneElementGenerator<ExampleDomainObject> {
    @Override
    public Element getElement(final ExampleDomainObject obj) {
        if (obj.getIds().length > 1) {
            final Edge edge = new Edge(obj.getType());
            edge.setSource(obj.getIds()[0]);
            edge.setDestination(obj.getIds()[1]);
            if (obj.getIds().length > 2) {
                edge.setDirected(Boolean.TRUE.equals(obj.getIds()[2]));
            }

            return edge;
        }

        final Entity entity = new Entity(obj.getType());
        entity.setVertex(obj.getIds()[0]);
        return entity;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public ExampleDomainObject getObject(final Element element) {
        if (element instanceof Entity) {
            return new ExampleDomainObject(element.getGroup(), ((Entity) element).getVertex());
        }

        return new ExampleDomainObject(element.getGroup(),
                ((Edge) element).getSource(), ((Edge) element).getDestination(), ((Edge) element).isDirected());
    }
}
