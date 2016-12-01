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
package uk.gov.gchq.gaffer.integration.generators;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.integration.domain.EdgeDomainObject;

/**
 * Implementation of {@link uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator} to translate between integration test 'edge'
 * object, and a Gaffer framework edge.
 * <br>
 * Allows translation of one domain object to one graph object only, where the domain object being translated is an instance
 * of {@link uk.gov.gchq.gaffer.integration.domain.EdgeDomainObject}.  The generator can go both ways (i.e. domain object to graph element and
 * graph element to domain object).
 */
public class BasicEdgeGenerator extends OneToOneElementGenerator<EdgeDomainObject> {
    @Override
    public Element getElement(final EdgeDomainObject domainObject) {
        final Edge edge = new Edge(TestGroups.EDGE, domainObject.getSource(), domainObject.getDestination(), domainObject.getDirected());
        edge.putProperty(TestPropertyNames.INT, domainObject.getIntProperty());
        edge.putProperty(TestPropertyNames.COUNT, 1L);
        return edge;
    }

    @Override
    public EdgeDomainObject getObject(final Element element) {
        if (element instanceof Edge) {
            final Edge edge = ((Edge) element);
            final EdgeDomainObject basicEdge = new EdgeDomainObject();
            basicEdge.setSource((String) edge.getSource());
            basicEdge.setDestination((String) edge.getDestination());
            basicEdge.setDirected(edge.isDirected());
            basicEdge.setCount((Long) edge.getProperty(TestPropertyNames.COUNT));
            basicEdge.setIntProperty((Integer) edge.getProperty(TestPropertyNames.INT));
            return basicEdge;
        }

        throw new IllegalArgumentException("Entities cannot be handled with this generator.");
    }
}
