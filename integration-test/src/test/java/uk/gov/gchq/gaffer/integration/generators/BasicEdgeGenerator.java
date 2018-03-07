/*
 * Copyright 2016-2018 Crown Copyright
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
public class BasicEdgeGenerator implements OneToOneElementGenerator<EdgeDomainObject> {
    @Override
    public Element _apply(final EdgeDomainObject domainObject) {
        return new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(domainObject.getSource())
                .dest(domainObject.getDestination())
                .directed(domainObject.getDirected())
                .property(TestPropertyNames.INT, domainObject.getIntProperty())
                .property(TestPropertyNames.COUNT, 1L)
                .build();
    }
}
