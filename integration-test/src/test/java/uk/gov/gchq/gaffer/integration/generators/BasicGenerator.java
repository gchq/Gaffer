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

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.integration.domain.DomainObject;
import uk.gov.gchq.gaffer.integration.domain.EdgeDomainObject;
import uk.gov.gchq.gaffer.integration.domain.EntityDomainObject;

/**
 * Implementation of {@link OneToOneElementGenerator} to translate between integration test 'edge'
 * object, and a Gaffer framework edge.
 * <br>
 * Allows translation of one domain object to one graph object only, where the domain object being translated is an instance
 * of {@link EntityDomainObject}.  The generator can go both ways (i.e. domain object to graph element and
 * graph element to domain object).
 */
public class BasicGenerator extends OneToOneElementGenerator<DomainObject> {
    private final BasicEntityGenerator entityGenerator = new BasicEntityGenerator();
    private final BasicEdgeGenerator edgeGenerator = new BasicEdgeGenerator();

    @Override
    public Element getElement(final DomainObject domainObject) {
        if (domainObject instanceof EntityDomainObject) {
            return entityGenerator.getElement((EntityDomainObject) domainObject);
        }

        return edgeGenerator.getElement((EdgeDomainObject) domainObject);
    }

    @Override
    public DomainObject getObject(final Element element) {
        if (element instanceof Entity) {
            return entityGenerator.getObject(element);
        }

        return edgeGenerator.getObject(element);
    }
}
