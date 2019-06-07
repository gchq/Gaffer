/*
 * Copyright 2016-2019 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneObjectGenerator;
import uk.gov.gchq.gaffer.integration.domain.EntityDomainObject;

/**
 * Implementation of {@link uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator} to translate between integration test 'edge'
 * object, and a Gaffer framework edge.
 * <br>
 * Allows translation of one domain object to one graph object only, where the domain object being translated is an instance
 * of {@link EntityDomainObject}.  The generator can go both ways (i.e. domain object to graph element and
 * graph element to domain object).
 */
public class EntityToObjectGenerator implements OneToOneObjectGenerator<EntityDomainObject> {
    @Override
    public EntityDomainObject _apply(final Element element) {
        if (element instanceof Entity) {
            final Entity entity = (Entity) element;
            final EntityDomainObject basicEntity = new EntityDomainObject();
            basicEntity.setName((String) entity.getVertex());
            basicEntity.setIntProperty((Integer) entity.getProperty(TestPropertyNames.INT));
            basicEntity.setStringproperty(entity.getProperty(TestPropertyNames.SET).toString().replaceAll("[\\[\\]]", ""));
            return basicEntity;
        }

        throw new IllegalArgumentException("Edges cannot be handled with this generator.");
    }
}
