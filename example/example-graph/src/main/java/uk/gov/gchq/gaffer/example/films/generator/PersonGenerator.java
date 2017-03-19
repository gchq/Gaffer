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

package uk.gov.gchq.gaffer.example.films.generator;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.example.films.data.Person;
import uk.gov.gchq.gaffer.example.films.data.schema.Group;
import uk.gov.gchq.gaffer.example.films.data.schema.Property;

public class PersonGenerator extends OneToOneElementGenerator<Person> {
    @Override
    public Element getElement(final Person person) {
        final Entity entity = new Entity(Group.PERSON, person.getUserId());
        entity.putProperty(Property.NAME, person.getName());
        entity.putProperty(Property.AGE, person.getAge());

        return entity;
    }

    @Override
    public Person getObject(final Element element) {
        if (Group.PERSON.equals(element.getGroup()) && element instanceof Entity) {
            return new Person(((Entity) element).getVertex().toString(),
                    element.getProperty(Property.NAME).toString(),
                    (Integer) element.getProperty(Property.AGE));
        }

        throw new UnsupportedOperationException("Cannot generate Person object from " + element);
    }
}
