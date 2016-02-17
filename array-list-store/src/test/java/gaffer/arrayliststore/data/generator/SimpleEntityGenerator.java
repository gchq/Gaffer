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

package gaffer.arrayliststore.data.generator;

import gaffer.arrayliststore.data.SimpleEntityDataObject;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.AlwaysValid;
import gaffer.data.Validator;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.generator.OneToOneElementGenerator;

public class SimpleEntityGenerator extends OneToOneElementGenerator<SimpleEntityDataObject> {

    public SimpleEntityGenerator() {
        super(new Validator<Element>() {
            public boolean validate(final Element obj) {
                return obj instanceof Entity;
            }
        }, new AlwaysValid<SimpleEntityDataObject>(), false);
    }

    public Element getElement(final SimpleEntityDataObject simpleDataObject) {
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex(simpleDataObject.getId());
        entity.putProperty(TestPropertyNames.INT, simpleDataObject.getVisibility());
        entity.putProperty(TestPropertyNames.STRING, simpleDataObject.getProperties());
        return entity;
    }

    public SimpleEntityDataObject getObject(final Element element) {
        final Entity entity = (Entity) element;
        int id = (Integer) entity.getVertex();
        final Integer visibility = (Integer) entity.getProperty(TestPropertyNames.INT);
        final String properties = (String) entity.getProperty(TestPropertyNames.STRING);
        return new SimpleEntityDataObject(id, visibility, properties);
    }
}