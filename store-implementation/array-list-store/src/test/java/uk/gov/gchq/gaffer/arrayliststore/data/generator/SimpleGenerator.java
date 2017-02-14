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

package uk.gov.gchq.gaffer.arrayliststore.data.generator;

import uk.gov.gchq.gaffer.arrayliststore.data.SimpleEdgeDataObject;
import uk.gov.gchq.gaffer.arrayliststore.data.SimpleEntityDataObject;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

public class SimpleGenerator extends OneToOneElementGenerator<Object> {
    private final SimpleEntityGenerator entityConverter = new SimpleEntityGenerator();
    private final SimpleEdgeGenerator edgeConverter = new SimpleEdgeGenerator();

    public Element getElement(final Object obj) {
        if (obj instanceof SimpleEntityDataObject) {
            return entityConverter.getElement(((SimpleEntityDataObject) obj));
        }

        if (obj instanceof SimpleEdgeDataObject) {
            return edgeConverter.getElement(((SimpleEdgeDataObject) obj));
        }

        throw new IllegalArgumentException("This converter can only handle objects of type SimpleEntityDataObject and SimpleEdgeDataObject");
    }

    public Object getObject(final Element element) {
        if (TestGroups.ENTITY.equals(element.getGroup())) {
            return entityConverter.getObject(element);
        }

        if (TestGroups.EDGE.equals(element.getGroup())) {
            return edgeConverter.getObject(element);
        }

        throw new IllegalArgumentException("This converter can only handle elements with group  "
                + TestGroups.ENTITY + ", or " + TestGroups.EDGE);
    }
}