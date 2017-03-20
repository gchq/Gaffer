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

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToOneObjectGenerator;

public class SimpleObjectGenerator implements OneToOneObjectGenerator<Object> {
    private final SimpleEntityToObjectGenerator entityConverter = new SimpleEntityToObjectGenerator();
    private final SimpleEdgeToObjectGenerator edgeConverter = new SimpleEdgeToObjectGenerator();

    @Override
    public Object _apply(final Element element) {
        if (TestGroups.ENTITY.equals(element.getGroup())) {
            return entityConverter._apply(element);
        }

        if (TestGroups.EDGE.equals(element.getGroup())) {
            return edgeConverter._apply(element);
        }

        throw new IllegalArgumentException("This converter can only handle elements with group  "
                + TestGroups.ENTITY + ", or " + TestGroups.EDGE);
    }
}