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

package uk.gov.gchq.gaffer.basic.generator;

import uk.gov.gchq.gaffer.basic.ElementGroup;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

public class BasicElementGenerator implements OneToOneElementGenerator<String> {

    @Override
    public Element _apply(final String csv) {
        final String[] parts = csv.split(",");
        if (2 == parts.length) {
            return new Edge.Builder()
                    .group(ElementGroup.BASIC_EDGE)
                    .source(parts[0])
                    .dest(parts[1])
                    .directed(true)
                    .property("count", 1L)
                    .build();
        }

        if (1 == parts.length) {
            return new Entity.Builder()
                    .group(ElementGroup.BASIC_ENTITY)
                    .vertex(parts[0])
                    .property("count", 1L)
                    .build();
        }

        throw new IllegalArgumentException("CSV must contain 1 or 2 fields - 'vertex' or 'source,destination'");
    }
}
