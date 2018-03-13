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

package uk.gov.gchq.gaffer.rest.example;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

public class ExampleElementGenerator implements OneToOneElementGenerator<ExampleDomainObject> {
    @Override
    public Element _apply(final ExampleDomainObject obj) {
        if (obj.getIds().length > 1) {
            final Edge.Builder builder = new Edge.Builder()
                    .group(obj.getType())
                    .source(obj.getIds()[0])
                    .dest(obj.getIds()[1]);
            if (obj.getIds().length > 2) {
                builder.directed(Boolean.TRUE.equals(obj.getIds()[2]));
            }

            return builder.build();
        }

        return new Entity.Builder()
                .group(obj.getType())
                .vertex(obj.getIds()[0])
                .build();
    }
}
