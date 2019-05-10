/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.generator;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;

import java.util.Arrays;

public class TestGeneratorImpl implements OneToManyElementGenerator<String> {
    @Override
    public Iterable<Element> _apply(final String domainObject) {
        return Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(domainObject)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY_2)
                        .vertex(domainObject)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build()
        );
    }
}
