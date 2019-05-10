/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.data.element;

import uk.gov.gchq.gaffer.commonutil.TestGroups;

public final class TestElements {

    private TestElements() {
        // Private to avoid instantiation
    }

    public static Edge getBasicEdge(final Object source, final Object destination) {
        return new Edge.Builder()
                .source(source)
                .dest(destination)
                .group(TestGroups.EDGE)
                .directed(true)
                .build();
    }

    public static Entity getBasicEntity(final Object vertex) {
        return new Entity.Builder()
                .vertex(vertex)
                .group(TestGroups.ENTITY)
                .build();
    }
}
