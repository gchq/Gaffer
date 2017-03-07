/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.example.function.filter;

import uk.gov.gchq.gaffer.function.filter.CollectionContains;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class CollectionContainsExample extends FilterFunctionExample {
    public static void main(final String[] args) {
        new CollectionContainsExample().run();
    }

    public CollectionContainsExample() {
        super(CollectionContains.class);
    }

    public void runExamples() {
        collectionContains();
    }

    public void collectionContains() {
        // ---------------------------------------------------------
        final CollectionContains function = new CollectionContains(1);
        // ---------------------------------------------------------

        runExample(function,
                createSet(1, 2, 3),
                createSet(1),
                createSet(2),
                createSet());
    }

    public Set<Object> createSet(final Object... values) {
        final Set<Object> set = new HashSet<>(values.length);
        Collections.addAll(set, values);
        return set;
    }
}
