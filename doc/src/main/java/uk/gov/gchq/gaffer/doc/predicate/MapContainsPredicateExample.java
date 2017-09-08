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
package uk.gov.gchq.gaffer.doc.predicate;

import uk.gov.gchq.koryphe.impl.predicate.MapContains;
import uk.gov.gchq.koryphe.impl.predicate.MapContainsPredicate;
import uk.gov.gchq.koryphe.impl.predicate.Regex;

import java.util.HashMap;
import java.util.Map;

public class MapContainsPredicateExample extends PredicateExample {
    private final Map<String, String> map = new HashMap<>();
    final Map<String, String> mapNoA = new HashMap<>();
    final Map<String, String> mapNullA = new HashMap<>();

    public static void main(final String[] args) {
        new MapContainsPredicateExample().run();
    }

    public MapContainsPredicateExample() {
        super(MapContains.class);

        map.put("a1", "1");
        map.put("a2", "2");
        map.put("b", "2");
        map.put("c", "3");

        mapNoA.put("b", "2");
        mapNoA.put("c", "3");

        mapNullA.put("a", null);
        mapNullA.put("b", "2");
        mapNullA.put("c", "3");
    }

    @Override
    public void runExamples() {
        mapContainsPredicate();
    }

    public void mapContainsPredicate() {
        // ---------------------------------------------------------
        final MapContainsPredicate function = new MapContainsPredicate(new Regex("a.*"));
        // ---------------------------------------------------------

        runExample(function,
                null,
                map, mapNoA, mapNullA);
    }
}
