/*
 * Copyright 2017-2018 Crown Copyright
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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Enumeration of all protected property names.
 * These property names should not appear in any schemas used within a Gaffer graph.
 */
public enum ReservedPropertyNames {

    GROUP("group"),
    VERTEX("vertex"),
    SOURCE("src", "source"),
    DESTINATION("dst", "destination"),
    DIRECTED("directed"),
    MATCHED_VERTEX("matchedVertex"),
    ID("id");

    private static final List<ReservedPropertyNames> VALUES = Arrays.asList(values());

    private static final List<String> NAMES = VALUES.stream()
            .flatMap(ReservedPropertyNames::getNames)
            .collect(toList());

    private final List<String> nameList;

    ReservedPropertyNames(final String... names) {
        this.nameList = Arrays.stream(names)
                .flatMap(n -> Stream.of(n, n.toUpperCase()))
                .collect(toList());
    }

    public static boolean contains(final String property) {
        return NAMES.contains(property);
    }

    public Stream<String> getNames() {
        return nameList.stream();
    }

}
