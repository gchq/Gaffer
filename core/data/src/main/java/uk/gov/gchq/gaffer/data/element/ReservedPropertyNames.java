/*
 * Copyright 2017 Crown Copyright
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
import java.util.stream.Collectors;

/**
 * Enumeration of all protected property names.
 */
public enum ReservedPropertyNames {

    VERTEX("vertex"),
    SOURCE("src"),
    DESTINATION("dst"),
    DIRECTED("directed"),
    MATCHED_VERTEX("matchedVertex"),
    ID("id");

    private static final List<ReservedPropertyNames> VALUES = Arrays.asList(values());
    private static final List<String> NAMES = VALUES.stream()
            .map(ReservedPropertyNames::getName)
            .collect(Collectors.toList());

    private final String name;

    ReservedPropertyNames(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static boolean contains(final String property) {
        return NAMES.contains(property);
    }

    @Override
    public String toString() {
        return name;
    }
}
