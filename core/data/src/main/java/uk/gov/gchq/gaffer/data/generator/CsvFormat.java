/*
 * Copyright 2022-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.data.generator;

import uk.gov.gchq.gaffer.data.element.IdentifierType;

import java.util.LinkedHashMap;

public abstract class CsvFormat {
    public static final String ENTITY_GROUP = "ENTITY_GROUP";
    public static final String EDGE_GROUP = "EDGE_GROUP";

    public abstract String getVertex();
    public abstract String getEntityGroup();
    public abstract String getEdgeGroup();
    public abstract String getSource();
    public abstract String getDestination();

    public static LinkedHashMap<String, String> getIdentifiers(final CsvFormat csvFormat) {
        final LinkedHashMap<String, String> identifiers = new LinkedHashMap<>();
        identifiers.put(String.valueOf(IdentifierType.VERTEX), csvFormat.getVertex());
        identifiers.put(ENTITY_GROUP, csvFormat.getEntityGroup());
        identifiers.put(EDGE_GROUP, csvFormat.getEdgeGroup());
        identifiers.put(String.valueOf(IdentifierType.SOURCE), csvFormat.getSource());
        identifiers.put(String.valueOf(IdentifierType.DESTINATION), csvFormat.getDestination());
        return identifiers;
    }
}

