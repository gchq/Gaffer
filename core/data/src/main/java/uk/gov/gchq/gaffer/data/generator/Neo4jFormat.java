/*
 * Copyright 2022 Crown Copyright
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

public class Neo4jFormat implements CsvFormat {

    public static final String NEO4J_VERTEX = "_id";
    public static final String NEO4J_ENTITY_GROUP = "_labels";
    public static final String NEO4J_SOURCE = "_start";
    public static final String NEO4J_DESTINATION = "_end";
    public static final String NEO4J_EDGE_GROUP = "_type";

    public static final LinkedHashMap<String, String> IDENTIFIERS = new LinkedHashMap<String, String>() { {
        put(String.valueOf(IdentifierType.VERTEX), NEO4J_VERTEX);
        put("NEO4J_ENTITY_GROUP", NEO4J_ENTITY_GROUP);
        put("NEO4J_EDGE_GROUP", NEO4J_EDGE_GROUP);
        put(String.valueOf(IdentifierType.SOURCE), NEO4J_SOURCE);
        put(String.valueOf(IdentifierType.DESTINATION), NEO4J_DESTINATION);
    } };

    @Override
    public LinkedHashMap<String, String> getIdentifiers() {
        return IDENTIFIERS;
    }
}
