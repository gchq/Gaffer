/*
 * Copyright 2023 Crown Copyright
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
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.LinkedHashMap;

/**
 * Generates a Neptune CSV string for each Element, based on the fields and constants provided.
 */
@Since("2.0.0")
@Summary("Generates a Neptune compatible CSV string for each element")
public class NeptuneCsvGenerator extends Neo4jCsvGenerator {
    private final LinkedHashMap<String, String> neptuneFields = getIncludeDefaultFields() ? getDefaultFields() : new LinkedHashMap<>();

    @Override
    public boolean getIncludeDefaultFields() {
        return true;
    }

    @Override
    public boolean getIncludeSchemaProperties() {
        return true;
    }

    @Override
    public LinkedHashMap<String, String> getFields() {
        return neptuneFields;
    }

    /**
     * Sets the field mapping for Neptune Csvs
     */
    @Override
    protected LinkedHashMap<String, String> getDefaultFields() {
        final LinkedHashMap<String, String> defaultFields = new LinkedHashMap<>();
        defaultFields.put(IdentifierType.VERTEX.name(), ":ID");
        defaultFields.put(ENTITY_GROUP, ":LABEL");
        defaultFields.put(EDGE_GROUP, ":TYPE");
        defaultFields.put(IdentifierType.SOURCE.name(), ":START_ID");
        defaultFields.put(IdentifierType.DESTINATION.name(), ":END_ID");
        return defaultFields;
    }
}
