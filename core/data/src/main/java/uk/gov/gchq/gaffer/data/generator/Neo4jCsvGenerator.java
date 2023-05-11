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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Generates a Neo4j CSV string for each Element, based on the fields and constants provided.
 */
@Since("2.0.0")
@Summary("Generates a Neo4j compatible CSV string for each element")
public class Neo4jCsvGenerator extends CsvGenerator {
    private final LinkedHashMap<String, String> neo4jFields = getIncludeDefaultFields() ? getDefaultFields() : new LinkedHashMap<>();

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
        return neo4jFields;
    }

    /**
     * Sets the field mapping for Neo4j Csvs
     */
    @Override
    protected LinkedHashMap<String, String> getDefaultFields() {
        final LinkedHashMap<String, String> defaultFields = new LinkedHashMap<>();
        defaultFields.put(String.valueOf(IdentifierType.VERTEX), "_id");
        defaultFields.put(ENTITY_GROUP, "_labels");
        defaultFields.put(EDGE_GROUP, "_type");
        defaultFields.put(String.valueOf(IdentifierType.SOURCE), "_start");
        defaultFields.put(String.valueOf(IdentifierType.DESTINATION), "_end");
        return defaultFields;
    }

    @Override
    public void addAdditionalFieldsFromSchemaProperties(final LinkedHashMap<String, Class<?>> schemaProperties) {
        if (getIncludeSchemaProperties()) {
            final LinkedHashMap<String, String> propertyFields = new LinkedHashMap<>();
            for (final Entry<String, Class<?>> schemaProperty : schemaProperties.entrySet()) {
                final String propertyTypeName = schemaProperty.getValue().getSimpleName();
                final String propertyNeo4jTypeName = TYPE_MAPPINGS.getOrDefault(propertyTypeName, "String");
                propertyFields.put(schemaProperty.getKey(), schemaProperty.getKey() + ":" + propertyNeo4jTypeName);
            }
            getFields().putAll(propertyFields);
        }
    }

    // Map of Java type name to csv type names
    public static final Map<String, String> TYPE_MAPPINGS  = Collections.unmodifiableMap(new HashMap<String, String>() { {
        put("String", "String");
        put("Character", "Char");
        put("Date", "Date");
        put("LocalDate", "LocalDate");
        put("LocalDateTime", "LocalDateTime");
        put("Point", "Point");
        put("Duration", "Duration");
        put("Integer", "Int");
        put("Short", "Short");
        put("Byte", "Byte");
        put("DateTime", "DateTime");
        put("Long", "Long");
        put("Double", "Double");
        put("Float", "Float");
        put("Boolean", "Boolean");
    } });
}
