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

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.DESTINATION;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.EDGE_GROUP;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.ENTITY_GROUP;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.NEO4J_DESTINATION;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.NEO4J_EDGE_GROUP;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.NEO4J_ENTITY_GROUP;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.NEO4J_SOURCE;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.NEO4J_VERTEX;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.SOURCE;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.VERTEX;

import java.util.LinkedHashMap;

/**
 * Generates an openCypher formatted CSV string for each {@link Element}. An Elements Vertex, Source, Destination
 * and Group are mapped to their openCypher equivalents, either AWS Neptune or Neo4j depending on whether the flag is set.
 * An Elements properties and their respective types are taken from the schema.
 */
@Since("2.0.0")
@Summary("Generates an openCypher formatted CSV string for each element")
public class OpenCypherCsvGenerator extends CsvGenerator {

    /**
     * When set to true, the headers will be formatted to match those used by neo4j.
     */
    private boolean neo4jFormat = false;

    /**
     * Attempts to find the value of a field from a given {@link Element},
     * corresponding to a provided key, where the key is the name of the field.
     *
     * @param element the Element from which to retrieve a field value
     * @param key     the name of the field to be retrieved
     * @return the value of the field
     */

    @Override
    protected Object getFieldValue(final Element element, final String key) {
        final IdentifierType idType = IdentifierType.fromName(key);
        final Object value;
        String entityGroup = isNeo4jFormat() ? "NEO4J_ENTITY_GROUP" : "ENTITY_GROUP";
        String edgeGroup = isNeo4jFormat() ? "NEO4J_EDGE_GROUP" : "EDGE_GROUP";
        if (null == idType) {
            if (entityGroup.equals(key) && element.getClassName().contains("Entity")) {
                value = element.getGroup();
            } else if (edgeGroup.equals(key) && element.getClassName().contains("Edge")) {
                value = element.getGroup();
            } else {
                value = element.getProperty(key);
            }
        } else {
            value = element.getIdentifier(idType);
        }
        return value;
    }

    @Override
    public void setFields(final LinkedHashMap<String, String> headersFromSchema) {
        if (neo4jFormat) {
            this.fields.put(String.valueOf(IdentifierType.VERTEX), NEO4J_VERTEX);
            this.fields.put("NEO4J_ENTITY_GROUP", NEO4J_ENTITY_GROUP);
            this.fields.put("NEO4J_EDGE_GROUP", NEO4J_EDGE_GROUP);
            this.fields.put(String.valueOf(IdentifierType.SOURCE), NEO4J_SOURCE);
            this.fields.put(String.valueOf(IdentifierType.DESTINATION), NEO4J_DESTINATION);

        } else {
            this.fields.put(String.valueOf(IdentifierType.VERTEX), VERTEX);
            this.fields.put("ENTITY_GROUP", ENTITY_GROUP);
            this.fields.put("EDGE_GROUP", EDGE_GROUP);
            this.fields.put(String.valueOf(IdentifierType.SOURCE), SOURCE);
            this.fields.put(String.valueOf(IdentifierType.DESTINATION), DESTINATION);
        }
        for (final String key: headersFromSchema.keySet()) {
            fields.put(key, key + ":" + headersFromSchema.get(key));
        }
    }
    public boolean isNeo4jFormat() {
        return neo4jFormat;
    }

    public void setNeo4jFormat(final boolean neo4jFormat) {
        this.neo4jFormat = neo4jFormat;
    }

    public static class Builder {
        private boolean neo4jFormat;
        private LinkedHashMap<String, String> headers = new LinkedHashMap<>();

        /**
         * Stores any additional properties of an {@link Element}.
         *
         * @param headersFromSchema the name of the headers to be added
         * @return a new {@link OpenCypherCsvGenerator.Builder}
         */
        public OpenCypherCsvGenerator.Builder headers(final LinkedHashMap<String, String> headersFromSchema) {
           this.headers = headersFromSchema;
           return this;
        }

        /**
         * Stores the flag for whether or not the header values should be formated to match that described by neo4j.
         *
         * @param neo4jFormat true or false
         * @return a new {@link OpenCypherCsvGenerator.Builder}
         */
        public OpenCypherCsvGenerator.Builder neo4jFormat(final boolean neo4jFormat) {
            this.neo4jFormat = neo4jFormat;
            return this;
        }

        /**
         * Passes all of the configured fields about an {@link Element} to a new {@link OpenCypherCsvGenerator},
         * and sets the flag for whether the headers should be formatted to match those described by neo4j
         *
         * @return a new {@code openCypherCsvGenerator}, containing all configured information
         */
        public OpenCypherCsvGenerator build() {
            final OpenCypherCsvGenerator generator = new OpenCypherCsvGenerator();
            generator.setNeo4jFormat(neo4jFormat);
            generator.setFields(headers);
            return generator;
        }
    }
}
