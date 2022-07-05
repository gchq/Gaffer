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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

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

import java.util.Collection;
import java.util.LinkedHashMap;

/**
 * Generates an openCypher formatted CSV string for each {@link Element}, based on the fields and constants provided.
 * <p>
 * For example, if you provide:<pre>
 *  fields=[prop1, SOURCE, DESTINATION, prop2, GROUP] and constants=["constant1", "constant2"]
 * </pre>
 * The output will be:<pre>
 *  prop1Value,sourceValue,destinationValue,prop2,groupValue,constant1,constant2
 * </pre>
 */
@Since("2.0.0")
@Summary("Generates an openCypher formatted CSV string for each element")
public class OpenCypherCsvGenerator extends CsvGenerator {
    private LinkedHashMap<String, String> fields = new LinkedHashMap<>();

    /**
     * When set to true, each value in the csv will be wrapped in quotes.
     */
    private boolean quoted = false;

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

    private Object getFieldValue(final Element element, final String key) {
        final IdentifierType idType = IdentifierType.fromName(key);
        final Object value;
        String entityGroup = isNeo4jFormat() ? "NEO4J_ENTITY_GROUP" : "ENTITY_GROUP";
        String edgeGroup = isNeo4jFormat() ? "NEO4J_EDGE_GROUP" : "EDGE_GROUP";
        if (null == idType) {
            if (entityGroup.equals(key) && element.getClassName().contains("Entity")) {
                value = element.getGroup();
            } else  if (edgeGroup.equals(key) && element.getClassName().contains("Edge")) {
                value = element.getGroup();
            } else {
                value = element.getProperty(key);
            }
        } else {
            value = element.getIdentifier(idType);
        }
        return value;
    }

    public void setFields(final LinkedHashMap<String, String> headersFromSchema) {
        if (neo4jFormat) {
            this.fields.put(String.valueOf(IdentifierType.VERTEX), NEO4J_VERTEX + ":" + headersFromSchema.get(String.valueOf(IdentifierType.VERTEX)));
            this.fields.put("NEO4J_ENTITY_GROUP", NEO4J_ENTITY_GROUP + ":string");
            this.fields.put("NEO4J_EDGE_GROUP", NEO4J_EDGE_GROUP + ":string");
            this.fields.put(String.valueOf(IdentifierType.SOURCE), NEO4J_SOURCE + ":" + headersFromSchema.get(String.valueOf(IdentifierType.SOURCE)));
            this.fields.put(String.valueOf(IdentifierType.DESTINATION), NEO4J_DESTINATION + ":" + headersFromSchema.get(String.valueOf(IdentifierType.DESTINATION)));

        } else {
            this.fields.put(String.valueOf(IdentifierType.VERTEX), VERTEX + ":" + headersFromSchema.get(String.valueOf(IdentifierType.VERTEX)));
            this.fields.put("ENTITY_GROUP", ENTITY_GROUP + ":string");
            this.fields.put("EDGE_GROUP", EDGE_GROUP + ":string");
            this.fields.put(String.valueOf(IdentifierType.SOURCE), SOURCE + ":" + headersFromSchema.get(String.valueOf(IdentifierType.SOURCE)));
            this.fields.put(String.valueOf(IdentifierType.DESTINATION), DESTINATION + ":" + headersFromSchema.get(String.valueOf(IdentifierType.DESTINATION)));
        }
        headersFromSchema.remove(String.valueOf(IdentifierType.VERTEX));
        headersFromSchema.remove(String.valueOf(IdentifierType.SOURCE));
        headersFromSchema.remove(String.valueOf(IdentifierType.DESTINATION));
        //System.out.println(headersFromSchema);
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

    public LinkedHashMap<String, String> getFields() {
        return fields;
    }

    @JsonIgnore
    @Override
    public String getHeader() {
        LinkedHashMap<String, String> fields = getFields();
        return getHeaderFields(fields.values());
    }
    private String getHeaderFields(final Collection<String> fields) {
        return StringUtils.join(fields.stream().map(this::quoteString).toArray(), COMMA);
    }

   @Override
    public String _apply(final Element element) {
        final StringBuilder strBuilder = new StringBuilder();
        for (final String field : fields.keySet()) {
            final Object value = getFieldValue(element, field);

            if (null != value) {
                strBuilder.append(quoteString(value));
            }
            strBuilder.append(COMMA);
        }

        if (strBuilder.length() < 1) {
            return "";
        }

        return strBuilder.substring(0, strBuilder.length() - 1);
    }

    public static class Builder {
        private Boolean neo4jFormat;
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
         * Stores the Direction flag, indicating whether or not the {@link uk.gov.gchq.gaffer.data.element.Edge}
         * is directed.
         *
         * @param columnHeader true or false for if the {@code Edge} is directed or not
         * @return a new {@link OpenCypherCsvGenerator.Builder}
         */
        public OpenCypherCsvGenerator.Builder direction(final String columnHeader) {
            return identifier(IdentifierType.DIRECTED, columnHeader);
        }

        /**
         * Allows an {@link IdentifierType} of an {@link Element} to be stored, such as
         * an {@link uk.gov.gchq.gaffer.data.element.Edge}'s {@link IdentifierType#MATCHED_VERTEX}.
         *
         * @param identifierType the {@code IdentifierType} of the {@code Element}
         * @param columnHeader   the value for the corresponding field
         * @return a new {@link OpenCypherCsvGenerator.Builder}
         */
        public OpenCypherCsvGenerator.Builder identifier(final IdentifierType identifierType, final String columnHeader) {
            headers.put(identifierType.name(), columnHeader);
            return this;
        }

        /**
         * Stores the flag for whether or not each distinct value should be wrapped in quotation marks.
         *
         * @param neo4jFormat true or false
         * @return a new {@link OpenCypherCsvGenerator.Builder}
         */
        public OpenCypherCsvGenerator.Builder neo4jFormat(final boolean neo4jFormat) {
            this.neo4jFormat = neo4jFormat;
            return this;
        }

        /**
         * Passes all of the configured fields and flags about an {@link Element} to a new {@link OpenCypherCsvGenerator},
         * including the comma replacement String, and the flag for whether values should be quoted.
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
