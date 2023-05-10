/*
 * Copyright 2016-2023 Crown Copyright
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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

/**
 * Generates a CSV string for each {@link Element}, based on the fields and constants provided.
 * <p>
 * For example, if you provide:<pre>
 *  fields=[prop1, SOURCE, DESTINATION, prop2, GROUP] and constants=["constant1", "constant2"]
 * </pre>
 * The output will be:<pre>
 *  prop1Value,sourceValue,destinationValue,prop2,groupValue,constant1,constant2
 * </pre>
 */
@Since("1.0.0")
@Summary("Generates a CSV string for each element")
public class CsvGenerator implements OneToOneObjectGenerator<String> {
    // Custom identifiers used in case you want seperate Edge and Entity group columns
    protected static final String ENTITY_GROUP = "ENTITY_GROUP";
    protected static final String EDGE_GROUP = "EDGE_GROUP";

    public static final String COMMA = ",";
    private static final Pattern COMMA_PATTERN = Pattern.compile(COMMA);
    private static final String COMMA_REPLACEMENT_DEFAULT = " ";

    /**
     * When set to true, fields will be set to {@link #getDefaultFields()}.
     */
    private boolean includeDefaultFields = false;

    /**
     * Map of fields that define what is included in the csv.
     * Key is the IdentifierType or property and value is the associated header.
     */
    private LinkedHashMap<String, String> fields = getIncludeDefaultFields() ? getDefaultFields() : new LinkedHashMap<>();

    private LinkedHashMap<String, String> constants = new LinkedHashMap<>();

    /**
     * When set to true, schema properties can get added to fields.
     * This is used by the ToCsvHandler to add all of the properties in the Schema to fields.
     */
    private boolean includeSchemaProperties = false;

    /**
     * When set to true, each value in the csv will be wrapped in quotes.
     */
    private boolean quoted = false;

    /**
     * Replaces commas with this string. If null then no replacement is done.
     */
    private String commaReplacement = COMMA_REPLACEMENT_DEFAULT;

    /**
     * Used to determine the default fields for different CsvGenerators.
     * Default implementation returns basic Indentifiers.
     *
     * @return the default fields for the class
     */
    protected LinkedHashMap<String, String> getDefaultFields() {
        final LinkedHashMap<String, String> defaultFields = new LinkedHashMap<>();
        defaultFields.put(String.valueOf(IdentifierType.VERTEX), String.valueOf(IdentifierType.VERTEX));
        defaultFields.put(String.valueOf(IdentifierType.GROUP), String.valueOf(IdentifierType.GROUP));
        defaultFields.put(String.valueOf(IdentifierType.SOURCE), String.valueOf(IdentifierType.SOURCE));
        defaultFields.put(String.valueOf(IdentifierType.DESTINATION), String.valueOf(IdentifierType.DESTINATION));
        return defaultFields;
    }

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
        if (null == idType) {
            if (key.equals(ENTITY_GROUP) && element.getClass().equals(Entity.class)) {
                value = element.getGroup();
            } else if (key.equals(EDGE_GROUP) && element.getClass().equals(Edge.class)) {
                value = element.getGroup();
            } else {
                value = element.getProperty(key);
            }

        } else {
            value = element.getIdentifier(idType);
        }
        return value;
    }

    public LinkedHashMap<String, String> getFields() {
        return fields;
    }

    public void setFields(final LinkedHashMap<String, String> fields) {
        if (null == fields) {
            this.fields = getIncludeDefaultFields() ? getDefaultFields() : new LinkedHashMap<>();
        }
        this.fields = fields;
    }

    public LinkedHashMap<String, String> getConstants() {
        return constants;
    }

    public void setConstants(final LinkedHashMap<String, String> constants) {
        if (null == constants) {
            this.constants = new LinkedHashMap<>();
        }
        this.constants = constants;
    }

    public boolean getIncludeDefaultFields() {
        return includeDefaultFields;
    }

    public void setIncludeDefaultFields(final boolean includeDefaultFields) {
        this.includeDefaultFields = includeDefaultFields;
    }

    public boolean getIncludeSchemaProperties() {
        return includeSchemaProperties;
    }

    public void setIncludeSchemaProperties(final boolean includeSchemaProperties) {
        this.includeSchemaProperties = includeSchemaProperties;
    }

    /**
     * Adds all properties from a schema to the fields if
     * includeSchemaProperties is true.
     *
     * @param schemaProperties a Map of property names to types.
     */
    public void addAdditionalFieldsFromSchemaProperties(final LinkedHashMap<String, Class<?>> schemaProperties) {
        if (getIncludeSchemaProperties()) {
            for (final String propertyName : schemaProperties.keySet()) {
                getFields().put(propertyName, propertyName);
            }
        }
    }

    @Override
    public String _apply(final Element element) {
        final StringBuilder strBuilder = new StringBuilder();
        for (final String field : getFields().keySet()) {
            final Object value = getFieldValue(element, field);
            if (null != value) {
                strBuilder.append(quoteString(value));
            }
            strBuilder.append(COMMA);
        }

        if (!constants.isEmpty()) {
            for (final String constant : constants.keySet()) {
                strBuilder.append(quoteString(constant));
                strBuilder.append(COMMA);
            }
        }

        if (strBuilder.length() < 1) {
            return "";
        }

        return strBuilder.substring(0, strBuilder.length() - 1);
    }

    /**
     * Generates a CSV String from, if present, the fields and constants
     *
     * @return a CSV String of all fields, or constants, or both.
     */
    @JsonIgnore
    public String getHeader() {
        if (getFields().isEmpty()) {
            if (constants.isEmpty()) {
                return "";
            }
            return getHeaderFields(constants.values());
        }

        if (constants.isEmpty()) {
            return getHeaderFields(getFields().values());
        }

        return getHeaderFields(getFields().values()) + COMMA + getHeaderFields(constants.values());
    }

    private String getHeaderFields(final Collection<String> fields) {
        return StringUtils.join(fields.stream().map(this::quoteString).toArray(), COMMA);
    }

    private String quoteString(final Object s) {
        String value;
        if (null == s) {
            value = "";
        } else {
            value = s.toString();
        }

        if (null != commaReplacement) {
            value = COMMA_PATTERN.matcher(value).replaceAll(commaReplacement);
        }

        if (quoted) {
            value = "\"" + value + "\"";
        }

        return value;
    }

    public boolean isQuoted() {
        return quoted;
    }

    public void setQuoted(final boolean quoted) {
        this.quoted = quoted;
    }

    public String getCommaReplacement() {
        return commaReplacement;
    }

    public void setCommaReplacement(final String commaReplacement) {
        this.commaReplacement = commaReplacement;
    }

    public static class Builder {
        private final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        private final LinkedHashMap<String, String> constants = new LinkedHashMap<>();
        private final LinkedHashMap<String, Class<?>> schemaProperties = new LinkedHashMap<>();

        private String commaReplacement = COMMA_REPLACEMENT_DEFAULT;
        private Boolean quoted;

        /**
         * Stores the group of an {@link Element}.
         *
         * @param columnHeader the group of the {@code Element}
         * @return a new {@link Builder}
         */
        public Builder group(final String columnHeader) {
            return identifier(IdentifierType.GROUP, columnHeader);
        }

        /**
         * Stores the group of an {@link Entity} on a seperate column.
         *
         * @param columnHeader the group of the {@code Entity}
         * @return a new {@link Builder}
         */
        public Builder entityGroup(final String columnHeader) {
            return property(ENTITY_GROUP, columnHeader);
        }

        /**
         * Stores the group of an {@link Edge} on a seperate column.
         *
         * @param columnHeader the group of the {@code Edge}
         * @return a new {@link Builder}
         */
        public Builder edgeGroup(final String columnHeader) {
            return property(EDGE_GROUP, columnHeader);
        }

        /**
         * Stores any additional properties of an {@link Element}.<br>
         * For example: property("count", "3").<br>
         * This would add the "count" property with a value of "3"
         *
         * @param propertyName the name of the property
         * @param columnHeader the value of the property
         * @return a new {@link Builder}
         */
        public Builder property(final String propertyName, final String columnHeader) {
            fields.put(propertyName, columnHeader);
            return this;
        }

        /**
         * Stores the Vertex of an {@link uk.gov.gchq.gaffer.data.element.Entity}
         *
         * @param columnHeader the vertex contained within the {@code Entity}
         * @return a new {@link Builder}
         */
        public Builder vertex(final String columnHeader) {
            return identifier(IdentifierType.VERTEX, columnHeader);
        }

        /**
         * Stores the Source Vertex of an {@link uk.gov.gchq.gaffer.data.element.Edge}.
         *
         * @param columnHeader the source vertex
         * @return a new {@link Builder}
         */
        public Builder source(final String columnHeader) {
            return identifier(IdentifierType.SOURCE, columnHeader);
        }

        /**
         * Stores the Destination Vertex of an {@link uk.gov.gchq.gaffer.data.element.Entity}
         *
         * @param columnHeader the destination vertex
         * @return a new {@link Builder}
         */
        public Builder destination(final String columnHeader) {
            return identifier(IdentifierType.DESTINATION, columnHeader);
        }

        /**
         * Stores the Direction flag, indicating whether or not the {@link uk.gov.gchq.gaffer.data.element.Edge}
         * is directed.
         *
         * @param columnHeader true or false for if the {@code Edge} is directed or not
         * @return a new {@link Builder}
         */
        public Builder direction(final String columnHeader) {
            return identifier(IdentifierType.DIRECTED, columnHeader);
        }

        /**
         * Allows an {@link IdentifierType} of an {@link Element} to be stored, such as
         * an {@link uk.gov.gchq.gaffer.data.element.Edge}'s {@link IdentifierType#MATCHED_VERTEX}.
         *
         * @param identifierType the {@code IdentifierType} of the {@code Element}
         * @param columnHeader   the value for the corresponding field
         * @return a new {@link Builder}
         */
        public Builder identifier(final IdentifierType identifierType, final String columnHeader) {
            fields.put(identifierType.name(), columnHeader);
            return this;
        }

        /**
         * Stores any constants specific to a given {@link Element}.
         *
         * @param key   the name of the constant
         * @param value the value of the constant
         * @return a new {@link Builder}
         */
        public Builder constant(final String key, final String value) {
            constants.put(key, value);
            return this;
        }

        /**
         * Saves all properties from the schema which be added to fields.
         *
         * @param schemaProperties   the Map of property names to type
         * @return a new {@link Builder}
         */
        public Builder setAdditionalFieldsFromSchemaProperties(final LinkedHashMap<String, Class<?>> schemaProperties) {
            this.schemaProperties.putAll(schemaProperties);
            return this;
        }

        /**
         * Stores the String with which any encountered commas will be replaced.
         *
         * @param commaReplacement the replacement String
         * @return a new {@link Builder}
         */
        public Builder commaReplacement(final String commaReplacement) {
            this.commaReplacement = commaReplacement;
            return this;
        }

        /**
         * Stores the flag for whether or not each distinct value should be wrapped in quotation marks.
         *
         * @param quoted true or false
         * @return a new {@link Builder}
         */
        public Builder quoted(final boolean quoted) {
            this.quoted = quoted;
            return this;
        }

        /**
         * Passes all the configured fields and constants about an {@link Element} to a new {@link CsvGenerator},
         * including the comma replacement String, and the flag for whether values should be quoted.
         *
         * @return a new {@code CsvGenerator}, containing all configured information
         */
        public CsvGenerator build() {
            final CsvGenerator generator = new CsvGenerator();
            generator.setFields(fields);
            generator.setConstants(constants);
            generator.addAdditionalFieldsFromSchemaProperties(schemaProperties);
            generator.setCommaReplacement(commaReplacement);
            if (null != quoted) {
                generator.setQuoted(quoted);
            }
            return generator;
        }
    }
}
