/*
 * Copyright 2016-2018 Crown Copyright
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
    public static final String GROUP = "GROUP";
    public static final String COMMA = ",";
    private static final Pattern COMMA_PATTERN = Pattern.compile(COMMA);
    private static final String COMMA_REPLACEMENT_DEFAULT = " ";

    private LinkedHashMap<String, String> fields = new LinkedHashMap<>();

    private LinkedHashMap<String, String> constants = new LinkedHashMap<>();

    /**
     * When set to true, each value in the csv will be wrapped in quotes.
     */
    private boolean quoted = false;

    /**
     * Replaces commas with this string. If null then no replacement is done.
     */
    private String commaReplacement = COMMA_REPLACEMENT_DEFAULT;

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
            if (GROUP.equals(key)) {
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
            this.fields = new LinkedHashMap<>();
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


    @Override
    public String _apply(final Element element) {
        final StringBuilder strBuilder = new StringBuilder();
        for (final String field : fields.keySet()) {
            final Object value = getFieldValue(element, field);
            if (null != value) {
                strBuilder.append(quoteString(value));
                strBuilder.append(COMMA);
            }
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
        if (fields.isEmpty()) {
            if (constants.isEmpty()) {
                return "";
            }
            return getHeaderFields(constants.values());
        }

        if (constants.isEmpty()) {
            return getHeaderFields(fields.values());
        }

        return getHeaderFields(fields.values()) + COMMA + getHeaderFields(constants.values());
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
        private LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        private LinkedHashMap<String, String> constants = new LinkedHashMap<>();
        private String commaReplacement = COMMA_REPLACEMENT_DEFAULT;
        private Boolean quoted;

        /**
         * Stores the group of an {@link Element}.
         *
         * @param columnHeader the group of the {@code Element}
         * @return a new {@link Builder}
         */
        public Builder group(final String columnHeader) {
            fields.put(GROUP, columnHeader);
            return this;
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
         * Passes all of the configured fields and constants about an {@link Element} to a new {@link CsvGenerator},
         * including the comma replacement String, and the flag for whether values should be quoted.
         *
         * @return a new {@code CsvGenerator}, containing all configured information
         */
        public CsvGenerator build() {
            final CsvGenerator generator = new CsvGenerator();
            generator.setFields(fields);
            generator.setConstants(constants);
            generator.setCommaReplacement(commaReplacement);
            if (null != quoted) {
                generator.setQuoted(quoted);
            }

            return generator;
        }
    }
}
