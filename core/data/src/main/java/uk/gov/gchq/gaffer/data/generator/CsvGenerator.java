/*
 * Copyright 2016-2017 Crown Copyright
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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

/**
 * Generates a CSV string for each element, based on the fields and constants provided.
 * <p>
 * For example, if you provide fields=[prop1, SOURCE, DESTINATION, prop2, GROUP] and constants=["constant1", "constant2"]
 * The output will be:
 * prop1Value,sourceValue,destinationValue,prop2,groupValue,constant1,constant2
 * </p>
 */
public class CsvGenerator implements OneToOneObjectGenerator<String> {
    public static final String GROUP = "GROUP";
    public static final String COMMA = ",";
    private LinkedHashMap<String, String> fields = new LinkedHashMap<>();
    private LinkedHashMap<String, String> constants = new LinkedHashMap<>();

    /**
     * When set to true each value in the csv will be wrapped in quotes.
     */
    private boolean quoted = false;

    /**
     * Replaces commas with this string. If null then no replacement is done.
     */
    private String commaReplacement = " ";

    private Pattern commaReplacementPattern = Pattern.compile(commaReplacement);

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

        if (null != commaReplacementPattern) {
            value = commaReplacementPattern.matcher(value).replaceAll(commaReplacement);
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
        if (null == this.commaReplacement) {
            commaReplacementPattern = null;
        } else {
            commaReplacementPattern = Pattern.compile(this.commaReplacement);
        }
    }

    public static class Builder {
        private LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        private LinkedHashMap<String, String> constants = new LinkedHashMap<>();
        private String commaReplacement;
        private boolean quoted;

        public CsvGenerator.Builder group(final String columnHeader) {
            fields.put(GROUP, columnHeader);
            return this;
        }

        public CsvGenerator.Builder property(final String propertyName, final String columnHeader) {
            fields.put(propertyName, columnHeader);
            return this;
        }

        public CsvGenerator.Builder vertex(final String columnHeader) {
            return identifier(IdentifierType.VERTEX, columnHeader);
        }

        public CsvGenerator.Builder source(final String columnHeader) {
            return identifier(IdentifierType.SOURCE, columnHeader);
        }

        public CsvGenerator.Builder destination(final String columnHeader) {
            return identifier(IdentifierType.DESTINATION, columnHeader);
        }

        public CsvGenerator.Builder direction(final String columnHeader) {
            return identifier(IdentifierType.DIRECTED, columnHeader);
        }

        public CsvGenerator.Builder identifier(final IdentifierType identifierType, final String columnHeader) {
            fields.put(identifierType.name(), columnHeader);
            return this;
        }

        public CsvGenerator.Builder constant(final String key, final String value) {
            constants.put(key, value);
            return this;
        }

        public Builder commaReplacement(final String commaReplacement) {
            this.commaReplacement = commaReplacement;
            return this;
        }

        public Builder quoted(final boolean quoted) {
            this.quoted = quoted;
            return this;
        }

        public CsvGenerator build() {
            final CsvGenerator generator = new CsvGenerator();
            generator.setFields(fields);
            generator.setConstants(constants);
            generator.setCommaReplacement(commaReplacement);
            generator.setQuoted(quoted);

            return generator;
        }
    }
}
