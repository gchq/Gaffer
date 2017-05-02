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

import org.apache.commons.lang3.StringUtils;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import java.util.LinkedList;
import java.util.List;
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
    private List<String> fields = new LinkedList<>();
    private List<String> constants = new LinkedList<>();

    /**
     * When set to true each value in the csv will be wrapped in quotes.
     */
    private boolean quoted = false;

    /**
     * Replaces commas with this string. If null then no replacement is done.
     */
    private String commaReplacement = " ";

    private Pattern commaReplacementPattern = Pattern.compile(commaReplacement);

    @Override
    public String _apply(final Element element) {
        final StringBuilder strBuilder = new StringBuilder();
        for (final String field : fields) {
            final Object value = getFieldValue(element, field);
            if (null != value) {
                strBuilder.append(quoteString(value));
                strBuilder.append(COMMA);
            }
        }

        if (!constants.isEmpty()) {
            for (final String constant : constants) {
                strBuilder.append(quoteString(constant));
                strBuilder.append(COMMA);
            }
        }

        if (strBuilder.length() < 1) {
            return "";
        }

        return strBuilder.substring(0, strBuilder.length() - 1);
    }

    public String getHeader() {
        if (fields.isEmpty()) {
            if (constants.isEmpty()) {
                return "";
            }
            return getHeaderFields(constants);
        }

        if (constants.isEmpty()) {
            return getHeaderFields(fields);
        }

        return getHeaderFields(fields) + COMMA + getHeaderFields(constants);
    }

    private String getHeaderFields(final List<String> fields) {
        return StringUtils.join(fields.stream().map(s -> quoteString(s)).toArray(), COMMA);
    }

    private String quoteString(final Object s) {
        String value = s.toString();

        if (null == value) {
            value = "";
        } else if (null != commaReplacementPattern) {
            value = commaReplacementPattern.matcher(value).replaceAll(commaReplacement);
        }

        if (quoted) {
            value = "\"" + value + "\"";
        }

        return value;
    }

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

    public List<String> getFields() {
        return fields;
    }

    public void setFields(final List<String> fields) {
        if (null == fields) {
            this.fields = new LinkedList<>();
        }
        this.fields = fields;
    }

    public List<String> getConstants() {
        return constants;
    }

    public void setConstants(final List<String> constants) {
        if (null == constants) {
            this.constants = new LinkedList<>();
        } else {
            for (final String constant : constants) {
                if (null != constant) {
                    constants.add(constant.replaceAll("\"", "\'"));
                }
            }
        }
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
        private List<String> fields = new LinkedList<>();
        private List<String> constants = new LinkedList<>();
        private String commaReplacement;
        private boolean quoted;

        public Builder group() {
            fields.add(GROUP);
            return this;
        }

        public Builder property(final String... propertyNames) {
            for (final String propertyName : propertyNames) {
                if (null != propertyName) {
                    fields.add(propertyName.replaceAll("\"", "\'"));
                }
            }
            return this;
        }

        public Builder vertex() {
            fields.add(IdentifierType.VERTEX.name());
            return this;
        }

        public Builder source() {
            fields.add(IdentifierType.SOURCE.name());
            return this;
        }

        public Builder destination() {
            fields.add(IdentifierType.DESTINATION.name());
            return this;
        }

        public Builder direction() {
            fields.add(IdentifierType.DIRECTED.name());
            return this;
        }

        public Builder identifier(final IdentifierType... identifierTypes) {
            for (IdentifierType identifierType : identifierTypes) {
                if (null != identifierType) {
                    fields.add(identifierType.name());
                }
            }
            return this;
        }

        public Builder constant(final String value) {
            if (null != value) {
                constants.add(value.replaceAll("\"", "\'"));
            }
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
