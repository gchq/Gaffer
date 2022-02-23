/*
 * Copyright 2021-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
@JsonPropertyOrder(value = {"class", "handlerClass"}, alphabetic = true)
public class FieldDeclaration {
    //TODO FS modify for iterables contents
    private TreeMap<String, Class> fields = new TreeMap<>(String::compareToIgnoreCase);
    private TreeSet<String> optionalFields = new TreeSet<>(String::compareToIgnoreCase);

    @JsonCreator
    public FieldDeclaration(@JsonProperty("fields") final TreeMap<String, Class> fields) {
        this.fields = fields;
    }

    public FieldDeclaration() {
    }

    public TreeMap<String, Class> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("fields", fields)
                .build();
    }

    public FieldDeclaration field(final String field, final Class valueClass) {
        return fieldRequired(field, valueClass);
    }

    public FieldDeclaration fieldOptional(Map.Entry<String, Class> entry) {
        return fieldOptional(entry.getKey(), entry.getValue());
    }

    public FieldDeclaration fieldOptional(final String field, final Class valueClass) {
        fieldRequired(field, valueClass);
        optionalFields.add(field);
        return this;
    }

    public FieldDeclaration fieldRequired(Map.Entry<String, Class> entry) {
        return fieldRequired(entry.getKey(), entry.getValue());
    }

    public FieldDeclaration fieldRequired(final String field, final Class valueClass) {
        fields.put(field, valueClass);
        optionalFields.remove(field);
        return this;
    }

    public boolean optionalContains(final String field) {
        return optionalFields.contains(field);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final FieldDeclaration that = (FieldDeclaration) o;

        return new EqualsBuilder()
                .append(fields, that.fields)
                .append(optionalFields, that.optionalFields)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(fields)
                .append(optionalFields)
                .toHashCode();
    }
}
