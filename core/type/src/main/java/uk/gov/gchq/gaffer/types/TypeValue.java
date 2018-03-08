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

package uk.gov.gchq.gaffer.types;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.koryphe.serialisation.json.JsonSimpleClassName;

import java.io.Serializable;
import java.util.Comparator;

/**
 * A {@code TypeValue} is used to store information relating to types and associated
 * values.
 */
@JsonSimpleClassName
public class TypeValue implements Comparable<TypeValue>, Serializable {

    private static Comparator<String> stringComparator = Comparator
            .nullsFirst(String::compareTo);

    private String type;
    private String value;

    public TypeValue() {
        // Empty
    }

    public TypeValue(final String type, final String value) {
        this.type = type;
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final TypeValue typeValue = (TypeValue) obj;

        return new EqualsBuilder()
                .append(type, typeValue.type)
                .append(value, typeValue.value)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 89)
                .append(type)
                .append(value)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("type", type)
                .append("value", value)
                .toString();
    }

    @Override
    public int compareTo(final TypeValue typeValue) {
        if (null == typeValue) {
            return 1;
        }
        int i = stringComparator.compare(type, typeValue.getType());
        if (i != 0) {
            return i;
        }
        return stringComparator.compare(value, typeValue.getValue());
    }
}
