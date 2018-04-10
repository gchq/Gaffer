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
 * A {@code TypeSubTypeValue} is used to store information relating to types,
 * sub-types and associated values.
 */
@JsonSimpleClassName
public class TypeSubTypeValue implements Comparable<TypeSubTypeValue>, Serializable {

    private static Comparator<String> stringComparator = Comparator
            .nullsFirst(String::compareTo);

    private String type;
    private String subType;
    private String value;

    public TypeSubTypeValue() {

    }

    public TypeSubTypeValue(final String type, final String subType, final String value) {
        this.type = type;
        this.subType = subType;
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

    public void setSubType(final String subType) {
        this.subType = subType;
    }

    public String getSubType() {
        return subType;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 89)
                .append(type)
                .append(subType)
                .append(value)
                .toHashCode();
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }

        if (null == object || getClass() != object.getClass()) {
            return false;
        }

        final TypeSubTypeValue tstv = (TypeSubTypeValue) object;

        return new EqualsBuilder()
                .append(type, tstv.type)
                .append(subType, tstv.subType)
                .append(value, tstv.value)
                .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("type", type)
                .append("subType", subType)
                .append("value", value)
                .toString();
    }

    @Override
    public int compareTo(final TypeSubTypeValue typeSubTypeValue) {
        if (null == typeSubTypeValue) {
            return 1;
        }
        int i = stringComparator.compare(type, typeSubTypeValue.getType());
        if (i != 0) {
            return i;
        }
        i = stringComparator.compare(subType, typeSubTypeValue.getSubType());
        if (i != 0) {
            return i;
        }
        return stringComparator.compare(value, typeSubTypeValue.getValue());
    }
}
