/*
 * Copyright 2016 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.util.Comparator;

public class TypeSubTypeValue implements Comparable<TypeSubTypeValue> {

    private static Comparator<String> stringComparator = Comparator
            .nullsFirst(String::compareTo);

    private static final int PRIME = 31;
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
        int result = 1;
        result = PRIME * result + ((value == null) ? 0 : value.hashCode());
        result = PRIME * result + ((type == null) ? 0 : type.hashCode());
        result = PRIME * result + ((subType == null) ? 0 : subType.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object object) {
        if (TypeSubTypeValue.class.isInstance(object)) {
            return this.equals((TypeSubTypeValue) object);
        } else {
            return false;
        }
    }

    public boolean equals(final TypeSubTypeValue typeSubTypeValue) {
        if (this.type == null) {
            if (null != typeSubTypeValue.getType()) {
                return false;
            }
        } else if (!this.type.equals(typeSubTypeValue.getType())) {
            return false;
        }
        if (this.subType == null) {
            if (null != typeSubTypeValue.getSubType()) {
                return false;
            }
        } else if (!this.subType.equals(typeSubTypeValue.getSubType())) {
            return false;
        }
        if (this.value == null) {
            if (null != typeSubTypeValue.getValue()) {
                return false;
            }
        } else if (!this.value.equals(typeSubTypeValue.getValue())) {
            return false;
        }
        return true;
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
        if (typeSubTypeValue == null) {
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
