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

public class TypeValue {

    private static final int PRIME = 31;
    private String type;
    private String value;

    public TypeValue() {

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
    public int hashCode() {
        int result = 1;
        result = PRIME * result + ((value == null) ? 0 : value.hashCode());
        result = PRIME * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object object) {
        if (TypeValue.class.isInstance(object)) {
            return this.equals((TypeValue) object);
        } else {
            return false;
        }
    }

    public boolean equals(final TypeValue typeValue) {
        if (this.type == null) {
            if (null != typeValue.getType()) {
                return false;
            }
        } else if (!this.type.equals(typeValue.getType())) {
            return false;
        }
        if (this.value == null) {
            if (null != typeValue.getValue()) {
                return false;
            }
        } else if (!this.value.equals(typeValue.getValue())) {
            return false;
        }
        return true;
    }
}
