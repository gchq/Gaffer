/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.Serializable;

@JsonDeserialize(builder = VariableDetail.Builder.class)
public class VariableDetail implements Serializable {
    private static final long serialVersionUID = -883113279873631469L;
    private String description;
    private Object value;
    private Class valueClass;

    public VariableDetail(final String description, final Class clazz, final Object value) {
        if (null == clazz) {
            throw new IllegalArgumentException("class must not be empty");
        }

        this.description = description;
        this.value = value;
        this.valueClass = clazz;

        try {
            byte[] json = JSONSerialiser.serialise(value);
            JSONSerialiser.deserialise(json, this.valueClass);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getDescription() {
        return description;
    }

    public Object getValue() {
        return value;
    }

    public Class getValueClass() {
        return valueClass;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final VariableDetail vd = (VariableDetail) obj;

        return new EqualsBuilder()
                .append(value, vd.value)
                .append(description, vd.description)
                .append(valueClass, vd.valueClass)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(71, 5)
                .append(value)
                .append(description)
                .append(valueClass)
                .hashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("description", description)
                .append("valueClass", valueClass)
                .append("value", value)
                .toString();
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        private String description;
        private Object value;
        private Class valueClass;

        public Builder value(final Object value) {
            this.value = value;
            return this;
        }

        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        public Builder valueClass(final Class clazz) {
            this.valueClass = clazz;
            return this;
        }

        public VariableDetail build() {
            return new VariableDetail(description, valueClass, value);
        }
    }
}
