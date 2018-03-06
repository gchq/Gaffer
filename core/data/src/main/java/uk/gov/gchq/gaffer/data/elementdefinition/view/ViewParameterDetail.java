/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.data.elementdefinition.view;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.Serializable;

/**
 * Simple POJO providing parameter details for {@link NamedView}s.
 */
@JsonDeserialize(builder = ViewParameterDetail.Builder.class)
public class ViewParameterDetail implements Serializable {
    private static final long serialVersionUID = -2732125973172603774L;
    private String description;
    private Object defaultValue;
    private Class valueClass;
    private boolean required;

    public ViewParameterDetail(final String description, final Class clazz, final boolean required, final Object defaultValue) {
        if (null == description) {
            throw new IllegalArgumentException("description must not be empty");
        }
        if (null == clazz) {
            throw new IllegalArgumentException("class must not be empty");
        }
        if (required && null != defaultValue) {
            throw new IllegalArgumentException("required is true but a default value has been provided");
        }

        this.description = description;
        this.required = required;
        this.defaultValue = defaultValue;
        this.valueClass = clazz;

        try {
            byte[] json = JSONSerialiser.serialise(defaultValue);
            JSONSerialiser.deserialise(json, this.valueClass);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getDescription() {
        return description;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public boolean isRequired() {
        return required;
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

        final ViewParameterDetail vpd = (ViewParameterDetail) obj;

        return new EqualsBuilder()
                .append(defaultValue, vpd.defaultValue)
                .append(required, vpd.required)
                .append(description, vpd.description)
                .append(valueClass, vpd.valueClass)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(71, 5)
                .append(defaultValue)
                .append(required)
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
                .append("required", required)
                .append("defaultValue", defaultValue)
                .toString();
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        private String description;
        private Object defaultValue;
        private boolean required = false;
        private Class valueClass;

        public Builder defaultValue(final Object defaultValue) {
            this.defaultValue = defaultValue;
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

        public Builder required(final boolean required) {
            this.required = required;
            return this;
        }

        public ViewParameterDetail build() {
            return new ViewParameterDetail(description, valueClass, required, defaultValue);
        }
    }
}
