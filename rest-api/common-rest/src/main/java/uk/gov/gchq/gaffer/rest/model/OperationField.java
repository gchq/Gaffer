/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.util.Set;

public class OperationField {
    private final String name;
    private final String summary;
    private final String className;
    private final Set<String> options;
    private final boolean required;

    public OperationField(final String name, final String summary, final String className, final Set<String> options, final boolean required) {
        this.name = name;
        this.summary = summary;
        this.className = className;
        this.options = options;
        this.required = required;
    }

    public String getName() {
        return name;
    }

    public String getSummary() {
        return summary;
    }

    public String getClassName() {
        return className;
    }

    public Set<String> getOptions() {
        return options;
    }

    public boolean isRequired() {
        return required;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final OperationField that = (OperationField) o;

        return new EqualsBuilder()
                .append(required, that.required)
                .append(name, that.name)
                .append(summary, that.summary)
                .append(className, that.className)
                .append(options, that.options)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(name)
                .append(summary)
                .append(className)
                .append(options)
                .append(required)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", name)
                .append("summary", summary)
                .append("className", className)
                .append("options", options)
                .append("required", required)
                .toString();
    }
}


