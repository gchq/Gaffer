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
package uk.gov.gchq.gaffer.operation.util;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import uk.gov.gchq.gaffer.operation.Operation;

import java.util.function.Predicate;

/**
 * A {@code Conditional} is a simple POJO for wrapping an {@link Operation} and a {@link Predicate},
 * to allow for pre-predicate transformations, whilst preserving the input data.
 */
@JsonPropertyOrder(value = {"transform", "predicate"}, alphabetic = true)
public class Conditional {
    private Predicate predicate;
    private Operation transform;

    public Conditional() {
    }

    public Conditional(final Predicate predicate) {
        this.predicate = predicate;
    }

    public Conditional(final Predicate predicate, final Operation transform) {
        this.predicate = predicate;
        this.transform = transform;
    }

    public Conditional shallowClone() {
        final Operation transformClone = null != transform ? transform.shallowClone() : null;
        return new Conditional(predicate, transformClone);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Operation getTransform() {
        return transform;
    }

    public void setTransform(final Operation transform) {
        this.transform = transform;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Predicate getPredicate() {
        return predicate;
    }

    public void setPredicate(final Predicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final Conditional conditional = (Conditional) obj;

        return new EqualsBuilder()
                .append(transform, conditional.transform)
                .append(predicate, conditional.predicate)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 83)
                .append(transform)
                .append(predicate)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append(transform)
                .append(predicate)
                .toString();
    }
}
