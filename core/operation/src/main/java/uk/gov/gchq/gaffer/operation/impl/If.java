/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

public class If implements InputOutput<Object, Object>, Operations {

    private Object input;
    private Map<String, String> options;
    private Boolean condition;
    private Operation then;
    private Operation otherwise;
    private Predicate<Object> predicate;

    @Override
    public Object getInput() {
        return input;
    }

    @Override
    public void setInput(final Object input) {
        this.input = input;
    }

    @Override
    public TypeReference<Object> getOutputTypeReference() {
        return new TypeReferenceImpl.Object();
    }

    @Override
    public If shallowClone() throws CloneFailedException {
        return new If.Builder()
                .condition(condition)
                .predicate(predicate)
                .then(then)
                .otherwise(otherwise)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public Collection getOperations() {
        return null;
    }

    public Boolean getCondition() {
        return condition;
    }

    public void setCondition(final boolean condition) {
        this.condition = condition;
    }

    public Operation getThen() {
        return then;
    }

    public void setThen(final Operation then) {
        this.then = then;
    }

    public Operation getOtherwise() {
        return otherwise;
    }

    public void setOtherwise(final Operation otherwise) {
        this.otherwise = otherwise;
    }

    public Predicate<Object> getPredicate() {
        return predicate;
    }

    public void setPredicate(final Predicate<Object> predicate) {
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

        final If filter = (If) obj;

        return new EqualsBuilder()
                .append(condition, filter.condition)
                .append(predicate, filter.predicate)
                .append(then, filter.then)
                .append(otherwise, filter.otherwise)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 83)
                .append(condition)
                .append(predicate)
                .append(then)
                .append(otherwise)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append(condition)
                .append(predicate)
                .append(then)
                .append(otherwise)
                .toString();
    }

    public static final class Builder extends Operation.BaseBuilder<If, Builder>
        implements InputOutput.Builder<If, Object, Object, Builder> {
        public Builder() {
            super(new If());
        }

        public Builder condition(final boolean condition) {
            _getOp().condition = condition;
            return _self();
        }

        public Builder predicate(final Predicate<Object> predicate) {
            _getOp().predicate = predicate;
            return _self();
        }

        public Builder then(final Operation op) {
            _getOp().then = op;
            return _self();
        }

        public Builder otherwise(final Operation op) {
            _getOp().otherwise = op;
            return _self();
        }
    }
}
