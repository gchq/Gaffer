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
    public Operation shallowClone() throws CloneFailedException {
        return null;
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

    //todo write builder, fill in shallowClone, override hashcode, equals, and toString

    public static final class Builder extends Operation.BaseBuilder<If, Builder>
        implements InputOutput.Builder<If, Object, Object, Builder> {
        public Builder() {
            super(new If());
        }

        public Builder condition(final boolean condition) {
            _getOp().condition = condition;
            return _self();
        }

//        public Builder

}
