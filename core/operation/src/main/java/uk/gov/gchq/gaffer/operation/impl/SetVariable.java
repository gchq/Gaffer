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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

@Since("1.8.0")
@Summary("Sets a variable in the Context")
@JsonPropertyOrder(value = {"input", "variableName", "options"}, alphabetic = true)
public class SetVariable implements Input<Object> {
    private Object input;
    private String variableName;
    private Map<String, String> options;

    @Override
    public Object getInput() {
        return input;
    }

    @Override
    public void setInput(final Object input) {
        this.input = input;
    }

    public String getVariableName() {
        return variableName;
    }

    public void setVariableName(final String variableName) {
        this.variableName = variableName;
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
    public SetVariable shallowClone() throws CloneFailedException {
        return new SetVariable.Builder()
                .input(input)
                .variableName(variableName)
                .options(options)
                .build();
    }

    public static final class Builder
            extends Operation.BaseBuilder<SetVariable, Builder>
            implements Input.Builder<SetVariable, Object, Builder> {
        public Builder() {
            super(new SetVariable());
        }

        public Builder variableName(final String variableName) {
            _getOp().setVariableName(variableName);
            return _self();
        }
    }
}
