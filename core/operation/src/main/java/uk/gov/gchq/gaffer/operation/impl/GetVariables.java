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
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.List;
import java.util.Map;

@Since("1.8.0")
@Summary("Gets all variables from the Context variable map")
@JsonPropertyOrder(value = {"variableNames", "options"}, alphabetic = true)
public class GetVariables implements Output<Map<String, Object>> {
    private List<String> variableNames;
    private Map<String, String> options;

    public List<String> getVariableNames() {
        return variableNames;
    }

    public void setVariableNames(final List<String> variableNames) {
        this.variableNames = variableNames;
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
    public TypeReference<Map<String, Object>> getOutputTypeReference() {
        return new TypeReferenceImpl.MapStringObject();
    }

    @Override
    public GetVariables shallowClone() throws CloneFailedException {
        return new GetVariables.Builder()
                .variableNames(variableNames)
                .options(options)
                .build();
    }

    public static final class Builder
            extends Operation.BaseBuilder<GetVariables, Builder>
            implements Output.Builder<GetVariables, Map<String, Object>, Builder> {

        public Builder() {
            super(new GetVariables());
        }

        public Builder variableNames(final List<String> variableNames) {
            _getOp().setVariableNames(variableNames);
            return _self();
        }
    }

}
