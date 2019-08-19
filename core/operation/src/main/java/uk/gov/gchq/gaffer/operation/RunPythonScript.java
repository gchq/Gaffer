/*
 * Copyright 2019 Crown Copyright
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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.util.Map;

public class RunPythonScript<I_ITEM, O> implements
        InputOutput<Iterable<? extends I_ITEM>, O>,
        MultiInput<I_ITEM>,
        Operation {

    private Iterable<? extends I_ITEM> input;
    private Map<String, String> options;
    private String scriptName;
    private Map<String, Object> parameters;
    private String repoName;
    private String repoURI;

    @Override
    public Iterable<? extends I_ITEM> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends I_ITEM> input) {
        this.input = input;
    }

    @Override
    public TypeReference<O> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.Object();
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new RunPythonScript.Builder<>().name(scriptName).parameters(parameters).repoName(repoName).repoURI(repoURI).build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    private void setScriptName(final String scriptName) {
        this.scriptName = scriptName;
    }

    public String getScriptName() {
        return scriptName;
    }

    public Map<String, Object> getParameters() { return parameters; }

    public void setParameters(final Map<String, Object> parameters) { this.parameters = parameters; }

    public String getRepoName() {
        return repoName;
    }

    public void setRepoName(String repoName) {
        this.repoName = repoName;
    }

    public String getRepoURI() {
        return repoURI;
    }

    public void setRepoURI(String repoURI) {
        this.repoURI = repoURI;
    }

    public static class Builder<I_ITEM, O> extends BaseBuilder<RunPythonScript<I_ITEM, O>, Builder<I_ITEM, O>>
            implements InputOutput.Builder<RunPythonScript<I_ITEM, O>, Iterable<? extends I_ITEM>, O, Builder<I_ITEM, O>>,
            MultiInput.Builder<RunPythonScript<I_ITEM, O>, I_ITEM, Builder<I_ITEM, O>> {
        public Builder() {
            super(new RunPythonScript<>());
        }

        public Builder<I_ITEM, O> name(final String scriptName) {
            _getOp().setScriptName(scriptName);
            return _self();
        }

        public Builder<I_ITEM, O> parameters(final Map<String, Object> parameters) {
            _getOp().setParameters(parameters);
            return _self();
        }

        public Builder<I_ITEM, O> repoName(final String repoName) {
            _getOp().setRepoName(repoName);
            return _self();
        }

        public Builder<I_ITEM, O> repoURI(final String repoURI) {
            _getOp().setRepoURI(repoURI);
            return _self();
        }
    }
}
