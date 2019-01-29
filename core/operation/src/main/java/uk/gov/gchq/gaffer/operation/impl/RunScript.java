/*
 * Copyright 2016-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code RunScript} operation runs a provided script against an input and returns the result.
 * The 'type' field allows you to configure the type of script, e.g JavaScript or Python.
 * This is disabled by default as it is not designed for public use.
 * The only script type allowed using the default handler is JavaScript. The script should have a global apply method/function
 * that takes a single argument and returns the result - i.e it implements Java's Function interface.
 * e.g:
 * <pre>
 *     function apply(input) { return input + 1; };
 * </pre>
 *
 * @see RunScript.Builder
 */
@JsonPropertyOrder(value = {"class", "input", "script", "type"}, alphabetic = true)
@Since("1.8.0")
@Summary("Runs a provided script")
public class RunScript<I, O> implements InputOutput<I, O> {

    @Required
    private String script;

    @Required
    private String type;

    private I input;
    private Map<String, String> options;

    public RunScript() {
    }

    public RunScript(final String script, final String type) {
        this.script = script;
        this.type = type;
    }

    @Override
    public I getInput() {
        return input;
    }

    @Override
    public void setInput(final I input) {
        this.input = input;
    }

    @Override
    public TypeReference<O> getOutputTypeReference() {
        return TypeReferenceImpl.createExplicitT();
    }

    @Override
    public RunScript<I, O> shallowClone() {
        return new RunScript.Builder<I, O>()
                .script(script)
                .input(input)
                .type(type)
                .options(options)
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

    public String getScript() {
        return script;
    }

    public void setScript(final String script) {
        this.script = script;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public static final class Builder<I, O>
            extends BaseBuilder<RunScript<I, O>, Builder<I, O>>
            implements InputOutput.Builder<RunScript<I, O>, I, O, Builder<I, O>> {
        public Builder() {
            super(new RunScript<>());
        }

        public Builder<I, O> script(final String script) {
            _getOp().setScript(script);
            return _self();
        }

        public Builder<I, O> type(final String type) {
            _getOp().setType(type);
            return _self();
        }
    }
}
