/*
 * Copyright 2016 Crown Copyright
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

package koryphe.function;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A <code>FunctionContext</code> wraps a {@link Function}. The <code>FunctionContext</code> applies an {@link Adapter}
 * to input and output values so that the function can be applied in the context of different domain data models.
 * @see Adapter
 */
public abstract class FunctionContext<C, I, IA extends Adapter<C, I>, O, OA extends Adapter<C, O>, F extends Function<I, O>> {
    protected F function;
    private IA inputAdapter;
    private OA outputAdapter;

    /**
     * Default constructor - for serialisation.
     */
    public FunctionContext() { }

    /**
     * Create a <code>ValidatorContext</code> with the given function and input/output criteria.
     * @param inputAdapter Function input selection criteria.
     * @param function Function to execute.
     * @param outputAdapter Function output projection criteria.
     */
    public FunctionContext(final IA inputAdapter, final F function, final OA outputAdapter) {
        setFunction(function);
        setInputAdapter(inputAdapter);
        setOutputAdapter(outputAdapter);
    }

    /**
     * @param function Function to execute.
     */
    public void setFunction(final F function) {
        this.function = function;
    }

    /**
     * @return Function to execute.
     */
    public F getFunction() {
        return function;
    }

    /**
     * @param inputAdapter Function input selection criteria.
     */
    public void setInputAdapter(final IA inputAdapter) {
        this.inputAdapter = inputAdapter;
    }

    /**
     * @return Function input selection criteria.
     */
    public IA getInputAdapter() {
        return inputAdapter;
    }

    @JsonIgnore
    public void setInputContext(final C inputContext) {
        if (inputAdapter != null) {
            inputAdapter.setContext(inputContext);
        }
    }

    @JsonIgnore
    public I getInput(final C inputContext) {
        if (inputContext == null) {
            return null;
        } else {
            if (inputAdapter != null) {
                return inputAdapter.from(inputContext);
            } else {
                return (I) inputContext; // assume no adapter required
            }
        }
    }

    /**
     * @param outputAdapter Function output projection criteria.
     */
    public void setOutputAdapter(final OA outputAdapter) {
        this.outputAdapter = outputAdapter;
    }

    /**
     * @return Function output projection criteria.
     */
    public OA getOutputAdapter() {
        return outputAdapter;
    }

    @JsonIgnore
    public void setOutputContext(final C outputContext) {
        if (outputAdapter != null) {
            outputAdapter.setContext(outputContext);
        }
    }

    @JsonIgnore
    public O getOutput(final C outputContext) {
        if (outputContext == null) {
            return null;
        } else {
            if (outputAdapter != null) {
                return outputAdapter.from(outputContext);
            } else {
                return (O) outputContext; // assume no adapter required
            }
        }
    }

    @JsonIgnore
    public C setOutput(final O output) {
        if (outputAdapter != null) {
            return outputAdapter.to(output);
        } else {
            return (C) output; // assume no adapter required
        }
    }
}
