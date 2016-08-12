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

/**
 * A <code>ValidatorContext</code> wraps a {@link Function}. The <code>ValidatorContext</code> applies an {@link Adapter}
 * to input and output values so that the function can be applied in the context of different domain data models.
 * @see Adapter
 */
public abstract class FunctionContext<I, FI, FO, O, F extends Function<FI, FO>> {
    protected F function;
    private Adapter<I, FI> inputAdapter;
    private Adapter<O, FO> outputAdapter;

    /**
     * Default constructor - for serialisation.
     */
    public FunctionContext() { }

    /**
     * Create a <code>ValidatorContext</code> with the given function and input/output criteria.
     * @param selection Function input selection criteria.
     * @param function Function to execute.
     * @param projection Function output projection criteria.
     */
    public FunctionContext(final Adapter<I, FI> selection, final F function, final Adapter<O, FO> projection) {
        setFunction(function);
        setInputAdapter(selection);
        setOutputAdapter(projection);
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
    public void setInputAdapter(final Adapter<I, FI> inputAdapter) {
        this.inputAdapter = inputAdapter;
    }

    /**
     * @return Function input selection criteria.
     */
    public Adapter<I, FI> getInputAdapter() {
        return inputAdapter;
    }

    protected FI adaptFromInput(final I input) {
        if (inputAdapter == null) {
            return (FI) input;
        } else {
            return inputAdapter.from(input);
        }
    }

    protected I adaptToInput(final FI functionInput) {
        if (inputAdapter == null) {
            return (I) functionInput;
        } else {
            return inputAdapter.to(functionInput);
        }
    }

    public void setInputContext(final I inputContext) {
        if (inputAdapter != null) {
            inputAdapter.setContext(inputContext);
        }
    }

    /**
     * @param outputAdapter Function output projection criteria.
     */
    public void setOutputAdapter(final Adapter<O, FO> outputAdapter) {
        this.outputAdapter = outputAdapter;
    }

    /**
     * @return Function output projection criteria.
     */
    public Adapter<O, FO> getOutputAdapter() {
        return outputAdapter;
    }

    protected O adaptToOutput(final FO functionOutput) {
        if (outputAdapter == null) {
            return (O) functionOutput;
        } else {
            return outputAdapter.to(functionOutput);
        }
    }

    protected FO adaptFromOutput(final O output) {
        if (outputAdapter == null) {
            return (FO) output;
        } else {
            return outputAdapter.from(output);
        }
    }

    public void setOutputContext(final O outputContext) {
        if (outputAdapter != null) {
            outputAdapter.setContext(outputContext);
        }
    }

    protected <FC extends FunctionContext<I, FI, FO, O, F>> FC copyInto(final FC copy) {
        if (function != null) {
            copy.setFunction((F) function.copy());
        } else {
            copy.setFunction(null);
        }

        if (inputAdapter != null) {
            copy.setInputAdapter(inputAdapter.copy());
        } else {
            copy.setInputAdapter(null);
        }

        if (outputAdapter != null) {
            copy.setOutputAdapter(outputAdapter.copy());
        } else {
            copy.setOutputAdapter(null);
        }

        return copy;
    }
}
