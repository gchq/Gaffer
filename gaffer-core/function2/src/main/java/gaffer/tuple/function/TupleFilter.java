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

package gaffer.tuple.function;

import gaffer.function2.Validator;
import gaffer.tuple.Tuple;
import gaffer.tuple.function.context.FunctionContext;
import gaffer.tuple.function.context.TupleFunctionValidator;

import java.util.ArrayList;
import java.util.List;

/**
 * A <code>TupleFilter</code> validates input {@link gaffer.tuple.Tuple}s by applying {@link gaffer.function2.Validator}s
 * to the tuple values. It calculates the logical AND of the function results, and if true, the input tuple is returned,
 * otherwise <code>null</code>.
 * @param <R> The type of reference used by tuples.
 */
public class TupleFilter<R> extends StatelessTupleFunction<R> {
    private List<FunctionContext<Validator, R>> validators;

    /**
     * Default constructor - for serialisation.
     */
    public TupleFilter() { }

    /**
     * Create a <code>TupleFilter</code> that applies the given functions.
     * @param validators {@link gaffer.function2.Validator}s to validate tuple values.
     */
    public TupleFilter(final List<FunctionContext<Validator, R>> validators) {
        setValidators(validators);
    }

    /**
     * @param validators {@link gaffer.function2.Validator}s to validate tuple values.
     */
    public void setValidators(final List<FunctionContext<Validator, R>> validators) {
        this.validators = validators;
    }

    /**
     * @return {@link gaffer.function2.Validator}s to validate tuple values.
     */
    public List<FunctionContext<Validator, R>> getValidators() {
        return validators;
    }

    /**
     * @param validator {@link gaffer.function2.Validator} to validate tuple values.
     */
    public void addValidator(final FunctionContext<Validator, R> validator) {
        if (validators == null) {
            validators = new ArrayList<FunctionContext<Validator, R>>();
        }
        validators.add(validator);
    }

    /**
     * Validate an input tuple.
     * @param input Input value
     * @return true if all {@link gaffer.function2.Validator}s are successful, otherwise false.
     */
    public boolean validate(final Tuple<R> input) {
        if (validators != null) {
            for (FunctionContext<Validator, R> validator : validators) {
                if (!(validator.getFunction().validate(validator.select(input)))) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean validateInput(final Object schemaTuple) {
        return TupleFunctionValidator.validateInput(validators, schemaTuple);
    }

    @Override
    public boolean validateOutput(final Object schemaTuple) {
        return TupleFunctionValidator.validateOutput(validators, schemaTuple);
    }

    @Override
    public Tuple<R> execute(final Tuple<R> input) {
        return validate(input) ? input : null;
    }

    /**
     * @return New <code>TupleFilter</code> with new {@link gaffer.function2.Validator}s.
     */
    public TupleFilter<R> copy() {
        TupleFilter<R> copy = new TupleFilter<R>();
        for (FunctionContext<Validator, R> validator : this.validators) {
            copy.addValidator(validator.copy());
        }
        return copy;
    }
}
