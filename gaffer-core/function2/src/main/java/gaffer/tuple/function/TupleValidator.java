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
import gaffer.tuple.view.TupleView;

import java.util.ArrayList;
import java.util.List;

/**
 * A <code>TupleValidator</code> validates input {@link gaffer.tuple.Tuple}s by applying
 * {@link gaffer.function2.Validator}s to the tuple values. Returns the logical AND of the function results.
 * @param <R> The type of reference used by tuples.
 */
public class TupleValidator<R> extends Validator<Tuple<R>> {
    private List<FunctionContext<Validator, R>> validators;

    /**
     * Default constructor - for serialisation.
     */
    public TupleValidator() { }

    /**
     * Create a <code>TupleValidator</code> that applies the given functions.
     * @param validators {@link gaffer.function2.Validator}s to validate tuple values.
     */
    public TupleValidator(final List<FunctionContext<Validator, R>> validators) {
        setValidators(validators);
    }

    /**
     * @param validators {@link gaffer.function2.Validator}s to validate tuple values.
     */
    public void setValidators(final List<FunctionContext<Validator, R>> validators) {
        this.validators = validators;
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
     * @param selection Validator input selection criteria.
     * @param validator Validator function.
     */
    public void addValidator(final TupleView<R> selection, final Validator validator) {
        FunctionContext<Validator, R> context = new FunctionContext<Validator, R>(selection, validator, null);
        addValidator(context);
    }

    /**
     * Validate an input tuple.
     * @param input Input value
     * @return true if all {@link gaffer.function2.Validator}s are successful, otherwise false.
     */
    public boolean validate(final Tuple<R> input) {
        if (validators != null) {
            for (FunctionContext<Validator, R> validator : validators) {
                boolean valid = validator.getFunction().validate(validator.selectFrom(input));
                if (!valid) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * @return New <code>TupleValidator</code> with new {@link gaffer.function2.Validator}s.
     */
    public TupleValidator<R> copy() {
        TupleValidator<R> copy = new TupleValidator<R>();
        for (FunctionContext<Validator, R> validator : this.validators) {
            copy.addValidator(validator.copy());
        }
        return copy;
    }
}
