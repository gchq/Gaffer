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

package koryphe.function.stateless.validator.bool;

import koryphe.function.stateless.validator.Validator;

import java.util.List;

/**
 * A {@link Validator} that wraps a list of validators and returns true if all of the wrapped
 * validators return true.
 * @param <I> Input type
 */
public final class And<I> extends BooleanOperator<I> {
    public And() { }

    public And(final List<Validator<I>> validators) {
        setValidators(validators);
    }

    @Override
    public Boolean execute(final I input) {
        for (Validator<I> validator : validators) {
            if (!validator.execute(input)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public And<I> copy() {
        And<I> validator = new And();
        for (Validator<I> v : validators) {
            validator.addValidator((Validator) v.copy());
        }
        return validator;
    }
}
