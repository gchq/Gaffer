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

package koryphe.function.validate.bool;

import koryphe.function.CompositeFunction;
import koryphe.function.validate.Validator;

import java.util.List;

/**
 * A composite {@link Validator} that returns true if all of it's validators return true, otherwise false.
 * @param <I> Type of input to be validated
 */
public final class And<I> extends CompositeFunction<Validator<I>> implements Validator<I> {
    public And() {
        super();
    }

    public And(final List<Validator<I>> validators) {
        super(validators);
    }

    @Override
    public Boolean execute(final I input) {
        for (Validator<I> validator : this) {
            if (!validator.execute(input)) {
                return false;
            }
        }
        return true;
    }
}
