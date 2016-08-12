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

/**
 * A {@link Validator} that returns the inverse of the wrapped validator result.
 * @param <I> Input type
 */
public final class Not<I> implements Validator<I> {
    private Validator<I> validator;

    public Not() { }

    public Not(final Validator<I> validator) {
        setValidator(validator);
    }

    public void setValidator(final Validator<I> validator) {
        this.validator = validator;
    }

    public Validator<I> getValidator() {
        return validator;
    }

    @Override
    public Boolean execute(final I input) {
        return !validator.execute(input);
    }

    @Override
    public Not<I> copy() {
        return new Not<>((Validator) validator.copy());
    }
}
