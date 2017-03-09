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

package uk.gov.gchq.koryphe.function.predicate;


import java.util.function.Predicate;

/**
 * A {@link Predicate} that returns the inverse of the wrapped validator.
 *
 * @param <I> Type of input to be validated
 */
public final class Not<I> implements Predicate<I> {
    private Predicate<I> validator;

    public Not() {
    }

    public Not(final Predicate<I> validator) {
        setPredicate(validator);
    }

    public void setPredicate(final Predicate<I> validator) {
        this.validator = validator;
    }

    public Predicate<I> getPredicate() {
        return validator;
    }

    @Override
    public boolean test(final I input) {
        return !validator.test(input);
    }
}
