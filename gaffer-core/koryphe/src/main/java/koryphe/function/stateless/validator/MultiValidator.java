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

package koryphe.function.stateless.validator;

import koryphe.function.MultiFunction;

/**
 * A {@link Validator} that applies a list of validators.
 * @param <I> Input type
 */
public final class MultiValidator<I> extends MultiFunction<Validator<I>> implements Validator<I> {
    @Override
    public Boolean execute(final I input) {
        for (Validator<I> validator : functions) {
            if (!validator.execute(input)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public MultiValidator copy() {
        MultiValidator mv = new MultiValidator();
        for (Validator<I> v : functions) {
            mv.addFunction(v.copy());
        }
        return mv;
    }
}
