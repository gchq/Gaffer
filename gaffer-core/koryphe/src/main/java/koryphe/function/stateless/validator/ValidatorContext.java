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

import koryphe.function.Adapter;
import koryphe.function.FunctionContext;

public class ValidatorContext<C, I, IA extends Adapter<C, I>> extends FunctionContext<C, I, IA, Boolean, Adapter<C, Boolean>, Validator<I>> implements Validator<C> {
    /**
     * Default constructor - for serialisation.
     */
    public ValidatorContext() { }

    @Override
    public Boolean execute(final C input) {
        setInputContext(input);
        return function.execute(getInput(input));
    }
}
