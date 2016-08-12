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

import java.util.ArrayList;
import java.util.List;

public abstract class BooleanOperator<I> implements Validator<I> {
    protected List<Validator<I>> validators = new ArrayList<>();

    public void setValidators(final List<Validator<I>> validators) {
        this.validators.clear();
        this.validators.addAll(validators);
    }

    public void addValidator(final Validator<I> validator) {
        validators.add(validator);
    }

    public List<Validator<I>> getValidators() {
        return validators;
    }
}
