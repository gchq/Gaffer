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

package gaffer.function2;

/**
 * A <code>Validator</code> {@link gaffer.function2.StatelessFunction} tests an input value
 * and outputs a <code>boolean</code> result.
 * @param <I> Function input type
 */
public abstract class Validator<I> extends StatelessFunction<I, Boolean> {
    public Boolean execute(final I input) {
        return validate(input);
    }

    /**
     * Validate an input value.
     * @param input Input value
     * @return <code>True</code> if input value is valid, otherwise <code>False</code>
     */
    public abstract boolean validate(I input);

    /**
     * @return New <code>Validator</code> of the same type.
     */
    public abstract Validator<I> copy();
}
