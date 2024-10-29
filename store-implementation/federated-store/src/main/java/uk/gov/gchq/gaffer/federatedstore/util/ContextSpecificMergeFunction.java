/*
 * Copyright 2022-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.util;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;

import java.util.HashMap;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * a function which is configurable based on the operation.
 *
 * @param <T> – the type of the first argument to the function
 * @param <U> – the type of the second argument to the function
 * @param <R> – the type of the result of the function
 * @see BiFunction
 * @deprecated Merging will be overhauled in 2.4.0.
 */
@Deprecated
public interface ContextSpecificMergeFunction<T, U, R> extends BiFunction<T, U, R> {
    ContextSpecificMergeFunction<T, U, R> createFunctionWithContext(final HashMap<String, Object> context) throws GafferCheckedException;

    default boolean isRequired(final String name) {
        return getRequiredContextValues().contains(name);
    }

    Set<String> getRequiredContextValues();
}
