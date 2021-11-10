/*
 * Copyright 2021 Crown Copyright
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
package uk.gov.gchq.gaffer.data.element.function;

import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

/**
 * A {@code TypeSubTypeValueToTuple} is a {@link KorypheFunction} that converts an {@link TypeSubTypeValue} into a {@link TypeSubTypeValueTuple}.
 */
@Since("1.19.0")
@Summary("Converts an TypeSubTypeValue into a Tuple")
public class TypeSubTypeValueToTuple extends KorypheFunction<TypeSubTypeValue, TypeSubTypeValueTuple> {
    @Override
    public TypeSubTypeValueTuple apply(final TypeSubTypeValue input) {
        if (null == input) {
            return null;
        }
        return new TypeSubTypeValueTuple(input);
    }
}
