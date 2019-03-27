/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.types.function;

import uk.gov.gchq.gaffer.types.TypeValue;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.tuple.function.KorypheFunction2;

/**
 * A {@code StringsToTypeSubTypeValue} is a {@link KorypheFunction2} that converts 2 strings: type and value
 * into a {@link TypeValue}.
 */
@Since("1.8.0")
@Summary("Converts 2 strings into a TypeValue")
public class StringsToTypeValue extends KorypheFunction2<String, String, TypeValue> {
    @Override
    public TypeValue apply(final String type, final String value) {
        return new TypeValue(type, value);
    }
}
