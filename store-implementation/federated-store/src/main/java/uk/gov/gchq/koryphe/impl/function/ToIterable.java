/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.koryphe.impl.function;


import uk.gov.gchq.gaffer.commonutil.iterable.EmptyIterable;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.util.Arrays;
import java.util.Collections;

public class ToIterable extends KorypheFunction<Object, Iterable<Object>> {
    public ToIterable() {
    }

    public Iterable<Object> apply(final Object value) {
        final Iterable<Object> rtn;
        if (null == value) {
            rtn = new EmptyIterable<>();
        } else if (value instanceof Iterable) {
            //noinspection unchecked
            rtn = (Iterable<Object>) value;
        } else if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        } else {
            rtn = Collections.singletonList(value);
        }
        return rtn;
    }
}
