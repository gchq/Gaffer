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

package uk.gov.gchq.gaffer.federatedstore.util;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.koryphe.impl.function.IterableConcat;
import uk.gov.gchq.koryphe.impl.function.ToList;

import java.util.Collections;
import java.util.function.BiFunction;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class DefaultBestEffortsMergeFunction implements BiFunction<Object, Iterable<Object>, Iterable<Object>> {

    @Override
    public Iterable<Object> apply(final Object o, final Iterable<Object> objects) {
        final Iterable<Object> oAsNonNullIterable = isNull(o) ? Collections.emptyList() : (Iterable<Object>) new ToList().apply(o);
        return isNull(objects)
                ? oAsNonNullIterable
                : new IterableConcat<>().apply(Lists.newArrayList(oAsNonNullIterable, objects));
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 47)
                .append(super.hashCode())
                .toHashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return nonNull(obj) && this.getClass().equals(obj.getClass());
    }
}
