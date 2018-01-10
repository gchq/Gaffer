/*
 * Copyright 2018 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.util;

import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameCache;
import uk.gov.gchq.koryphe.util.IterableUtil;

import java.util.Arrays;
import java.util.stream.Collectors;

public final class OperationUtil {
    private OperationUtil() {
    }

    public static Iterable<? extends ElementId> toElementIds(final Object... input) {
        if (null == input) {
            return null;
        }
        return (Iterable) Arrays.stream(input)
                .map(i -> i instanceof ElementId ? i : new EntitySeed(i))
                .collect(Collectors.toList());
    }

    public static Iterable<? extends ElementId> toElementIds(final Iterable<?> input) {
        if (null == input) {
            return null;
        }
        return IterableUtil.map(input, i -> i instanceof ElementId ? i : new EntitySeed(i));
    }

    public static Iterable<?> fromElementIds(final Iterable<? extends ElementId> input) {
        if (null == input) {
            return null;
        }
        if (SimpleClassNameCache.isUseFullNameForSerialisation()) {
            return input;
        }
        return IterableUtil.map(input, e -> e instanceof EntityId ? ((EntityId) e).getVertex() : e);
    }

    public static Iterable<? extends EntityId> toEntityIds(final Object... input) {
        if (null == input) {
            return null;
        }
        return (Iterable) Arrays.stream(input)
                .map(i -> i instanceof EntityId ? i : new EntitySeed(i))
                .collect(Collectors.toList());
    }

    public static Iterable<? extends EntityId> toEntityIds(final Iterable<?> input) {
        if (null == input) {
            return null;
        }
        return IterableUtil.map(input, i -> i instanceof EntityId ? i : new EntitySeed(i));
    }

    public static Iterable<?> fromEntityIds(final Iterable<? extends EntityId> input) {
        if (null == input) {
            return null;
        }
        if (SimpleClassNameCache.isUseFullNameForSerialisation()) {
            return input;
        }
        return IterableUtil.map(input, e -> e instanceof EntityId ? ((EntityId) e).getVertex() : e);
    }
}
