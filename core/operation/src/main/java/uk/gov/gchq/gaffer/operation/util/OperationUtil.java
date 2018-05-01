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
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.function.FromElementId;
import uk.gov.gchq.gaffer.operation.function.FromEntityId;
import uk.gov.gchq.gaffer.operation.function.ToElementId;
import uk.gov.gchq.gaffer.operation.function.ToEntityId;
import uk.gov.gchq.koryphe.util.IterableUtil;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Utility methods for {@link uk.gov.gchq.gaffer.operation.Operation}s.
 */
public final class OperationUtil {
    private OperationUtil() {
    }

    /**
     * Converts an array of objects into {@link ElementId}s.
     * If an item is not an {@link ElementId} then it is wrapped in an {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}.
     * If an item is already an {@link ElementId} is not modified.
     *
     * @param input an array containing vertices and {@link ElementId}s.
     * @return an iterable if {@link ElementId}s.
     */
    public static Iterable<? extends ElementId> toElementIds(final Object... input) {
        if (null == input) {
            return null;
        }
        return Arrays.stream(input).map(new ToElementId()).collect(Collectors.toList());
    }

    /**
     * Converts an iterable of objects into {@link ElementId}s.
     * If an item is not an {@link ElementId} then it is wrapped in an {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}.
     * If an item is already an {@link ElementId} is not modified.
     * The conversion is done lazily.
     *
     * @param input an iterable containing vertices and {@link ElementId}s.
     * @return an iterable if {@link ElementId}s.
     */
    public static Iterable<? extends ElementId> toElementIds(final Iterable<?> input) {
        if (null == input) {
            return null;
        }
        return IterableUtil.map(input, new ToElementId());
    }

    /**
     * Takes an iterable of {@link ElementId}s and unwraps any {@link EntityId} into
     * simple vertex objects.
     * The conversion is done lazily.
     *
     * @param input an iterable if {@link ElementId}s.
     * @return an iterable if {@link uk.gov.gchq.gaffer.data.element.id.EdgeId}s and vertices.
     */
    public static Iterable<?> fromElementIds(final Iterable<? extends ElementId> input) {
        if (null == input) {
            return null;
        }
        return IterableUtil.map(input, new FromElementId());
    }

    /**
     * Converts an array of objects into {@link EntityId}s.
     * If an item is not an {@link EntityId} then it is wrapped in an {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}.
     * If an item is already an {@link EntityId} is not modified.
     *
     * @param input an array containing vertices and {@link EntityId}s.
     * @return an iterable if {@link EntityId}s.
     */
    public static Iterable<? extends EntityId> toEntityIds(final Object... input) {
        if (null == input) {
            return null;
        }
        return Arrays.stream(input).map(new ToEntityId()).collect(Collectors.toList());
    }

    /**
     * Converts an iterable of objects into {@link EntityId}s.
     * If an item is not an {@link EntityId} then it is wrapped in an {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}.
     * If an item is already an {@link EntityId} is not modified.
     * The conversion is done lazily.
     *
     * @param input an iterable containing vertices and {@link EntityId}s.
     * @return an iterable if {@link EntityId}s.
     */
    public static Iterable<? extends EntityId> toEntityIds(final Iterable<?> input) {
        if (null == input) {
            return null;
        }
        return IterableUtil.map(input, new ToEntityId());
    }

    /**
     * Takes an iterable of {@link EntityId}s and unwraps them into simple vertex objects.
     * The conversion is done lazily.
     *
     * @param input an iterable if {@link ElementId}s.
     * @return an iterable if {@link uk.gov.gchq.gaffer.data.element.id.EdgeId}s and vertices.
     */
    public static Iterable<?> fromEntityIds(final Iterable<? extends EntityId> input) {
        if (null == input) {
            return null;
        }
        return IterableUtil.map(input, new FromEntityId());
    }

    public static Operation extractNextOp(final Iterator<Operation> itr) {
        final Operation nextOp = itr.next();
        if (!(nextOp instanceof Operations) || !((Operations) nextOp).getOperations().isEmpty()) {
            return nextOp;
        }

        return null;
    }
}
