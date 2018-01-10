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

package uk.gov.gchq.gaffer.accumulostore.operation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.util.OperationUtil;

/**
 * {@code MultiEntityIdInputB} operations are Gaffer operations which consume multiple
 * {@link EntityId}s.
 */
public interface MultiEntityIdInputB extends MultiInputB<EntityId> {

    /**
     * Creates an array using the iterable set as the inputB and returns null if the inputB is null.
     *
     * @return an array of inputBs
     */
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "If inputB is null then null should be returned")
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @JsonGetter("inputB")
    default Object[] createInputBArrayOfVerticesAndIds() {
        if (null == getInputB()) {
            return null;
        }

        return Iterables.toArray(
                OperationUtil.fromElementIds(getInputB()),
                Object.class
        );
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @JsonSetter("inputB")
    default void setInputBFromVerticesAndIds(final Object[] inputB) {
        setInputB(OperationUtil.toEntityIds(inputB));
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "If inputB is null then null should be returned")
    @JsonIgnore
    default Object[] createInputBArray() {
        return null != getInputB() ? Iterables.toArray(getInputB(), Object.class) : null;
    }

    default void setInputB(final EntityId[] inputB) {
        if (null == inputB) {
            setInputB(((Iterable) null));
        }
        setInputB(Lists.newArrayList(inputB));
    }

    interface Builder<OP extends MultiEntityIdInputB, B extends Builder<OP, ?>>
            extends InputB.Builder<OP, Iterable<? extends EntityId>, B> {
        @SuppressWarnings("unchecked")
        default B inputB(final Object... inputB) {
            if (null != _getOp().getInputB()) {
                throw new IllegalStateException("InputB has already been set");
            }
            return inputB(Lists.newArrayList(inputB));
        }

        @Override
        default B inputB(final Iterable inputB) {
            if (null != _getOp().getInputB()) {
                throw new IllegalStateException("InputB has already been set");
            }
            _getOp().setInputB(OperationUtil.toEntityIds(inputB));
            return _self();
        }
    }
}
