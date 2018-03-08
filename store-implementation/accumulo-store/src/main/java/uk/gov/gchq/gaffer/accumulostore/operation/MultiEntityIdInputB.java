/*
 * Copyright 2016-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.util.OperationUtil;

/**
 * {@code MultiEntityIdInputB} operations are Gaffer operations which consume multiple
 * {@link EntityId}s.
 */
public interface MultiEntityIdInputB extends MultiInputB<EntityId> {
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @JsonSetter("inputB")
    default void setInputBFromVerticesAndIds(final Object[] inputB) {
        setInputB(OperationUtil.toEntityIds(inputB));
    }

    @Override
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    default Object[] createInputBArray() {
        return MultiInputB.super.createInputBArray();
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

        default B inputB(final EntityId... input) {
            if (null != _getOp().getInputB()) {
                throw new IllegalStateException("Input has already been set");
            }
            return inputIdsB(Lists.newArrayList(input));
        }

        default B inputIdsB(final Iterable<? extends EntityId> input) {
            if (null != _getOp().getInputB()) {
                throw new IllegalStateException("Input has already been set");
            }
            _getOp().setInputB(input);
            return _self();
        }
    }
}
