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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public interface MultiInputB<I_ITEM> extends InputB<Iterable<? extends I_ITEM>> {
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "If inputB is null then null should be returned")
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonGetter("inputB")
    default Object[] createInputBArray() {
        return null != getInputB() ? Iterables.toArray(getInputB(), Object.class) : null;
    }

    @JsonIgnore
    @Override
    Iterable<? extends I_ITEM> getInputB();

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonSetter("inputB")
    default void setInputB(final I_ITEM[] inputB) {
        setInputB(Lists.newArrayList(inputB));
    }

    @JsonIgnore
    @Override
    void setInputB(final Iterable<? extends I_ITEM> inputB);

    interface Builder<OP extends MultiInputB<I_ITEM>, I_ITEM, B extends Builder<OP, I_ITEM, ?>>
            extends InputB.Builder<OP, Iterable<? extends I_ITEM>, B> {
        @SuppressWarnings("unchecked")
        default B inputB(final I_ITEM... inputB) {
            return inputB(Lists.newArrayList(inputB));
        }

        @Override
        default B inputB(final Iterable<? extends I_ITEM> inputB) {
            _getOp().setInputB((Iterable) inputB);
            return _self();
        }
    }
}
