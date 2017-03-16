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

package uk.gov.gchq.gaffer.operation.io;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.operation.Operation;
import java.util.Arrays;

public interface IterableInputB<I_ITEM> extends InputB<Iterable<I_ITEM>> {
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "If inputB is null then null should be returned")
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonGetter("inputB")
    default Object[] createInputBArray() {
        return null != getInputB() ? Iterables.toArray(getInputB(), Object.class) : null;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonSetter("inputB")
    default void setInputB(I_ITEM[] inputB) {
        setInputB(Arrays.asList(inputB));
    }

    interface Builder<OP extends IterableInputB<I_ITEM>, I_ITEM, B extends Builder<OP, I_ITEM, ?>>
            extends Operation.Builder<OP, B> {
        default B inputB(final I_ITEM... inputB) {
            return inputB(Lists.newArrayList(inputB));
        }

        default B inputB(final Iterable<? extends I_ITEM> input) {
            _getOp().setInputB((Iterable) input);
            return _self();
        }
    }
}
