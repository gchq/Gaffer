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

package uk.gov.gchq.gaffer.operation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;

public interface IterableInput<I_ITEM> extends Input<Iterable<I_ITEM>> {
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "If input is null then null should be returned")
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonGetter("input")
    default Object[] createInputArray() {
        return null != getInput() ? Iterables.toArray(getInput(), Object.class) : null;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonSetter("input")
    default void setInput(I_ITEM[] input) {
        setInput(Arrays.asList(input));
    }

    interface Builder<OP extends IterableInput<I_ITEM>, I_ITEM, B extends Builder<OP, I_ITEM, ?>>
            extends Input.Builder<OP, Iterable<I_ITEM>, B> {
        default B input(final I_ITEM... input) {
            return input(Lists.newArrayList(input));
        }
    }
}
