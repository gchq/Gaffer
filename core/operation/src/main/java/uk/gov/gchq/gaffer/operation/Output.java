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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;

/**
 * A <code>Get</code> operation gets information out of gaffer and applies filtering
 * based on a {@link View}.
 *
 * @param <O> output type
 */
public interface Output<O> {
    default O castToOutputType(final Object result) {
        try {
            return (O) result;
        } catch (final ClassCastException e) {
            final Class<?> resultClass = null != result ? result.getClass() : null;
            throw new IllegalArgumentException("Operation result is an invalid type: " + resultClass, e);
        }
    }

    @JsonIgnore
    TypeReference<O> getOutputTypeReference();

    interface Builder<OP extends Output<O>, O, B extends Builder<OP, O, ?>> extends Operation.Builder<OP, B> {
    }
}
