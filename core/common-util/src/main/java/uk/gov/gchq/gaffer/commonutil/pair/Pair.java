/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.commonutil.pair;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;

public interface Pair<F, S> extends Serializable {
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    F getFirst();

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    S getSecond();

    static <F, S> Pair<F, S> of(final F first, final S second) {
        if (null != first) {
            return new ImmutablePair<>(first, second);
        } else {
            throw new IllegalArgumentException("First entry in a Pair object cannot be null.");
        }
    }
}
