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

package uk.gov.gchq.gaffer.operation.export;

import com.fasterxml.jackson.annotation.JsonIgnore;

import uk.gov.gchq.gaffer.operation.Operation;

/**
 * The base interface for {@link Operation}s which export results.
 */
public interface Export {
    String DEFAULT_KEY = "ALL";

    String getKey();

    @JsonIgnore
    default String getKeyOrDefault() {
        final String key = getKey();
        if (null == key) {
            return Export.DEFAULT_KEY;
        } else {
            return key;
        }
    }

    void setKey(String key);

    interface Builder<OP extends Export, B extends Builder<OP, ?>>
            extends Operation.Builder<OP, B> {
        default B key(final String key) {
            _getOp().setKey(key);
            return _self();
        }
    }
}
