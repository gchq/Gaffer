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
import java.util.HashMap;
import java.util.Map;

public interface Options {
    /**
     * @return the operation options. This may contain store specific options such as authorisation strings or and
     * other properties required for the operation to be executed. Note these options will probably not be interpreted
     * in the same way by every store implementation.
     */
    Map<String, String> getOptions();

    /**
     * @param options the operation options. This may contain store specific options such as authorisation strings or and
     *                other properties required for the operation to be executed. Note these options will probably not be interpreted
     *                in the same way by every store implementation.
     */
    void setOptions(final Map<String, String> options);

    /**
     * Adds an operation option. This may contain store specific options such as authorisation strings or and
     * other properties required for the operation to be executed. Note these options will probably not be interpreted
     * in the same way by every store implementation.
     *
     * @param name  the name of the option
     * @param value the value of the option
     */
    default void addOption(final String name, final String value) {
        if (null == getOptions()) {
            setOptions(new HashMap<>());
        }

        getOptions().put(name, value);
    }

    /**
     * Gets an operation option by its given name.
     *
     * @param name the name of the option
     * @return the value of the option
     */
    default String getOption(final String name) {
        if (null == getOptions()) {
            return null;
        }

        return getOptions().get(name);
    }

    @JsonGetter("options")
    default Map<String, String> _getNullOrOptions() {
        if (null == getOptions()) {
            return null;
        }

        return getOptions().isEmpty() ? null : getOptions();
    }

    interface Builder<OP extends Options, B extends Builder<OP, ?>> extends Operation.Builder<OP, B> {
        /**
         * @param name  the name of the option to add
         * @param value the value of the option to add
         * @return this Builder
         * @see Options#addOption(String, String)
         */
        default B option(final String name, final String value) {
            _getOp().addOption(name, value);
            return _self();
        }

        default B options(final Map<String, String> options) {
            if (null == _getOp().getOptions()) {
                _getOp().setOptions(new HashMap<>(options));
            } else {
                _getOp().getOptions().putAll(options);
            }
            return _self();
        }
    }
}

