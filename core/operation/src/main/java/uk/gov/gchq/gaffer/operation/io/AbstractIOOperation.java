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

package uk.gov.gchq.gaffer.operation.io;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractIOOperation<OPERATION extends AbstractIOOperation, INPUT> implements MultiInput<INPUT> {
    protected Iterable<? extends INPUT> input;
    protected Map<String, String> options;

    public Iterable<? extends INPUT> getInput() {
        return input;
    }

    public void setInput(final Iterable<? extends INPUT> elements) {
        this.input = elements;
    }

    public OPERATION input(final Iterable<? extends INPUT> elements) {
        if (elements != null) {
            if (getInput() == null) {
                setInput(elements);
            } else {
                setInput(Iterables.concat(getInput(), elements));
            }
        }
        return (OPERATION) this;
    }

    public OPERATION input(final INPUT... input) {
        if (input != null) {
            this.input(Lists.newArrayList(input));
        }
        return (OPERATION) this;
    }

    public OPERATION options(final Map<String, String> options) {
        if (options != null) {
            if (getOptions() == null) {
                this.setOptions(new HashMap<>());
                this.options(options);
            } else {
                this.getOptions().putAll(options);
            }
        }
        return (OPERATION) this;
    }

    public OPERATION option(final String key, final String value) {
        final Map<String, String> options = getOptions();
        if (options == null) {
            this.setOptions(new HashMap<>());
            this.option(key, value);
        } else {
            this.options.put(key, value);
        }
        return (OPERATION) this;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }
}
