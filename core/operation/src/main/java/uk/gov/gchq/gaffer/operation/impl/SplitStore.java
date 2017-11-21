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
package uk.gov.gchq.gaffer.operation.impl;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;

import java.util.Map;

/**
 * The {@code SplitStore} operation is for splitting a store
 * based on a sequence file of split points.
 *
 * @see SplitStore.Builder
 * @deprecated use {@link SplitStoreFromFile} instead
 */
@Deprecated
public class SplitStore implements Operation {
    @Required
    private String inputPath;
    private Map<String, String> options;

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(final String inputPath) {
        this.inputPath = inputPath;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public SplitStore shallowClone() {
        return new SplitStore.Builder()
                .inputPath(inputPath)
                .options(options)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<SplitStore, Builder> {
        public Builder() {
            super(new SplitStore());
        }

        public Builder inputPath(final String inputPath) {
            _getOp().setInputPath(inputPath);
            return _self();
        }
    }
}
