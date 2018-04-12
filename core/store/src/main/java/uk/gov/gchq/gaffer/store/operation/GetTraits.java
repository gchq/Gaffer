/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.TypeReferenceStoreImpl;

import java.util.Map;
import java.util.Set;

/**
 * An Operation used for getting traits from the Store.
 */
public class GetTraits implements Operation, Output<Set<StoreTrait>> {
    public static final boolean DEFAULT_CURRENT_TRAITS = true;

    /**
     * The currentTraits holds a boolean value, which if false
     * will return a list of all supported traits from the store.
     * If true, it will return a list of current traits.
     * By default it will return a list of current traits.
     */
    private boolean currentTraits = DEFAULT_CURRENT_TRAITS;
    private Map<String, String> options;

    @Override
    public GetTraits shallowClone() throws CloneFailedException {
        return new Builder()
                .options(options)
                .currentTraits(currentTraits)
                .build();
    }

    public boolean isCurrentTraits() {
        return currentTraits;
    }

    public void setCurrentTraits(final boolean currentTraits) {
        this.currentTraits = currentTraits;
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
    public TypeReference<Set<StoreTrait>> getOutputTypeReference() {
        return new TypeReferenceStoreImpl.StoreTraits();
    }

    public static class Builder extends BaseBuilder<GetTraits, Builder> {
        public Builder() {
            super(new GetTraits());
        }

        public Builder currentTraits(final boolean currentTraits) {
            _getOp().setCurrentTraits(currentTraits);
            return _self();
        }
    }
}
