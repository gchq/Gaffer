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

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.operation.AbstractSeededGet;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

public class SeededGetImpl<I_ITEM, O> extends AbstractSeededGet<I_ITEM, O> {
    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.Object();
    }

    public static final class Builder<I_ITEM, O>
            extends AbstractSeededGet.BaseBuilder<SeededGetImpl<I_ITEM, O>, I_ITEM, O, Builder<I_ITEM, O>> {
        public Builder() {
            super(new SeededGetImpl<>());
        }

        @Override
        protected Builder<I_ITEM, O> self() {
            return this;
        }
    }
}