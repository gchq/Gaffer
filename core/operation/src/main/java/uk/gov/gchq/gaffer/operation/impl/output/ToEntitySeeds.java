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
package uk.gov.gchq.gaffer.operation.impl.output;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.io.IterableInputIterableOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

public class ToEntitySeeds implements
        Operation,
        IterableInputIterableOutput<Object, EntitySeed> {
    private Iterable<Object> input;

    @Override
    public Iterable<Object> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<Object> input) {
        this.input = input;
    }

    @Override
    public TypeReference<CloseableIterable<EntitySeed>> getOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableEntitySeed();
    }

    public static final class Builder
            extends BaseBuilder<ToEntitySeeds, ToEntitySeeds.Builder>
            implements IterableInputIterableOutput.Builder<ToEntitySeeds, Object, EntitySeed, ToEntitySeeds.Builder> {
        public Builder() {
            super(new ToEntitySeeds());
        }
    }
}
