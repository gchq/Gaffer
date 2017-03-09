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

package uk.gov.gchq.gaffer.accumulostore.operation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractAccumuloTwoSetSeededOperation<SEED_TYPE extends ElementSeed, ELEMENT_TYPE extends Element>
        extends GetElements<SEED_TYPE, ELEMENT_TYPE> {

    private Iterable<SEED_TYPE> seedsB;

    public AbstractAccumuloTwoSetSeededOperation() {
    }

    public AbstractAccumuloTwoSetSeededOperation(final Iterable<SEED_TYPE> seedsA, final Iterable<SEED_TYPE> seedsB) {
        super(seedsA);
        this.setSeedsB(seedsB);
    }

    public AbstractAccumuloTwoSetSeededOperation(final Iterable<SEED_TYPE> seedsA, final Iterable<SEED_TYPE> seedsB,
                                                 final View view) {
        super(view, seedsA);
        this.setSeedsB(seedsB);
    }

    @JsonGetter(value = "seedsB")
    List<SEED_TYPE> getSeedBArray() {
        final Iterable<SEED_TYPE> seedsB = getSeedsB();
        return null != seedsB ? Lists.newArrayList(seedsB) : null;
    }

    @JsonSetter(value = "seedsB")
    void setSeedBArray(final SEED_TYPE[] seedsB) {
        setSeedsB(Arrays.asList(seedsB));
    }

    @JsonIgnore
    public Iterable<SEED_TYPE> getSeedsB() {
        return seedsB;
    }

    public void setSeedsB(final Iterable<SEED_TYPE> seedsB) {
        this.seedsB = seedsB;
    }

    public abstract static class BaseBuilder<OP_TYPE extends AbstractAccumuloTwoSetSeededOperation<SEED_TYPE, ELEMENT_TYPE>,
            SEED_TYPE extends ElementSeed,
            ELEMENT_TYPE extends Element,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, SEED_TYPE, ELEMENT_TYPE, ?>>
            extends GetElements.BaseBuilder<OP_TYPE, SEED_TYPE, ELEMENT_TYPE, CHILD_CLASS> {
        private final List<SEED_TYPE> seedsB = new ArrayList<>();

        protected BaseBuilder(final OP_TYPE op) {
            super(op);
        }

        public CHILD_CLASS seedsB(final Iterable<SEED_TYPE> seedsB) {
            this.op.setSeedsB(seedsB);
            return self();
        }

        public CHILD_CLASS addSeedB(final SEED_TYPE seed) {
            this.seedsB.add(seed);
            return self();
        }

        @Override
        public OP_TYPE build() {
            super.build();
            if (!this.seedsB.isEmpty()) {
                final Iterable<SEED_TYPE> seeds = this.op.getSeedsB();
                if (null != seeds) {
                    for (final SEED_TYPE seed : seeds) {
                        this.seedsB.add(seed);
                    }
                }
                this.op.setSeedsB(this.seedsB);
            }
            return this.op;
        }
    }

    public static final class Builder<OP_TYPE extends AbstractAccumuloTwoSetSeededOperation<SEED_TYPE, ELEMENT_TYPE>,
            SEED_TYPE extends ElementSeed,
            ELEMENT_TYPE extends Element>
            extends BaseBuilder<OP_TYPE, SEED_TYPE, ELEMENT_TYPE, Builder<OP_TYPE, SEED_TYPE, ELEMENT_TYPE>> {

        protected Builder(final OP_TYPE op) {
            super(op);
        }

        @Override
        protected Builder<OP_TYPE, SEED_TYPE, ELEMENT_TYPE> self() {
            return this;
        }
    }

}
