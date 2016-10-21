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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractGetOperation<SEED_TYPE, RESULT_TYPE>
        extends AbstractOperation<CloseableIterable<SEED_TYPE>, RESULT_TYPE> implements GetOperation<SEED_TYPE, RESULT_TYPE> {
    private boolean includeEntities = true;
    private IncludeEdgeType includeEdges = IncludeEdgeType.ALL;
    private IncludeIncomingOutgoingType includeIncomingOutGoing = IncludeIncomingOutgoingType.BOTH;
    private SeedMatchingType seedMatching = SeedMatchingType.RELATED;
    private boolean populateProperties = true;
    private boolean deduplicate = false;
    private Integer resultLimit;

    protected AbstractGetOperation() {
        super();
    }

    protected AbstractGetOperation(final Iterable<SEED_TYPE> seeds) {
        this(new WrappedCloseableIterable<>(seeds));
    }

    protected AbstractGetOperation(final CloseableIterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    protected AbstractGetOperation(final View view) {
        super(view);
    }

    protected AbstractGetOperation(final View view, final Iterable<SEED_TYPE> seeds) {
        this(view, new WrappedCloseableIterable<>(seeds));
    }

    protected AbstractGetOperation(final View view, final CloseableIterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    protected AbstractGetOperation(final GetOperation<SEED_TYPE, ?> operation) {
        super(operation);
        setPopulateProperties(operation.isPopulateProperties());
        setIncludeEdges(operation.getIncludeEdges());
        setIncludeEntities(operation.isIncludeEntities());
        setSeedMatching(operation.getSeedMatching());
    }

    /**
     * @param seedMatching a {@link gaffer.operation.GetOperation.SeedMatchingType} describing how the seeds should be
     *                     matched to the identifiers in the graph.
     * @see uk.gov.gchq.gaffer.operation.GetOperation.SeedMatchingType
     */
    protected void setSeedMatching(final SeedMatchingType seedMatching) {
        this.seedMatching = seedMatching;
    }

    @Override
    public SeedMatchingType getSeedMatching() {
        return seedMatching;
    }

    @Override
    public CloseableIterable<SEED_TYPE> getSeeds() {
        return getInput();
    }

    public void setSeeds(final Iterable<SEED_TYPE> seeds) {
        setSeeds(new WrappedCloseableIterable<>(seeds));
    }

    @Override
    public void setSeeds(final CloseableIterable<SEED_TYPE> seeds) {
        setInput(seeds);
    }

    @JsonIgnore
    @Override
    public CloseableIterable<SEED_TYPE> getInput() {
        return super.getInput();
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonGetter(value = "seeds")
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "if the iterable is null then the array should be null")
    SEED_TYPE[] getSeedArray() {
        final Iterable<SEED_TYPE> input = getInput();
        return null != input ? (SEED_TYPE[]) Lists.newArrayList(input).toArray() : null;
    }

    @JsonSetter(value = "seeds")
    void setSeedArray(final SEED_TYPE[] seeds) {
        setInput(new WrappedCloseableIterable<>(Arrays.asList(seeds)));
    }

    @Override
    public boolean validate(final Edge edge) {
        return validateFlags(edge) && super.validate(edge);
    }

    @Override
    public boolean validate(final Entity entity) {
        return validateFlags(entity) && super.validate(entity);
    }

    @Override
    public boolean validateFlags(final Entity entity) {
        return isIncludeEntities();
    }

    @Override
    public boolean validateFlags(final Edge edge) {
        return null != getIncludeEdges() && getIncludeEdges().accept(edge.isDirected());
    }

    @Override
    public boolean isIncludeEntities() {
        return includeEntities;
    }

    @Override
    public void setIncludeEntities(final boolean includeEntities) {
        this.includeEntities = includeEntities;
    }

    @Override
    public IncludeIncomingOutgoingType getIncludeIncomingOutGoing() {
        return includeIncomingOutGoing;
    }

    @Override
    public void setIncludeIncomingOutGoing(final IncludeIncomingOutgoingType includeIncomingOutGoing) {
        this.includeIncomingOutGoing = includeIncomingOutGoing;
    }

    @Override
    public void setIncludeEdges(final IncludeEdgeType includeEdges) {
        this.includeEdges = includeEdges;
    }

    @Override
    public IncludeEdgeType getIncludeEdges() {
        return includeEdges;
    }

    @Override
    public boolean isPopulateProperties() {
        return populateProperties;
    }

    @Override
    public void setPopulateProperties(final boolean populateProperties) {
        this.populateProperties = populateProperties;
    }

    @Override
    public boolean isDeduplicate() {
        return deduplicate;
    }

    @Override
    public void setDeduplicate(final boolean deduplicate) {
        this.deduplicate = deduplicate;
    }

    @Override
    public Integer getResultLimit() {
        return resultLimit;
    }

    @Override
    public void setResultLimit(final Integer resultLimit) {
        this.resultLimit = resultLimit;
    }

    public abstract static class BaseBuilder<
            OP_TYPE extends AbstractGetOperation<SEED_TYPE, RESULT_TYPE>,
            SEED_TYPE,
            RESULT_TYPE,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, SEED_TYPE, RESULT_TYPE, ?>
            >
            extends AbstractOperation.BaseBuilder<OP_TYPE, CloseableIterable<SEED_TYPE>, RESULT_TYPE, CHILD_CLASS> {

        private List<SEED_TYPE> seeds;

        protected BaseBuilder(final OP_TYPE op) {
            super(op);
        }

        /**
         * Builds the operation and returns it.
         *
         * @return the built operation.
         */
        public OP_TYPE build() {
            if (null == op.getSeeds()) {
                if (seeds != null) {
                    op.setSeeds(seeds);
                }
            }
            return op;
        }

        /**
         * Sets an {@link java.lang.Iterable} of SEED_TYPE on the operation.
         * It should not be used in conjunction with addSeed(SEED_TYPE).
         *
         * @param newSeeds an {@link java.lang.Iterable} of SEED_TYPE to set on the operation.
         * @return this Builder
         */
        public CHILD_CLASS seeds(final Iterable<SEED_TYPE> newSeeds) {
            if (null != seeds) {
                throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
            }
            op.setSeeds(newSeeds);
            return self();
        }

        /**
         * Sets an {@link CloseableIterable} of SEED_TYPE on the operation.
         * It should not be used in conjunction with addSeed(SEED_TYPE).
         *
         * @param newSeeds an {@link CloseableIterable} of SEED_TYPE to set on the operation.
         * @return this Builder
         */
        public CHILD_CLASS seeds(final CloseableIterable<SEED_TYPE> newSeeds) {
            if (null != seeds) {
                throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
            }
            op.setSeeds(newSeeds);
            return self();
        }

        /**
         * Adds a single SEED_TYPE to a {@link java.util.LinkedList} of seeds on the operation.
         * It should not be used in conjunction with seeds(Iterable).
         *
         * @param seed a single SEED_TYPE to add to a {@link java.util.LinkedList} of seeds on the operation.
         * @return this Builder
         */
        public CHILD_CLASS addSeed(final SEED_TYPE seed) {
            if (null != op.getSeeds()) {
                throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
            }
            if (null == seeds) {
                seeds = new LinkedList<>();
            }
            seeds.add(seed);
            return self();
        }

        /**
         * @param includeEntities sets the includeEntities flag on the operation.
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.GetOperation#setIncludeEntities(boolean)
         */
        public CHILD_CLASS includeEntities(final boolean includeEntities) {
            op.setIncludeEntities(includeEntities);
            return self();
        }

        /**
         * @param includeEdgeType sets the includeEdges option on the operation.
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.GetOperation#setIncludeEdges(IncludeEdgeType)
         */
        public CHILD_CLASS includeEdges(final IncludeEdgeType includeEdgeType) {
            op.setIncludeEdges(includeEdgeType);
            return self();
        }

        /**
         * @param inOutType sets the includeIncomingOutGoing option on the operation.
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.GetOperation#setIncludeIncomingOutGoing(IncludeIncomingOutgoingType)
         */
        public CHILD_CLASS inOutType(final IncludeIncomingOutgoingType inOutType) {
            op.setIncludeIncomingOutGoing(inOutType);
            return self();
        }

        /**
         * @param deduplicate sets the deduplicate flag on the operation.
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.GetOperation#setDeduplicate(boolean)
         */
        public CHILD_CLASS deduplicate(final boolean deduplicate) {
            op.setDeduplicate(deduplicate);
            return self();
        }

        public CHILD_CLASS limitResults(final Integer resultLimit) {
            op.setResultLimit(resultLimit);
            return self();
        }

        /**
         * @param populateProperties set the populateProperties flag on the operation.
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.GetOperation#setPopulateProperties(boolean)
         */
        public CHILD_CLASS populateProperties(final boolean populateProperties) {
            op.setPopulateProperties(populateProperties);
            return self();
        }
    }

    public static final class Builder<OP_TYPE extends AbstractGetOperation<SEED_TYPE, RESULT_TYPE>, SEED_TYPE, RESULT_TYPE>
            extends BaseBuilder<OP_TYPE, SEED_TYPE, RESULT_TYPE, Builder<OP_TYPE, SEED_TYPE, RESULT_TYPE>> {

        protected Builder(final OP_TYPE op) {
            super(op);
        }

        @Override
        protected Builder<OP_TYPE, SEED_TYPE, RESULT_TYPE> self() {
            return this;
        }
    }
}
