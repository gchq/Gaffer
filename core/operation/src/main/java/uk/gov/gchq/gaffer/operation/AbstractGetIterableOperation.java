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

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;

public abstract class AbstractGetIterableOperation<SEED_TYPE, RESULT_TYPE>
        extends AbstractGetOperation<SEED_TYPE, CloseableIterable<RESULT_TYPE>> implements GetIterableOperation<SEED_TYPE,  RESULT_TYPE> {
    protected boolean deduplicate = false;
    protected Integer resultLimit;

    protected AbstractGetIterableOperation() {
        super();
    }

    protected AbstractGetIterableOperation(final Iterable<SEED_TYPE> seeds) {
        this(new WrappedCloseableIterable<>(seeds));
    }

    protected AbstractGetIterableOperation(final CloseableIterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    protected AbstractGetIterableOperation(final View view) {
        super(view);
    }

    protected AbstractGetIterableOperation(final View view, final Iterable<SEED_TYPE> seeds) {
        this(view, new WrappedCloseableIterable<>(seeds));
    }

    protected AbstractGetIterableOperation(final View view, final CloseableIterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    protected AbstractGetIterableOperation(final GetIterableOperation<SEED_TYPE, ?> operation) {
        super(operation);
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
            OP_TYPE extends AbstractGetIterableOperation<SEED_TYPE, RESULT_TYPE>,
            SEED_TYPE,
            RESULT_TYPE,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, SEED_TYPE, RESULT_TYPE, ?>
            >
            extends AbstractGetOperation.BaseBuilder<OP_TYPE, SEED_TYPE, CloseableIterable<RESULT_TYPE>, CHILD_CLASS> {

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
         * @param deduplicate sets the deduplicate flag on the operation.
         * @return this Builder
         * @see GetIterableOperation#setDeduplicate(boolean)
         */
        public CHILD_CLASS deduplicate(final boolean deduplicate) {
            op.setDeduplicate(deduplicate);
            return self();
        }

        public CHILD_CLASS limitResults(final Integer resultLimit) {
            op.setResultLimit(resultLimit);
            return self();
        }
    }

    public static final class Builder<OP_TYPE extends AbstractGetIterableOperation<SEED_TYPE, RESULT_TYPE>, SEED_TYPE, RESULT_TYPE>
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
