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
package uk.gov.gchq.gaffer.operation.simple.spark;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.AbstractGetOperation;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

public abstract class AbstractGetRDD<SEED_TYPE> extends AbstractGetOperation<SEED_TYPE, RDD<Element>> {

    private SparkContext sparkContext;

    public SparkContext getSparkContext() {
        return sparkContext;
    }

    public void setSparkContext(final SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    protected abstract static class BaseBuilder<OP_TYPE extends AbstractGetRDD<SEED_TYPE>,
                SEED_TYPE,
                CHILD_CLASS extends BaseBuilder<OP_TYPE, SEED_TYPE, ?>>
            extends AbstractGetOperation.BaseBuilder<OP_TYPE, SEED_TYPE, RDD<Element>, CHILD_CLASS> {

        public BaseBuilder(final OP_TYPE op) {
            super(op);
        }

        public CHILD_CLASS sparkContext(final SparkContext sparkContext) {
            op.setSparkContext(sparkContext);
            return self();
        }

        public CHILD_CLASS setIncludeEdges(final IncludeEdgeType value) {
            op.setIncludeEdges(value);
            return self();
        }

        public CHILD_CLASS setIncludeEntities(final boolean b) {
            op.setIncludeEntities(b);
            return self();
        }
    }

    protected static final class Builder<OP_TYPE extends AbstractGetRDD<SEED_TYPE>, SEED_TYPE>
            extends BaseBuilder<OP_TYPE, SEED_TYPE, Builder<OP_TYPE, SEED_TYPE>> {

        public Builder(final OP_TYPE op) {
            super(op);
        }

        @Override
        protected Builder<OP_TYPE, SEED_TYPE> self() {
            return this;
        }
    }
}
