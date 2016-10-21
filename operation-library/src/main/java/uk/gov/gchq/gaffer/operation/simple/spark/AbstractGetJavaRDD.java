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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class AbstractGetJavaRDD<SEED_TYPE> extends AbstractGetOperation<SEED_TYPE, JavaRDD<Element>> {

    private JavaSparkContext javaSparkContext;

    public JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
    }

    public void setJavaSparkContext(final JavaSparkContext javaSparkContext) {
        this.javaSparkContext = javaSparkContext;
    }

    protected abstract static class BaseBuilder<OP_TYPE extends AbstractGetJavaRDD<SEED_TYPE>,
            SEED_TYPE,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, SEED_TYPE, ?>>
            extends AbstractGetOperation.BaseBuilder<OP_TYPE, SEED_TYPE, JavaRDD<Element>, CHILD_CLASS> {

        public BaseBuilder(final OP_TYPE op) {
            super(op);
        }

        public CHILD_CLASS javaSparkContext(final JavaSparkContext sparkContext) {
            op.setJavaSparkContext(sparkContext);
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

    public static final class Builder<OP_TYPE extends AbstractGetJavaRDD<SEED_TYPE>, SEED_TYPE>
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
