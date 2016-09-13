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
package gaffer.operation.simple.spark;

import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.AbstractGetOperation;
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

    protected static class Builder<OP_TYPE extends AbstractGetJavaRDD<SEED_TYPE>, SEED_TYPE>
            extends AbstractGetOperation.Builder<OP_TYPE, SEED_TYPE, JavaRDD<Element>> {

        public Builder(final OP_TYPE op) {
            super(op);
        }

        public Builder<OP_TYPE, SEED_TYPE> javaSparkContext(final JavaSparkContext sparkContext) {
            op.setJavaSparkContext(sparkContext);
            return this;
        }

        public Builder<OP_TYPE, SEED_TYPE> seeds(final Iterable<SEED_TYPE> seeds) {
            super.seeds(seeds);
            return this;
        }

        public Builder<OP_TYPE, SEED_TYPE> view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public OP_TYPE build() {
            return super.build();
        }
    }
}
