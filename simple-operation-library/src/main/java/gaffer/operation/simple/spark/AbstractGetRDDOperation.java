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
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

public abstract class AbstractGetRDDOperation<INPUT> extends AbstractGetOperation<INPUT, RDD<Element>> {

    private SparkContext sparkContext;

    public SparkContext getSparkContext() {
        return sparkContext;
    }

    public void setSparkContext(final SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    protected static class Builder<OP_TYPE extends AbstractGetRDDOperation<INPUT>, INPUT>
            extends AbstractGetOperation.Builder<OP_TYPE, INPUT, RDD<Element>> {

        public Builder(final OP_TYPE op) {
            super(op);
        }

        public Builder sparkContext(final SparkContext sparkContext) {
            op.setSparkContext(sparkContext);
            return this;
        }

        public Builder seeds(final Iterable<INPUT> seeds) {
            super.seeds(seeds);
            return this;
        }

        public Builder view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public OP_TYPE build() {
            return (OP_TYPE) super.build();
        }
    }
}
