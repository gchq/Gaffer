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
package uk.gov.gchq.gaffer.spark.operation.scalardd;

import org.apache.spark.SparkContext;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import java.util.Collections;

public class GetRDDOfElements<I_ITEM extends ElementSeed> extends AbstractGetRDD<I_ITEM> {

    public GetRDDOfElements() {
    }

    public GetRDDOfElements(final SparkContext sparkContext, final Iterable<I_ITEM> seeds) {
        this(sparkContext, new WrappedCloseableIterable<>(seeds));
    }

    public GetRDDOfElements(final SparkContext sparkContext, final CloseableIterable<I_ITEM> seeds) {
        setSparkContext(sparkContext);
        setInput(new WrappedCloseableIterable<>(seeds));
    }

    public GetRDDOfElements(final SparkContext sparkContext, final I_ITEM seed) {
        this(sparkContext, Collections.singleton(seed));
    }

    public abstract static class BaseBuilder<I_ITEM extends ElementSeed, CHILD_CLASS extends BaseBuilder<I_ITEM, ?>>
            extends AbstractGetRDD.BaseBuilder<GetRDDOfElements<I_ITEM>, I_ITEM, CHILD_CLASS> {

        public BaseBuilder() {
            this(new GetRDDOfElements<I_ITEM>());
        }

        public BaseBuilder(final GetRDDOfElements<I_ITEM> op) {
            super(op);
        }
    }

    public static final class Builder<I_ITEM extends ElementSeed>
            extends BaseBuilder<I_ITEM, Builder<I_ITEM>> {

        public Builder() {
        }

        public Builder(final GetRDDOfElements<I_ITEM> op) {
            super(op);
        }

        @Override
        protected Builder<I_ITEM> self() {
            return this;
        }
    }
}
