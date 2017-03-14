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
package uk.gov.gchq.gaffer.spark.operation.javardd;

import org.apache.spark.api.java.JavaSparkContext;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;

public class GetJavaRDDOfElements<I_ITEM extends ElementSeed> extends AbstractGetJavaRDD<I_ITEM> {

    public GetJavaRDDOfElements() {
    }

    public GetJavaRDDOfElements(final JavaSparkContext sparkContext, final Iterable<I_ITEM> seeds) {
        this(sparkContext, new WrappedCloseableIterable<>(seeds));
    }

    public GetJavaRDDOfElements(final JavaSparkContext sparkContext, final CloseableIterable<I_ITEM> seeds) {
        setJavaSparkContext(sparkContext);
        setInput(seeds);
    }

    public abstract static class BaseBuilder<I_ITEM extends ElementSeed, CHILD_CLASS extends BaseBuilder<I_ITEM, ?>>
            extends AbstractGetJavaRDD.BaseBuilder<GetJavaRDDOfElements<I_ITEM>, I_ITEM, CHILD_CLASS> {

        public BaseBuilder() {
            this(new GetJavaRDDOfElements<I_ITEM>());
        }

        public BaseBuilder(final GetJavaRDDOfElements<I_ITEM> op) {
            super(op);
        }
    }

    public static final class Builder<I_ITEM extends ElementSeed>
            extends BaseBuilder<I_ITEM, Builder<I_ITEM>> {

        public Builder() {
        }

        public Builder(final GetJavaRDDOfElements<I_ITEM> op) {
            super(op);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
