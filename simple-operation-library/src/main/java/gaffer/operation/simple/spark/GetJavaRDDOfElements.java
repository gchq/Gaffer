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

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.commonutil.iterable.WrappedCloseableIterable;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.data.ElementSeed;
import org.apache.spark.api.java.JavaSparkContext;

public class GetJavaRDDOfElements<SEED_TYPE extends ElementSeed> extends AbstractGetJavaRDD<SEED_TYPE> {

    public GetJavaRDDOfElements() {
    }

    public GetJavaRDDOfElements(final JavaSparkContext sparkContext, final Iterable<SEED_TYPE> seeds) {
        this(sparkContext, new WrappedCloseableIterable<>(seeds));
    }

    public GetJavaRDDOfElements(final JavaSparkContext sparkContext, final CloseableIterable<SEED_TYPE> seeds) {
        setJavaSparkContext(sparkContext);
        setInput(seeds);
    }

    public static class Builder<SEED_TYPE extends ElementSeed>
            extends AbstractGetJavaRDD.Builder<GetJavaRDDOfElements<SEED_TYPE>, SEED_TYPE> {

        public Builder() {
            this(new GetJavaRDDOfElements<SEED_TYPE>());
        }

        public Builder(final GetJavaRDDOfElements<SEED_TYPE> op) {
            super(op);
        }

        public Builder<SEED_TYPE> javaSparkContext(final JavaSparkContext javaSparkContext) {
            super.javaSparkContext(javaSparkContext);
            return this;
        }

        public Builder<SEED_TYPE> seeds(final Iterable<SEED_TYPE> seeds) {
            super.seeds(seeds);
            return this;
        }

        public Builder<SEED_TYPE> seeds(final CloseableIterable<SEED_TYPE> seeds) {
            super.seeds(seeds);
            return this;
        }

        public Builder<SEED_TYPE> view(final View view) {
            super.view(view);
            return this;
        }

        public Builder<SEED_TYPE> setIncludeEntities(final boolean includeEntities) {
            super.includeEntities(includeEntities);
            return this;
        }

        public Builder<SEED_TYPE> setIncludeEdges(final IncludeEdgeType includeEdgeType) {
            super.includeEdges(includeEdgeType);
            return this;
        }

        @Override
        public GetJavaRDDOfElements<SEED_TYPE> build() {
            return super.build();
        }
    }

}
