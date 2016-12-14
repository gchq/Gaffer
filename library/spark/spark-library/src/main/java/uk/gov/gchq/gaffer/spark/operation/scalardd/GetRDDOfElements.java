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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import java.util.Collections;

public class GetRDDOfElements<SEED_TYPE extends ElementSeed> extends AbstractGetRDD<SEED_TYPE> {
    private TypeReference<RDD<Element>> typeReference =
            new TypeReference<RDD<Element>>() {
            };

    public GetRDDOfElements() {
    }

    public GetRDDOfElements(final SparkContext sparkContext, final Iterable<SEED_TYPE> seeds) {
        this(sparkContext, new WrappedCloseableIterable<>(seeds));
    }

    public GetRDDOfElements(final SparkContext sparkContext, final CloseableIterable<SEED_TYPE> seeds) {
        setSparkContext(sparkContext);
        setInput(new WrappedCloseableIterable<>(seeds));
    }

    public GetRDDOfElements(final SparkContext sparkContext, final SEED_TYPE seed) {
        this(sparkContext, Collections.singleton(seed));
    }

    @JsonIgnore
    @Override
    public TypeReference<RDD<Element>> getTypeReference() {
        return typeReference;
    }

    public abstract static class BaseBuilder<SEED_TYPE extends ElementSeed, CHILD_CLASS extends BaseBuilder<SEED_TYPE, ?>>
            extends AbstractGetRDD.BaseBuilder<GetRDDOfElements<SEED_TYPE>, SEED_TYPE, CHILD_CLASS> {

        public BaseBuilder() {
            this(new GetRDDOfElements<SEED_TYPE>());
        }

        public BaseBuilder(final GetRDDOfElements<SEED_TYPE> op) {
            super(op);
        }
    }

    public static final class Builder<SEED_TYPE extends ElementSeed>
            extends BaseBuilder<SEED_TYPE, Builder<SEED_TYPE>> {

        public Builder() {
        }

        public Builder(final GetRDDOfElements<SEED_TYPE> op) {
            super(op);
        }

        @Override
        protected Builder<SEED_TYPE> self() {
            return this;
        }
    }
}
