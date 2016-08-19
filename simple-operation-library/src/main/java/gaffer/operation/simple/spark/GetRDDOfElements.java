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

import gaffer.data.elementdefinition.view.View;
import gaffer.operation.data.EntitySeed;
import org.apache.spark.SparkContext;

import java.util.Collections;

public class GetRDDOfElements extends AbstractGetRDD<EntitySeed> {

    public GetRDDOfElements() { }

    public GetRDDOfElements(final SparkContext sparkContext, final Iterable<EntitySeed> entitySeeds) {
        setSparkContext(sparkContext);
        setInput(entitySeeds);
    }

    public GetRDDOfElements(final SparkContext sparkContext, final EntitySeed entitySeed) {
        this(sparkContext, Collections.singleton(entitySeed));
    }

    public static class Builder extends AbstractGetRDD.Builder<GetRDDOfElements, EntitySeed> {
        public Builder() {
            this(new GetRDDOfElements());
        }

        public Builder(final GetRDDOfElements op) {
            super(op);
        }

        public Builder sparkContext(final SparkContext sparkContext) {
            super.sparkContext(sparkContext);
            return this;
        }

        public Builder seeds(final Iterable<EntitySeed> seeds) {
            super.seeds(seeds);
            return this;
        }

        public Builder view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public GetRDDOfElements build() {
            return (GetRDDOfElements) super.build();
        }
    }
}
