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
import org.apache.spark.api.java.JavaSparkContext;

public class GetJavaRDDOfElementsOperation extends AbstractGetJavaRDDOperation<EntitySeed> {

    public GetJavaRDDOfElementsOperation() { }

    public GetJavaRDDOfElementsOperation(final JavaSparkContext sparkContext, final Iterable<EntitySeed> entitySeeds) {
        setJavaSparkContext(sparkContext);
        setInput(entitySeeds);
    }

    public static class Builder extends AbstractGetJavaRDDOperation.Builder<GetJavaRDDOfElementsOperation, EntitySeed> {
        public Builder() {
            this(new GetJavaRDDOfElementsOperation());
        }

        public Builder(final GetJavaRDDOfElementsOperation op) {
            super(op);
        }

        public Builder javaSparkContext(final JavaSparkContext javaSparkContext) {
            super.javaSparkContext(javaSparkContext);
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
        public GetJavaRDDOfElementsOperation build() {
            return (GetJavaRDDOfElementsOperation) super.build();
        }
    }

}
