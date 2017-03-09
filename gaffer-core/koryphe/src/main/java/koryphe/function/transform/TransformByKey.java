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

package koryphe.function.transform;

import java.util.HashMap;
import java.util.Map;

/**
 * Applies a {@link Transformer} to the values of a {@link Map}.
 * @param <I> Type of input value
 * @param <O> Type of output value
 */
public class TransformByKey<K, I, O> implements Transformer<Map<K, I>, Map<K, O>> {
    private Transformer<I, O> transformer;

    public TransformByKey() { }

    public TransformByKey(final Transformer<I, O> transformer) {
        setTransformer(transformer);
    }

    public void setTransformer(final Transformer<I, O> transformer) {
        this.transformer = transformer;
    }

    public Transformer<I, O> getTransformer() {
        return transformer;
    }

    @Override
    public Map<K, O> execute(final Map<K, I> input) {
        if (input == null) {
            return null;
        } else {
            Map<K, O> transformed = new HashMap<K, O>(input.size());
            for (Map.Entry<K, I> entry : input.entrySet()) {
                transformed.put(entry.getKey(), transformer.execute(entry.getValue()));
            }
            return transformed;
        }
    }
}
