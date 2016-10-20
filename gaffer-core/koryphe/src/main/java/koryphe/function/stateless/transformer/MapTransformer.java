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

package koryphe.function.stateless.transformer;

import java.util.Map;

public class MapTransformer<K, I, O> implements Transformer<Map<K, I>, Map<K, O>> {
    private Transformer<I, O> transformer;

    public MapTransformer() { }

    public MapTransformer(final Transformer<I, O> transformer) {
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
        for (Map.Entry<K, I> entry : input.entrySet()) {
            Map<K, O> transformed = (Map<K, O>) input;
            transformed.put(entry.getKey(), transformer.execute(entry.getValue()));
        }
        return (Map<K, O>) input;
    }
}
