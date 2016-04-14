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

package gaffer.tuple.view;

import gaffer.tuple.Tuple;

/**
 * A view that selects from or projects into a tuple using a single field reference.
 * @param <R> The type of reference used by the tuple.
 */
public class FieldView<R> extends View<R> {
    private R reference;

    public FieldView(final R reference) {
        this.reference = reference;
    }

    @Override
    public Object select(final Tuple<R> tuple) {
        return tuple.get(reference);
    }

    @Override
    public void project(final Tuple<R> tuple, final Object value) {
        tuple.put(reference, value);
    }

    @Override
    public Reference<R> getReference() {
        return new Reference<>(reference);
    }
}
