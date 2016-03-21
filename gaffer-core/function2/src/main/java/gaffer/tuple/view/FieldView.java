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
 * A <code>FieldView</code> allows the selection of a single value from a {@link gaffer.tuple.Tuple}.
 * @param <R> The type of reference used by the tuple.
 */
public class FieldView<R> implements View<R> {
    private R reference;

    /**
     * Create a <code>FieldView</code> from a singleton {@link Reference}.
     * @param reference Singleton reference.
     */
    public FieldView(final Reference<R> reference) {
        setReference(reference);
    }

    /**
     * Select the referenced value from a source {@link gaffer.tuple.Tuple}.
     * @param source Source tuple.
     * @return Selected value.
     */
    public Object select(final Tuple<R> source) {
        return source.get(reference);
    }

    /**
     * Project a value into a target {@link gaffer.tuple.Tuple}.
     * @param target Target tuple.
     * @param value Value to project.
     */
    public void project(final Tuple<R> target, final Object value) {
        target.put(reference, value);
    }

    /**
     * Get the {@link Reference} used by this <code>FieldView</code>.
     * @return Singleton reference.
     */
    public Reference<R> getReference() {
        if (reference == null) {
            return null;
        } else {
            Reference<R> ref = new Reference<>();
            ref.setField(reference);
            return ref;
        }
    }

    /**
     * Set the {@link Reference} to be used by this <code>FieldView</code>.
     * @param reference Singleton reference.
     * @throws IllegalArgumentException If reference is null or not a singleton.
     */
    public void setReference(final Reference<R> reference) {
        if (reference == null || reference.getField() == null) {
            throw new IllegalArgumentException("Reference passed to FieldView must be a non-null field reference.");
        } else {
            this.reference = reference.getField();
        }
    }
}
