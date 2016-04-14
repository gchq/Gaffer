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
import gaffer.tuple.tuplen.view.View1;
import gaffer.tuple.tuplen.view.View2;
import gaffer.tuple.tuplen.view.View3;
import gaffer.tuple.tuplen.view.View4;
import gaffer.tuple.tuplen.view.View5;
import gaffer.tuple.tuplen.view.ViewN;

/**
 * A <code>View</code> allows the selection or projection of a subset of values in a {@link gaffer.tuple.Tuple} based on
 * a {@link gaffer.tuple.view.Reference}.
 * @param <R> The type of reference used by the tuple.
 */
public abstract class View<R> {
    public abstract Object select(Tuple<R> tuple);
    public abstract void project(Tuple<R> tuple, Object value);
    public abstract Reference<R> getReference();

    /**
     * Create a new {@link TupleView} from a <code>Reference</code>.
     * @param reference Reference to create view for.
     * @param <R> Type of reference used by the view.
     * @return View for tuples based on the reference.
     */
    public static <R> View<R> createView(final Reference<R> reference) {
        if (reference.isFieldReference()) {
            R field = reference.getField();
            return field == null ? null : new FieldView<>(field);
        } else if (reference.isTupleReference()) {
            Reference<R>[] tuple = reference.getTupleReferences();
            if (tuple == null || tuple.length < 1) {
                return null;
            } else {
                switch (tuple.length) {
                    case 1:
                        return new View1<>(tuple[0]);
                    case 2:
                        return new View2<>(tuple[0], tuple[1]);
                    case 3:
                        return new View3<>(tuple[0], tuple[1], tuple[2]);
                    case 4:
                        return new View4<>(tuple[0], tuple[1], tuple[2], tuple[3]);
                    case 5:
                        return new View5<>(tuple[0], tuple[1], tuple[2], tuple[3], tuple[4]);
                    default:
                        return new ViewN(tuple);
                }
            }
        } else {
            return null;
        }
    }
}
