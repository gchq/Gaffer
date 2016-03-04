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

package gaffer.tuple.function.context;

import gaffer.function2.Function;
import gaffer.tuple.Tuple;
import gaffer.tuple.view.TupleView;

public class FunctionContext<F extends Function, R> {
    protected F function;
    private TupleView<R> selectionView;
    private TupleView<R> projectionView;

    public FunctionContext(final TupleView<R> selectionView, final F function, final TupleView<R> projectionView) {
        setSelection(selectionView);
        setFunction(function);
        setProjection(projectionView);
    }

    public FunctionContext() { }

    public void setFunction(final F function) {
        this.function = function;
    }

    public F getFunction() {
        return function;
    }

    public Object selectFrom(final Tuple<R> source) {
        if (selectionView != null) {
            return selectionView.get(source);
        } else {
            return null;
        }
    }

    public void projectInto(final Tuple<R> target, final Object output) {
        if (projectionView != null) {
            projectionView.set(target, output);
        }
    }

    public void setSelection(final TupleView<R> selectionView) {
        this.selectionView = selectionView;
    }

    public void setProjection(final TupleView<R> projectionView) {
        this.projectionView = projectionView;
    }

    public FunctionContext<F, R> copy() {
        return new FunctionContext<F, R>(selectionView, (F) function.copy(), projectionView);
    }
}
