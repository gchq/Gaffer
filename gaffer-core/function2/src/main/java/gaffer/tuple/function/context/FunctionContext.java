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

import com.fasterxml.jackson.annotation.JsonIgnore;
import gaffer.function2.Function;
import gaffer.tuple.Tuple;
import gaffer.tuple.handler.TupleHandler;
import gaffer.tuple.handler.TupleView;

/**
 * A <code>FunctionContext</code> wraps a {@link gaffer.function2.Function}. It appends application-specific
 * configuration data to the function so that it can be executed in the context of that application. The
 * <code>FunctionContext</code> uses a {@link gaffer.tuple.handler.TupleView} to select and project values
 * into/out of {@link gaffer.tuple.Tuple}s.
 * @param <F> The type of {@link gaffer.function2.Function} wrapped by the context.
 * @param <R> The type of reference used to select from and project into tuples.
 * @see gaffer.tuple.handler.TupleView
 */
public class FunctionContext<F extends Function, R> implements TupleHandler<R> {
    protected F function;
    private TupleView<R> selectionView;
    private TupleView<R> projectionView;

    /**
     * Create a <code>FunctionContext</code> with the given selection and projection.
     * @param selectionView Function input selection criteria
     * @param function Function to execute
     * @param projectionView Function output projection criteria
     */
    public FunctionContext(final TupleView<R> selectionView, final F function, final TupleView<R> projectionView) {
        setSelection(selectionView);
        setFunction(function);
        setProjection(projectionView);
    }

    /**
     * Default constructor - for serialisation.
     */
    public FunctionContext() { }

    /**
     * @param function Function to execute.
     */
    public void setFunction(final F function) {
        this.function = function;
    }

    /**
     * @return Function to execute.
     */
    public F getFunction() {
        return function;
    }

    /**
     * Select the input value for the function from a source {@link gaffer.tuple.Tuple}.
     * @param source Source tuple.
     * @return Input value.
     */
    public Object select(final Tuple<R> source) {
        if (selectionView != null) {
            return selectionView.select(source);
        } else {
            return null;
        }
    }

    /**
     * Project the output value from the function into a target {@link gaffer.tuple.Tuple}.
     * @param target Target tuple.
     * @param output Output value.
     */
    public void project(final Tuple<R> target, final Object output) {
        if (projectionView != null) {
            projectionView.project(target, output);
        }
    }

    /**
     * @param selectionView Function input selection criteria.
     */
    @JsonIgnore
    public void setSelection(final TupleView<R> selectionView) {
        this.selectionView = selectionView;
    }

    /**
     * @param selection References of tuple values to select.
     */
    public void setSelection(final R[][] selection) {
        selectionView = new TupleView<R>(selection);
    }

    /**
     * @param selection References of tuple values to select.
     */
    public void setSelection(final R[] selection) {
        selectionView = new TupleView<R>(selection);
    }

    /**
     * @return References of tuple values to select.
     */
    public R[][] getSelection() {
        return selectionView.getReferences();
    }

    /**
     * @param projectionView Function output projection criteria.
     */
    @JsonIgnore
    public void setProjection(final TupleView<R> projectionView) {
        this.projectionView = projectionView;
    }

    /**
     * @param projection References of tuple values to project.
     */
    public void setProjection(final R[][] projection) {
        projectionView = new TupleView<R>(projection);
    }

    /**
     * @param projection References of tuple values to project.
     */
    public void setProjection(final R[] projection) {
        projectionView = new TupleView<R>(projection);
    }

    /**
     * @return References of tuple values to project.
     */
    public R[][] getProjection() {
        return projectionView.getReferences();
    }

    /**
     * @return New <code>FunctionContext</code> with the same selection criteria, function and
     * projection criteria.
     */
    public FunctionContext<F, R> copy() {
        return new FunctionContext<F, R>(selectionView, (F) function.copy(), projectionView);
    }
}
