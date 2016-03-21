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
import gaffer.tuple.view.View;
import gaffer.tuple.view.Reference;

/**
 * A <code>FunctionContext</code> wraps a {@link gaffer.function2.Function}. It appends application-specific
 * configuration data to the function so that it can be executed in the context of that application. The
 * <code>FunctionContext</code> uses a {@link gaffer.tuple.view.View} to select and project values into/out
 * of {@link gaffer.tuple.Tuple}s.
 * @param <F> The type of {@link gaffer.function2.Function} wrapped by the context.
 * @param <R> The type of reference used to select from and project into tuples.
 * @see gaffer.tuple.view.View
 */
public class FunctionContext<F extends Function, R> {
    protected F function;
    @JsonIgnore
    private View<R> selectionView;
    @JsonIgnore
    private View<R> projectionView;

    /**
     * Default constructor - for serialisation.
     */
    public FunctionContext() { }

    /**
     * Create a <code>FunctionContext</code> with the given function and input/output criteria.
     * @param selection Function input selection criteria.
     * @param function Function to execute.
     * @param projection Function output projection criteria.
     */
    public FunctionContext(final View<R> selection, final F function, final View<R> projection) {
        setFunction(function);
        setSelectionView(selection);
        setProjectionView(projection);
    }

    /**
     * Create a <code>FunctionContext</code> with the given function and input/output criteria.
     * @param selection Function input selection criteria.
     * @param function Function to execute.
     * @param projection Function output projection criteria.
     */
    public FunctionContext(final Reference<R> selection, final F function, final Reference<R> projection) {
        setFunction(function);
        setSelection(selection);
        setProjection(projection);
    }

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
     * @param selection Function input selection criteria.
     */
    public void setSelection(final Reference<R> selection) {
        if (selection != null) {
            setSelectionView(selection.createView());
        } else {
            setSelectionView(null);
        }
    }

    /**
     * @param selectionView Function input selection criteria.
     */
    public void setSelectionView(final View<R> selectionView) {
        this.selectionView = selectionView;
    }

    /**
     * @return Function input selection criteria.
     */
    public Reference<R> getSelection() {
        return selectionView == null ? null : selectionView.getReference();
    }

    /**
     * @return Function input selection criteria.
     */
    public View<R> getSelectionView() {
        return selectionView;
    }

    /**
     * @param projection Function output projection criteria.
     */
    public void setProjection(final Reference<R> projection) {
        if (projection != null) {
            setProjectionView(projection.createView());
        } else {
            setProjectionView(null);
        }
    }

    /**
     * @param projectionView Function output projection criteria.
     */
    public void setProjectionView(final View<R> projectionView) {
        this.projectionView = projectionView;
    }

    /**
     * @return Function output projection criteria.
     */
    public Reference<R> getProjection() {
        return projectionView == null ? null : projectionView.getReference();
    }

    /**
     * @return Function output projection criteria.
     */
    public View<R> getProjectionView() {
        return projectionView;
    }

    /**
     * @return New <code>FunctionContext</code> with the same selection criteria, function and
     * projection criteria.
     */
    public FunctionContext<F, R> copy() {
        return new FunctionContext<F, R>(this.selectionView, (F) function.copy(), this.projectionView);
    }
}
