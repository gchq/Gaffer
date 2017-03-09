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

package uk.gov.gchq.koryphe.tuple.function;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import uk.gov.gchq.koryphe.tuple.mask.TupleMask;
import java.util.function.Predicate;

public abstract class TupleInputPredicate<R, I, F extends Predicate<I>> {
    protected F function;
    protected TupleMask<R, I> selection;

    public TupleInputPredicate() {
    }

    public TupleInputPredicate(TupleMask<R, I> selection, F function) {
        setSelection(selection);
        setFunction(function);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public F getFunction() {
        return function;
    }

    public void setFunction(F function) {
        this.function = function;
    }

    public TupleMask<R, I> getSelection() {
        return selection;
    }

    public void setSelection(TupleMask<R, I> selection) {
        this.selection = selection;
    }
}
