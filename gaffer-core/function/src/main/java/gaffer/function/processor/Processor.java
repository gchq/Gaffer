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

package gaffer.function.processor;

import gaffer.function.context.FunctionContext;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * A <code>Processor</code> executes {@link gaffer.function.Function}s against {@link gaffer.function.Tuple}s. It
 * uses {@link gaffer.function.context.FunctionContext}s to bind functions to data in tuples.
 *
 * @param <R> The type of reference used by tuples.
 * @param <C> The type of {@link FunctionContext} to use.
 */
public abstract class Processor<R, C extends FunctionContext<?>> implements Cloneable {
    protected List<C> functions;

    /**
     * Default constructor - used for serialisation.
     */
    public Processor() {
    }

    /**
     * Create a <code>Processor</code> that executes the given {@link gaffer.function.context.FunctionContext}s.
     *
     * @param functions {@link gaffer.function.context.FunctionContext}s to execute.
     */
    public Processor(final Collection<C> functions) {
        addFunctions(functions);
    }

    /**
     * Add a {@link gaffer.function.context.FunctionContext} to be executed by this <code>Processor</code>.
     *
     * @param functionContext {@link gaffer.function.context.FunctionContext} to be executed.
     */
    public void addFunction(final C functionContext) {
        if (functions == null) {
            functions = new LinkedList<>();
        }
        functions.add(functionContext);
    }

    /**
     * Add a collection of {@link gaffer.function.context.FunctionContext}s to be executed by this
     * <code>Processor</code>.
     *
     * @param functionContext {@link gaffer.function.context.FunctionContext}s to be executed.
     */
    public void addFunctions(final Collection<C> functionContext) {
        if (functions == null) {
            functions = new LinkedList<>();
        }
        functions.addAll(functionContext);
    }

    /**
     * @return {@link gaffer.function.context.FunctionContext}s to be executed by this <code>Processor</code>.
     */
    public List<C> getFunctions() {
        return functions;
    }

    /**
     * @return Deep copy of this <code>Processor</code>.
     */
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public abstract Processor<R, C> clone();

    /**
     * @param functions {@link gaffer.function.context.FunctionContext}s to be executed by this <code>Processor</code>.
     */
    void setFunctions(final List<C> functions) {
        this.functions = functions;
    }
}
