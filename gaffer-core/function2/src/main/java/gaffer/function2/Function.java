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

package gaffer.function2;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import gaffer.function2.signature.Signature;
import org.apache.commons.lang3.reflect.TypeUtils;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

/**
 * A <code>Function</code> is a logical unit of processing that can be applied to an input to produce an output.
 * @param <I> Function input type
 * @param <O> Function output type
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public abstract class Function<I, O> {
    private Signature inputSignature;
    private Signature outputSignature;

    /**
     * @return New <code>Function</code> of the same type.
     */
    public abstract Function<I, O> copy();

    /**
     * @return {@link gaffer.function2.signature.Signature} of the function input.
     */
    @JsonIgnore
    public Signature getInputSignature() {
        if (inputSignature == null) {
            inputSignature = createSignatureFromTypeVariable(0);
        }
        return inputSignature;
    }

    /**
     * Tests the type(s) of input that an application intends to supply to the function to check if it is compatible
     * with the generic input type specified. The argument is expected to be either a single {@link java.lang.Class}
     * or a {@link gaffer.tuple.Tuple} containing Classes.
     * @param arguments Types to check.
     * @return True if supplied type(s) can be used to execute the function.
     */
    public boolean assignableFrom(final Object arguments) {
        return getInputSignature().assignableFrom(arguments);
    }

    /**
     * @return {@link gaffer.function2.signature.Signature} of the function output.
     */
    @JsonIgnore
    public Signature getOutputSignature() {
        if (outputSignature == null) {
            outputSignature = createSignatureFromTypeVariable(1);
        }
        return outputSignature;
    }

    /**
     * Tests the type(s) of output that an application expects from the function to check if it is compatible with the
     * generic output type specified. The argument is expected to be either a single {@link java.lang.Class} or a
     * {@link gaffer.tuple.Tuple} containing Classes.
     * @param arguments Types to check.
     * @return True if supplied type(s) are compatible with the function's output.
     */
    public boolean assignableTo(final Object arguments) {
        return getOutputSignature().assignableTo(arguments);
    }

    /**
     * Create a {@link gaffer.function2.signature.Signature} for the type variable at the given index.
     * @param typeVariableIndex 0 for I or 1 for O.
     * @return Signature of the type variable.
     */
    private Signature createSignatureFromTypeVariable(final int typeVariableIndex) {
        TypeVariable<?> tv = Function.class.getTypeParameters()[typeVariableIndex];
        Type type = TypeUtils.getTypeArguments(this.getClass(), Function.class).get(tv);
        return Signature.createSignature(type);
    }
}
