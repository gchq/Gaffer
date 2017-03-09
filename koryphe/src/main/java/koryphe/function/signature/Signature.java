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

package koryphe.function.signature;

import koryphe.function.Function;
import org.apache.commons.lang3.reflect.TypeUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

/**
 * A <code>Signature</code> is the type metadata for the input or output of a {@link Function}.
 */
public abstract class Signature {
    /**
     * Tests whether the supplied types can be assigned to this <code>Signature</code>.
     * @param arguments Class or Tuple of classes to test.
     * @return True if the arguments can be assigned to this signature.
     */
    public boolean assignableFrom(final Object arguments) {
        return assignable(arguments, false);
    }

    /**
     * Tests whether this <code>Signature</code> can be assigned to the supplied types.
     * @param arguments Class or Tuple of classes to test with.
     * @return True if this signature can be assigned to the arguments.
     */
    public boolean assignableTo(final Object arguments) {
        return assignable(arguments, true);
    }

    /**
     * Tests whether this <code>Signature</code> is compatible with the types supplied.
     * @param arguments Class or Tuple of classes to test.
     * @param to If the test should be performed as an assignableTo.
     * @return True if this signature is compatible with the supplied types.
     */
    public abstract boolean assignable(final Object arguments, final boolean to);

    /**
     * Get the input signature of a function.
     * @param function Function.
     * @return Input signature.
     */
    public static Signature getInputSignature(final Function function) {
        return createSignatureFromTypeVariable(function, 0);
    }

    /**
     * Get the output signature of a function.
     * @param function Function.
     * @return Output signature.
     */
    public static Signature getOutputSignature(final Function function) {
        return createSignatureFromTypeVariable(function, 1);
    }

    /**
     * Create a <code>Signature</code> for the type variable at the given index.
     * @param function Function to create signature for.
     * @param typeVariableIndex 0 for I or 1 for O.
     * @return Signature of the type variable.
     */
    private static Signature createSignatureFromTypeVariable(final Function function, final int typeVariableIndex) {
        TypeVariable<?> tv = Function.class.getTypeParameters()[typeVariableIndex];
        Type type = TypeUtils.getTypeArguments(function.getClass(), Function.class).get(tv);
        return createSignature(type);
    }

    /**
     * Create a <code>Signature</code> for the supplied {@link Type}. This could be a singleton or
     * iterable.
     * @param type Type to create a signature for.
     * @return Signature of supplied type.
     */
    public static Signature createSignature(final Type type) {
        Type rawType = type;
        if (type instanceof ParameterizedType) {
            rawType = ((ParameterizedType) type).getRawType();
        }

        if (!(rawType instanceof Class)) {
            // cannot resolve - default to Object;
            rawType = Object.class;
        }
        Class clazz = (Class) rawType;

        if (Iterable.class.isAssignableFrom(clazz)) {
            TypeVariable<?> tv = Iterable.class.getTypeParameters()[0];
            Type t = TypeUtils.getTypeArguments(type, Iterable.class).get(tv);
            return new IterableSignature(createSignature(t));
        } else {
            return new SingletonSignature(clazz);
        }
    }
}
