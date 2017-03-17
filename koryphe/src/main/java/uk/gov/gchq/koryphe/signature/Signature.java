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

package uk.gov.gchq.koryphe.signature;

import org.apache.commons.lang3.reflect.TypeUtils;
import uk.gov.gchq.koryphe.tuple.Tuple;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A <code>Signature</code> is the type metadata for the input or output of a {@link Function}.
 */
public abstract class Signature {
    /**
     * Tests whether the supplied types can be assigned to this <code>Signature</code>.
     *
     * @param arguments Class or Tuple of classes to test.
     * @return True if the arguments can be assigned to this signature.
     */
    public boolean assignableFrom(final Class... arguments) {
        return assignable(false, arguments);
    }

    /**
     * Tests whether this <code>Signature</code> can be assigned to the supplied types.
     *
     * @param arguments Class or Tuple of classes to test with.
     * @return True if this signature can be assigned to the arguments.
     */
    public boolean assignableTo(final Class... arguments) {
        return assignable(true, arguments);
    }

    /**
     * Tests whether this <code>Signature</code> is compatible with the types supplied.
     *
     * @param to        If the test should be performed as an assignableTo.
     * @param arguments Class or Tuple of classes to test.
     * @return True if this signature is compatible with the supplied types.
     */
    public abstract boolean assignable(final boolean to, final Class<?>... arguments);

    public boolean assignable(final Class<?>... arguments) {
        return assignable(true, arguments);
    }

    public abstract Class[] getClasses();

    /**
     * Get the input signature of a function.
     *
     * @param function Function.
     * @return Input signature.
     */
    public static Signature getInputSignature(final Predicate function) {
        return createSignatureFromTypeVariable(function, Predicate.class, 0);
    }

    /**
     * Get the input signature of a function.
     *
     * @param function Function.
     * @return Input signature.
     */
    public static Signature getInputSignature(final Function function) {
        return createSignatureFromTypeVariable(function, Function.class, 0);
    }

    /**
     * Get the output signature of a function.
     *
     * @param function Function.
     * @return Output signature.
     */
    public static Signature getOutputSignature(final Function function) {
        return createSignatureFromTypeVariable(function, Function.class, 1);
    }

    /**
     * Create a <code>Signature</code> for the type variable at the given index.
     *
     * @param function          Function to create signature for.
     * @param typeVariableIndex 0 for I or 1 for O.
     * @return Signature of the type variable.
     */
    protected static Signature createSignatureFromTypeVariable(final Object function, final Class functionClass, final int typeVariableIndex) {
        TypeVariable<?> tv = functionClass.getTypeParameters()[typeVariableIndex];
        final Map<TypeVariable<?>, Type> typeArgs = TypeUtils.getTypeArguments(function.getClass(), functionClass);
        Type type = typeArgs.get(tv);
        return createSignature(type, typeArgs);
    }

    /**
     * Create a <code>Signature</code> for the supplied {@link Type}. This could be a singleton or
     * iterable.
     *
     * @param type Type to create a signature for.
     * @return Signature of supplied type.
     */
    protected static Signature createSignature(final Type type) {
        return createSignature(type, Collections.emptyMap());
    }

    protected static Signature createSignature(final Type type, final Map<TypeVariable<?>, Type> typeArgs) {
        final Class clazz = getTypeClass(type, typeArgs);

        if (Tuple.class.isAssignableFrom(clazz)) {
            final TypeVariable[] tupleTypes = getTypeClass(type, typeArgs).getTypeParameters();
            final Map<TypeVariable<?>, Type> classTypeArgs = TypeUtils.getTypeArguments(type, clazz);
            Collection<? extends Type> types = TypeUtils.getTypeArguments(type, clazz).values();
            Class[] classes = new Class[types.size()];
            int i = 0;
            for (TypeVariable tupleType : tupleTypes) {
                classes[i++] = getTypeClass(classTypeArgs.get(tupleType), typeArgs);
            }

            return new TupleSignature(classes);
        }

        return new SingletonSignature(clazz);
    }

    protected static Class getTypeClass(final Type type, final Map<TypeVariable<?>, Type> typeArgs) {
        Type rawType = type;
        if (type instanceof ParameterizedType) {
            rawType = ((ParameterizedType) type).getRawType();
        }

        if (rawType instanceof Class) {
            return (Class) rawType;
        }


        if (rawType instanceof TypeVariable) {
            final Type t = typeArgs.get(rawType);
            if (null != t) {
                return getTypeClass(t, typeArgs);
            }
        }
        // cannot resolve - default to Object;
        return Object.class;
    }
}
