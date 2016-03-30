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

package gaffer.function2.signature;

import gaffer.tuple.tuplen.Tuple1;
import gaffer.tuple.tuplen.Tuple2;
import gaffer.tuple.tuplen.Tuple3;
import gaffer.tuple.tuplen.Tuple4;
import gaffer.tuple.tuplen.Tuple5;
import org.apache.commons.lang3.reflect.TypeUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A <code>Signature</code> is the type metadata for the input or output of a {@link gaffer.function2.Function}.
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
     * Create a <code>Signature</code> for the supplied {@link java.lang.reflect.Type}. This could be a singleton,
     * iterable or tuple signature depending on the type supplied.
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
        Class nTupleClass = determineNTupleClass(clazz);

        if (nTupleClass != null) {
            return new TupleSignature(createTupleSignatures(nTupleClass, type));
        } else if (Iterable.class.isAssignableFrom(clazz)) {
            TypeVariable<?> tv = Iterable.class.getTypeParameters()[0];
            Type t = TypeUtils.getTypeArguments(type, Iterable.class).get(tv);
            return new IterableSignature(createSignature(t));
        } else {
            return new SingletonSignature(clazz);
        }
    }

    private static List<Signature> createTupleSignatures(final Class nTupleClass, final Type type) {
        List<Signature> signatures = new ArrayList<>();
        TypeVariable<?>[] tvs = nTupleClass.getTypeParameters();
        Map<TypeVariable<?>, Type> params = TypeUtils.getTypeArguments(type, nTupleClass);
        for (TypeVariable<?> tv : tvs) {
            signatures.add(createSignature(params.get(tv)));
        }
        return signatures;
    }

    private static Class determineNTupleClass(final Class clazz) {
        if (Tuple5.class.isAssignableFrom(clazz)) {
            return Tuple5.class;
        } else if (Tuple4.class.isAssignableFrom(clazz)) {
            return Tuple4.class;
        } else if (Tuple3.class.isAssignableFrom(clazz)) {
            return Tuple3.class;
        } else if (Tuple2.class.isAssignableFrom(clazz)) {
            return Tuple2.class;
        } else if (Tuple1.class.isAssignableFrom(clazz)) {
            return Tuple1.class;
        } else {
            return null;
        }
    }
}
