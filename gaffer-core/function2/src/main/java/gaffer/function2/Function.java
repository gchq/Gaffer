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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import gaffer.tuple.Tuple;
import org.apache.commons.lang3.reflect.TypeUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

/**
 * A <code>Function</code> is a logical unit of processing that can be applied to an input to produce an output.
 * @param <I> Function input type
 * @param <O> Function output type
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public abstract class Function<I, O> {
    /**
     * @return New <code>Function</code> of the same type.
     */
    public abstract Function<I, O> copy();

    /**
     * Tests the type(s) of input that an application intends to supply to the function to check if it is compatible
     * with the generic input type specified. The argument is expected to be either a single {@link java.lang.Class}
     * or a {@link gaffer.tuple.Tuple} containing Classes.
     * @param classOrTuple Types to check.
     * @return True if supplied type(s) can be used to execute the function.
     */
    public boolean validateInput(final Object classOrTuple) {
        TypeVariable<?> tv = Function.class.getTypeParameters()[1];
        Type type = TypeUtils.getTypeArguments(this.getClass(), Function.class).get(tv);
        if (type instanceof ParameterizedType) {
            type = ((ParameterizedType) type).getRawType();
        }
        if (type instanceof Class) {
            return isCompatible((Class) type, classOrTuple);
        } else {
            throw new IllegalStateException("Generic input type cannot be resolved to a Class");
        }
    }

    /**
     * Tests the type(s) of output that an application expects from the function to check if it is compatible with the
     * generic output type specified. The argument is expected to be either a single {@link java.lang.Class} or a
     * {@link gaffer.tuple.Tuple} containing Classes.
     * @param classOrTuple Types to check.
     * @return True if supplied type(s) are compatible with the function's output.
     */
    public boolean validateOutput(final Object classOrTuple) {
        TypeVariable<?> tv = Function.class.getTypeParameters()[1];
        Type type = TypeUtils.getTypeArguments(this.getClass(), Function.class).get(tv);
        if (type instanceof ParameterizedType) {
            type = ((ParameterizedType) type).getRawType();
        }
        if (type instanceof Class) {
            return isCompatible((Class) type, classOrTuple);
        } else {
            throw new IllegalStateException("Generic output type cannot be resolved to a Class");
        }
    }

    private boolean isCompatible(final Class expected, final Object classOrTuple) {
        if (classOrTuple instanceof Tuple) {
            return Iterable.class.isAssignableFrom(expected);
        } else {
            if (classOrTuple instanceof Class) {
                return expected.isAssignableFrom((Class) classOrTuple);
            } else {
                throw new IllegalArgumentException("Supplied value must be either a Class or Tuple.");
            }
        }
    }
}
