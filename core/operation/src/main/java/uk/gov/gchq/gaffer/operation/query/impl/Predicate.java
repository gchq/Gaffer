/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.query.impl;

import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.query.IPredicate;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Objects;

public class Predicate<OUT> extends ViewElement<OUT> implements IPredicate {

    private KoryphePredicate predicate;

    public Predicate(final OperationChain<OUT> outFilter3) {
        super(outFilter3);
        if (outFilter3 instanceof Predicate) {
            Predicate that = (Predicate) outFilter3;
            this.predicate = that.predicate;
        }
    }

    public ViewElement predicate(String predicateClassName, Object... args) {
        Class[] argClasses = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argClasses[i] = args[i].getClass();
        }
        try {
            Class<?> aClass = this.getClass().getClassLoader().loadClass(predicateClassName);
            if (KoryphePredicate.class.isAssignableFrom(aClass)) {
                outter:
                for (Constructor<?> constructor : aClass.getDeclaredConstructors()) {
                    if (constructor.getParameterCount() == args.length) {
                        Class<?>[] parameterTypes = constructor.getParameterTypes();
                        for (int i = 0; i < args.length; i++) {
                            if (!parameterTypes[i].isAssignableFrom(argClasses[i])) {
                                break outter;
                            }
                        }
                        predicate = (KoryphePredicate) constructor.newInstance(args);
                        break;
                    }
                }
                if (Objects.isNull(predicate)) {
                    throw new RuntimeException("constructor not found for class: " + aClass.getName() + " with args: " + Arrays.toString(args));
                }
            } else {
                throw new RuntimeException("class name given is not assignable from KoryphePredicate: " + predicateClassName);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error creating KoryphePredicate.", e);
        }

        tidyUp();
        return new ViewElement(this);
    }

    @Override
    protected void tidyUp() {
        final String filterProp = getFilterProp();
        if (Objects.nonNull(filterProp)) {
            ElementFilter.Builder elementFilter = super.getElementFilter();
            if (Objects.isNull(elementFilter)) {
                elementFilter = new ElementFilter.Builder();
                super.elementFilter(elementFilter);
            }

            elementFilter
                    .select(filterProp)
                    .execute(predicate);
        }

        clear();
    }

    private void clear() {
        predicate = null;
        super.clearFilterProp();
    }


}
