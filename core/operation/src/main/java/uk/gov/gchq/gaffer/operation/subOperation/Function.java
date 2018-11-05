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

package uk.gov.gchq.gaffer.operation.subOperation;

import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Objects;

public class Function extends ViewElementLevel3 implements IFunction {

    private KorypheFunction function;

    public Function(final OperationChain operationChain) {
        super(operationChain);

    }

    @Override
    public ViewElementLevel3 function4(String predicateClassName, Object... args) {
        Class[] argClasses = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argClasses[i] = args[i].getClass();
        }
        try {
            Class<?> aClass = this.getClass().getClassLoader().loadClass(predicateClassName);
            if (KorypheFunction.class.isAssignableFrom(aClass)) {
                outter:
                for (Constructor<?> constructor : aClass.getDeclaredConstructors()) {
                    if (constructor.getParameterCount() == args.length) {
                        Class<?>[] parameterTypes = constructor.getParameterTypes();
                        for (int i = 0; i < args.length; i++) {
                            if (!parameterTypes[i].isAssignableFrom(argClasses[i])) {
                                break outter;
                            }
                        }
                        function = (KorypheFunction) constructor.newInstance(args);
                        break;
                    }
                }
                if (Objects.isNull(function)) {
                    throw new RuntimeException("constructor not found for class: " + aClass.getName() + " with args: " + Arrays.toString(args));
                }
            } else {
                throw new RuntimeException("class name given is not assignable from KorypheFunction: " + predicateClassName);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error creating KorypheFunction.", e);
        }

        tidyUp();
        return new ViewElementLevel3(this);
    }

    protected void tidyUp() {

        ElementTransformer.Builder elementTransformer = super.getElementTransformer();
        if (Objects.isNull(elementTransformer)) {
            elementTransformer = new ElementTransformer.Builder();
            super.elementTransformer(elementTransformer);
        }

        elementTransformer
                .select(getPropFrom())
                .execute(function)
                .project(getPropTo());

        clear();
    }

    private void clear() {
        this.function = null;
        super.clearPropFrom().clearPropTo();
    }
}
