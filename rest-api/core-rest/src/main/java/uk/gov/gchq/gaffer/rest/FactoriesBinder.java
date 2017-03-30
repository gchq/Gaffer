/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.rest;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;

/**
 * HK2 binder class to facilitate dependency injection with Jersey.
 * Any depedency which has the {@link javax.inject.Inject} annotation can be
 * included. This denoted which concrete instance is bound to a particular
 * interface, and optionally in which scope the binding applies.
 */
public class FactoriesBinder extends AbstractBinder {
    @Override
    protected void configure() {
        bind(getDefaultGraphFactory()).to(GraphFactory.class);
        bind(getDefaultUserFactory()).to(UserFactory.class);
    }

    private Class<?> getDefaultGraphFactory() {
        final String graphFactoryClass = System.getProperty(SystemProperty.GRAPH_FACTORY_CLASS,
                SystemProperty.GRAPH_FACTORY_CLASS_DEFAULT);

        try {
            return Class.forName(graphFactoryClass)
                    .asSubclass(GraphFactory.class);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to create graph factory from class: " + graphFactoryClass, e);
        }
    }

    private Class<?> getDefaultUserFactory() {
        final String userFactoryClass = System.getProperty(SystemProperty.USER_FACTORY_CLASS,
                SystemProperty.USER_FACTORY_CLASS_DEFAULT);

        try {
            return Class.forName(userFactoryClass)
                    .asSubclass(UserFactory.class);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to create user factory from class: " + userFactoryClass, e);
        }
    }
}
