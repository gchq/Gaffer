/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v2.example;

import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Singleton;

/**
 * HK2 binder class to facilitate dependency injection with Jersey.
 * Any dependency which has the {@link javax.inject.Inject} annotation can be
 * included. This denoted which concrete instance is bound to a particular
 * interface, and optionally in which scope the binding applies.
 */
public class ExampleBinder extends AbstractBinder {

    @Override
    protected void configure() {
        bind(DefaultExamplesFactory.class).to(ExamplesFactory.class).in(Singleton.class);
    }
}
