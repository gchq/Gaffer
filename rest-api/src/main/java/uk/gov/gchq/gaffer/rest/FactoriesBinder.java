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
import uk.gov.gchq.gaffer.rest.factory.DefaultGraphFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UnknownUserFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;

public class FactoriesBinder extends AbstractBinder {
    @Override
    protected void configure() {
        bind(DefaultGraphFactory.class).to(GraphFactory.class);
        bind(UnknownUserFactory.class).to(UserFactory.class);
    }
}
