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

package gaffer.rest.application;

import com.wordnik.swagger.jaxrs.JaxrsApiReader;
import com.wordnik.swagger.jaxrs.listing.ApiDeclarationProvider;
import com.wordnik.swagger.jaxrs.listing.ApiListingResource;
import com.wordnik.swagger.jaxrs.listing.ApiListingResourceJSON;
import com.wordnik.swagger.jaxrs.listing.ResourceListingProvider;
import gaffer.rest.service.SimpleExamplesService;
import gaffer.rest.service.SimpleGraphConfigurationService;
import gaffer.rest.service.SimpleOperationService;
import gaffer.rest.service.StatusService;

import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

/**
 * An <code>AbstractApplicationConfig</code> sets up the application resources and singletons.
 * To use this REST API this class should be extended and registered using the class annotation
 * ApplicationPath("your application path")
 */
public abstract class AbstractApplicationConfig extends Application {
    protected final Set<Object> singletons = new HashSet<>();
    protected final Set<Class<?>> resources = new HashSet<>();

    public AbstractApplicationConfig() {
        addSystemResources();
        addServices();
    }

    protected void addServices() {
        resources.add(StatusService.class);
        resources.add(SimpleOperationService.class);
        resources.add(SimpleGraphConfigurationService.class);
        resources.add(SimpleExamplesService.class);
    }

    protected void addSystemResources() {
        resources.add(JaxrsApiReader.class);
        resources.add(ApiListingResource.class);
        resources.add(ApiDeclarationProvider.class);
        resources.add(ApiListingResourceJSON.class);
        resources.add(ResourceListingProvider.class);
        resources.add(ApiListingResourceJSON.class);
    }

    @Override
    public Set<Class<?>> getClasses() {
        return resources;
    }

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }
}
