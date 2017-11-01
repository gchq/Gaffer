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

package uk.gov.gchq.gaffer.federatedstore;

import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.store.StoreProperties;

import java.io.InputStream;
import java.nio.file.Path;


/**
 * Additional {@link StoreProperties} for the {@link FederatedStore}.
 */
public class FederatedStoreProperties extends StoreProperties {
    /**
     * This is used....
     * e.g gaffer.federatedstore.isPublicAllowed=true
     */
    public static final String IS_PUBLIC_ACCESS_ALLOWED = "gaffer.federatedstore.isPublicAllowed";
    public static final String IS_PUBLIC_ACCESS_ALLOWED_DEFAULT = String.valueOf(true);
    /**
     * This is used....
     * e.g gaffer.federatedstore.customPropertiesAuths="auth1"
     */
    public static final String CUSTOM_PROPERTIES_AUTHS = "gaffer.federatedstore.customPropertiesAuths";
    public static final String CUSTOM_PROPERTIES_AUTHS_DEFAULT = null;

    /**
     * This is used....
     * eg.gaffer.federatedstore.cache.service.class="uk.gov.gchq.gaffer.cache.impl.HashMapCacheService"
     */
    public static final String CACHE_SERVICE_CLASS = CacheProperties.CACHE_SERVICE_CLASS;
    public static final String CACHE_SERVICE_CLASS_DEFAULT = null;

    public FederatedStoreProperties() {
        super(FederatedStore.class);
    }

    public static FederatedStoreProperties loadStoreProperties(final String pathStr) {
        return StoreProperties.loadStoreProperties(pathStr, FederatedStoreProperties.class);
    }

    public static FederatedStoreProperties loadStoreProperties(final InputStream storePropertiesStream) {
        return StoreProperties.loadStoreProperties(storePropertiesStream, FederatedStoreProperties.class);
    }

    public static FederatedStoreProperties loadStoreProperties(final Path storePropertiesPath) {
        return StoreProperties.loadStoreProperties(storePropertiesPath, FederatedStoreProperties.class);
    }

    public void setCustomPropertyAuths(final String auths) {
        set(CUSTOM_PROPERTIES_AUTHS, auths);
    }

    public void setCacheProperties(final String cacheServiceClassString) {
        set(CACHE_SERVICE_CLASS, cacheServiceClassString);
    }

    public String getCacheProperties() {
        return get(CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_DEFAULT);
    }

    public String getCustomPropsValue() {
        return this.get(CUSTOM_PROPERTIES_AUTHS, CUSTOM_PROPERTIES_AUTHS_DEFAULT);
    }

    public String getIsPublicAccessAllowed() {
        return get(IS_PUBLIC_ACCESS_ALLOWED, IS_PUBLIC_ACCESS_ALLOWED_DEFAULT);
    }

    public void setFalseGraphsCanHavePublicAccess() {
        setGraphsCanHavePublicAccess(false);
    }

    public void setTrueGraphsCanHavePublicAccess() {
        setGraphsCanHavePublicAccess(true);
    }

    public void setGraphsCanHavePublicAccess(final boolean b) {
        set(IS_PUBLIC_ACCESS_ALLOWED, Boolean.toString(b));
    }
}
