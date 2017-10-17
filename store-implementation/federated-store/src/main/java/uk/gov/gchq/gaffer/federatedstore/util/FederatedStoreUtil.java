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
package uk.gov.gchq.gaffer.federatedstore.util;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

public final class FederatedStoreUtil {
    private static final String SCHEMA_DEL_REGEX = Pattern.quote(",");

    private FederatedStoreUtil() {
    }

    public static List<String> getGraphIds(final Map<String, String> config) {
        if (null == config) {
            return null;
        }

        return getCleanStrings(config.get(KEY_OPERATION_OPTIONS_GRAPH_IDS));
    }

    public static List<String> getCleanStrings(final String value) {
        final List<String> values;
        if (value != null) {
            values = Lists.newArrayList(StringUtils.stripAll(value.split(SCHEMA_DEL_REGEX)));
            values.remove("");
            values.remove(null);
        } else {
            values = null;
        }
        return values;
    }
}
