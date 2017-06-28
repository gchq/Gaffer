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

package uk.gov.gchq.gaffer.commonutil;

import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ToStringBuilder extends org.apache.commons.lang3.builder.ToStringBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ToStringBuilder.class);

    public ToStringBuilder(final Object object) {
        super(object);
        if (!DebugUtil.checkDebugMode()) {
            LOGGER.debug("Debug mode set to false, short prefix toString will be used.");
            setDefaultStyle(ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }
}
