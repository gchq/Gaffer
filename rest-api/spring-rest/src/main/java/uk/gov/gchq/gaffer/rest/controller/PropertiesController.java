/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import uk.gov.gchq.gaffer.rest.PropertiesUtil;

import java.util.Map;

import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;

@RestController
public class PropertiesController implements IPropertiesController {

    @Override
    public ResponseEntity<Map<String, String>> getProperties() {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(PropertiesUtil.getProperties());
    }

    @Override
    public ResponseEntity<String> getProperty(@PathVariable("property") final String property) {
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(PropertiesUtil.getProperty(property));
    }
}
