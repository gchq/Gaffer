/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.script.operation.builder;

import uk.gov.gchq.gaffer.script.operation.image.Image;

import java.util.Map;

public interface ImageBuilder {

    /**
     * Builds an image which runs a script
     *
     * @param scriptName             the name of the script being run
     * @param scriptParameters       the parameters of the script being run
     * @param pathToBuildFiles       the path to the directory containing any build files
     * @return the image
     */
    Image buildImage(final String scriptName, final Map<String, Object> scriptParameters,
                     final String pathToBuildFiles) throws Exception;
}
