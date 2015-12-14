/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.predicate;

import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Used to indicate whether an item of type <code>T</code> is acceptable or not.
 *
 * @param <T>
 */
public interface Predicate<T> extends Writable, Serializable {

    boolean accept(T t) throws IOException;
}
