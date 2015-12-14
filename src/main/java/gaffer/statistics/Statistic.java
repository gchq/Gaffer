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
package gaffer.statistics;

import org.apache.hadoop.io.Writable;

import java.io.Serializable;

/**
 * An interface to define methods that a {@link Statistic} must implement.
 * Only two methods are required: <code>merge()</code> which merges one
 * {@link Statistic} into another (this should throw an {@link IllegalArgumentException}
 * if an attempt is made to merge statistics of two different types together);
 * <code>clone()</code> which should perform a deep clone.
 */
public interface Statistic extends Writable, Serializable {

	/**
	 * Merges another {@link Statistic} into this one.
	 * 
	 * @param s  the {@link Statistic} to be merged in.
	 * @throws IllegalArgumentException if s is of a different type
	 */
	void merge(Statistic s) throws IllegalArgumentException;

	/**
	 * Performs a deep clone of the {@link Statistic}.
	 * 
	 * @return returns a deep clone of the {@link Statistic}
	 */
	Statistic clone();
	
}
