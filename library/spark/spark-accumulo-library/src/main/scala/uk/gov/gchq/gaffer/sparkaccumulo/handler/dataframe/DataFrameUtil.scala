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

package uk.gov.gchq.gaffer.sparkaccumulo.handler.dataframe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Utility class for manipulating [[org.apache.spark.sql.DataFrame]]s using the Scala API.
  */
object DataFrameUtil {

  /**
    * Use pattern matching to fill a column with the corresponding value (if it
    * exists), otherwise null.
    *
    * @param cols    the columns to fill in
    * @param allCols a set containing all columns of interest
    * @return a list containing the filled columns
    */
  private def expr(cols: Set[String], allCols: Set[String]) = {
    allCols.toList.map {
      case x if cols.contains(x) => col(x)
      case x => lit(null).as(x)
    }
  }

  /**
    * Carry out a union of two [[org.apache.spark.sql.DataFrame]]s where the input
    * dataframes may contain a different number of columns.
    *
    * The resulting dataframe will contain entries for all of the columns found in
    * the input dataframes, with null entries used as placeholders.
    *
    * @param ds1 the first dataframe
    * @param ds2 the second dataframe
    * @return the combined dataframe
    */
  def union(ds1: DataFrame, ds2: DataFrame): DataFrame = {
    val ds1Cols = ds1.columns.toSet
    val ds2Cols = ds2.columns.toSet
    val total = ds1Cols ++ ds2Cols

    ds1.select(expr(ds1Cols, total): _*).union(ds2.select(expr(ds2Cols, total): _*))
  }
}
