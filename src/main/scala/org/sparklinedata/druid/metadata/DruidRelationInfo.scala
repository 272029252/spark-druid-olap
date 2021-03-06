/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sparklinedata.druid.metadata

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.sparklinedata.druid.DruidQueryGranularity

import scala.collection.mutable.{Map => MMap}

object NonAggregateQueryHandling extends Enumeration {
  val PUSH_FILTERS = Value("push_filters")
  val PUSH_PROJECT_AND_FILTERS = Value("push_project_and_filters")
  val PUSH_NONE = Value("push_none")

}

case class DruidRelationName(
                            sparkDataSource : String,
                            druidHost : String,
                            druidDataSource : String
                            )
case class DruidRelationInfo(val fullName : DruidRelationName,
                         val sourceDFName : String,
                            val timeDimensionCol : String,
                         val druidDS : DruidDataSource,
                         val sourceToDruidMapping : Map[String, DruidColumn],
                         val fd : FunctionalDependencies,
                            val starSchema : StarSchema,
                         val options : DruidRelationOptions) {

  val host : String = fullName.druidHost

  lazy val dimensionNamesSet = druidDS.dimensions.map(_.name).toSet

  def sourceDF(sqlContext : SQLContext) = sqlContext.table(sourceDFName)

  override def toString : String = {
    s"""DruidRelationInfo(fullName = $fullName, sourceDFName = $sourceDFName,
       |timeDimensionCol = $timeDimensionCol,
       |options = $options)""".stripMargin
  }


}

case class DruidRelationOptions(val maxCardinality : Long,
                                val cardinalityPerDruidQuery : Long,
                                pushHLLTODruid : Boolean,
                                streamDruidQueryResults : Boolean,
                                loadMetadataFromAllSegments : Boolean,
                                zkSessionTimeoutMs : Int,
                                zkEnableCompression : Boolean,
                                zkDruidPath : String,
                                queryHistoricalServers : Boolean,
                                zkQualifyDiscoveryNames : Boolean,
                                numSegmentsPerHistoricalQuery : Int,
                                useSmile : Boolean,
                                nonAggQueryHandling : NonAggregateQueryHandling.Value,
                                queryGranularity: DruidQueryGranularity,
                                allowTopN : Boolean,
                                topNMaxThreshold : Int,
                                numProcessingThreadsPerHistorical : Option[Int] = None
                                ) {

  def sqlContextOption(nm : String) = s"spark.sparklinedata.druid.option.$nm"

  def numSegmentsPerHistoricalQuery(sqlContext : SQLContext) : Int = {
    sqlContext.getConf(
      sqlContextOption("numSegmentsPerHistoricalQuery"),
        numSegmentsPerHistoricalQuery.toString
    ).toInt
  }

  def queryHistoricalServers(sqlContext : SQLContext) : Boolean = {
    sqlContext.getConf(
      sqlContextOption("queryHistoricalServers"),
      queryHistoricalServers.toString
    ).toBoolean
  }

  def useSmile(sqlContext : SQLContext) : Boolean = {
    sqlContext.getConf(
      sqlContextOption("useSmile"),
      useSmile.toString
    ).toBoolean
  }

  def allowTopN(sqlContext : SQLContext) : Boolean = {
    sqlContext.getConf(
      sqlContextOption("allowTopN"),
      allowTopN.toString
    ).toBoolean
  }

  def topNMaxThreshold(sqlContext : SQLContext) : Int = {
    sqlContext.getConf(
      sqlContextOption("topNMaxThreshold"),
      topNMaxThreshold.toString
    ).toInt
  }

}

private[druid] object MappingBuilder extends Logging {

  /**
   * Only top level Numeric and String Types are mapped.
    *
    * @param dT
   * @return
   */
  def supportedDataType(dT : DataType) : Boolean = dT match {
    case t if t.isInstanceOf[NumericType] => true
    case StringType => true
    case DateType => true
    case TimestampType => true
    case BooleanType => true
    case _ => false
  }

  def buildMapping(sqlContext : SQLContext,
                  sourceDFName : String,
                    starSchema : StarSchema,
                   nameMapping : Map[String, String],timeDimensionCol : String,
                   druidDS : DruidDataSource) : Map[String, DruidColumn] = {

    val m = MMap[String, DruidColumn]()

    starSchema.tableMap.values.foreach { t =>
      val tName = if ( t.name == starSchema.factTable.name) sourceDFName else t.name
      val df = sqlContext.table(tName)
      df.schema.iterator.foreach { f =>
        if (supportedDataType(f.dataType)) {
          val dCol = druidDS.columns.get(nameMapping.getOrElse(f.name, f.name))
          if (f.name == timeDimensionCol) {
            m += (f.name -> druidDS.timeDimension.get)
          } else if (dCol.isDefined) {
            m += (f.name -> dCol.get)
          }
        } else {
          logDebug(s"${f.name} not mapped to Druid dataSource, unsupported dataType")
        }
      }
    }

    m.toMap
  }
}
