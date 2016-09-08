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

package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.common.util.ExprUtil
import org.apache.spark.sql.common.util.ExprUtil.{SimplifyCast, SimplifyNotNullFil}
import org.sparklinedata.druid.DruidQueryBuilder
import org.sparklinedata.druid.metadata.DruidDimension


object DruidExprUtil {

  /**
    * Simplify given predicates
    *
    * @param dqb  DruidQueryBuilder
    * @param fils Predicates
    * @return
    */
  def simplifyPreds(dqb: DruidQueryBuilder, fils: Seq[Expression]): Seq[Expression] =
    fils.foldLeft[Seq[Expression]](List[Expression]()) { (l, f) =>
      var tpl = l
      for (tp <- simplifyPred(dqb, f))
        tpl = l :+ tp
      tpl
    }

  /**
    * Simplify given Predicate. Does rewrites for cast, Conj/Disj, not null expressions.
    *
    * @param dqb DruidQueryBuilder
    * @param fil Predicate
    * @return
    */
  def simplifyPred(dqb: DruidQueryBuilder, fil: Expression): Option[Expression] = fil match {
    case And(le, re) => simplifyBinaryPred(dqb, le, re, true)
    case Or(le, re) => simplifyBinaryPred(dqb, le, re, false)
    case SimplifyCast(e) => simplifyPred(dqb, e)
    case e@_ => e match {
      case SimplifyNotNullFil(se) =>
        var nullFil: Option[Expression] = Some(se)
        if (se.nullable) {
          if (ExprUtil.nullPreserving(se)) {
            val v1 = nullableAttributes(dqb, se.references)
            if (v1._2.isEmpty) {
              if (v1._1.nonEmpty) {
                nullFil = Some((v1._1).foldLeft[Expression](null)((es, ar) =>
                  if (es != null) {
                    And(es, IsNotNull(ar.asInstanceOf[Expression]))
                  } else {
                    IsNotNull(ar.asInstanceOf[Expression])
                  }))
              } else {
                nullFil = None
              }
            }
          }
        } else {
          nullFil = None
        }
        nullFil
      case _ => Some(e)
    }
  }

  /**
    * Simplify Binary Predicate
    *
    * @param dqb  DruidQueryBuilder
    * @param c1   First child (Left)
    * @param c2   Second child (Right)
    * @param conj If true this is a conjuction else disjunction
    * @return
    */
  def simplifyBinaryPred(dqb: DruidQueryBuilder, c1: Expression, c2: Expression, conj: Boolean):
  Option[Expression] = {
    var newFil: Option[Expression] = None
    val newLe = simplifyPred(dqb, c1)
    val newRe = simplifyPred(dqb, c2)
    if (newLe.nonEmpty) {
      newFil = newLe
    }
    if (newRe.nonEmpty) {
      if (newLe.nonEmpty) {
        if (conj) {
          newFil = Some(And(newLe.get, newRe.get))
        } else {
          newFil = Some(Or(newLe.get, newRe.get))
        }
      } else {
        newFil = newRe
      }
    }
    newFil
  }

  /**
    * Split the given AttributeSet to nullable and unresolvable.
    *
    * @param dqb   DruidQueryBuilder
    * @param attrs AttributeSet
    * @return
    */
  def nullableAttributes(dqb: DruidQueryBuilder, attrs: AttributeSet):
  (List[AttributeReference], List[Attribute]) =
    attrs.foldLeft((List[AttributeReference](), List[Attribute]())) { (s, a) =>
      var s1 = s._1
      var s2 = s._2
      val c = dqb.druidColumn(a.name)
      if (c.nonEmpty) {
        c.get match {
          case DruidDimension(_, _, _, _) if a.isInstanceOf[AttributeReference] =>
            s1 = s1 :+ a.asInstanceOf[AttributeReference]
          case _ => None
        }
      } else {
        s2 = (s2 :+ a)
      }
      (s1, s2)
    }
}