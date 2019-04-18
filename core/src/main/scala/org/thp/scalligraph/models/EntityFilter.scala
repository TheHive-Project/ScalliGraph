package org.thp.scalligraph.models

import gremlin.scala.GremlinScala
import org.thp.scalligraph.auth.AuthContext

abstract class EntityFilter { thisEf ⇒
  def apply[G](authContext: AuthContext)(raw: GremlinScala[G]): GremlinScala[G]
  def apply[S <: ScalliSteps[_, _, _]](authContext: AuthContext, step: S): S = step match {
    case s: ScalliSteps[_, e, _] ⇒ s.newInstance(apply[e](authContext)(s.raw)).asInstanceOf[S]
  }

  def and(ef: EntityFilter): EntityFilter = new EntityFilter {
    override def apply[G](authContext: AuthContext)(raw: GremlinScala[G]): GremlinScala[G] =
      raw.and(ef.apply(authContext), thisEf(authContext))
  }
}
