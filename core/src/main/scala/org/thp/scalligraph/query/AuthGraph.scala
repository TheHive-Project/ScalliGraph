package org.thp.scalligraph.query
import gremlin.scala.Graph
import org.thp.scalligraph.auth.AuthContext

case class AuthGraph(auth: Option[AuthContext], graph: Graph) {
  def getAuthContext: AuthContext = auth.getOrElse(sys.error("auth error"))
}
