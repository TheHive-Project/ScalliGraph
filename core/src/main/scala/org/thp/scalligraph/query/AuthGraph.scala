package org.thp.scalligraph.query

import gremlin.scala.Graph
import org.thp.scalligraph.auth.AuthContext

case class AuthGraph(auth: AuthContext, graph: Graph)
