package org.thp.scalligraph.arangodb
import scala.collection.JavaConverters._
import scala.util.Random

import play.api.Configuration

import com.arangodb.tinkerpop.gremlin.client.ArangoDBGraphClient
import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph
import com.typesafe.config.ConfigFactory
import gremlin.scala.Graph
import javax.inject.Singleton
import org.apache.commons.configuration.ConfigurationConverter
import org.thp.scalligraph.Config
import org.thp.scalligraph.models.{ BaseDatabase, EdgeModel, Model, VertexModel }

object ArangoDatabase {
  def randomName = new String(Array.fill(10)(('A'+Random.nextInt(26)).toChar))
  val defaultConfiguration = Configuration(ConfigFactory.parseString(s"""
     |gremlin.arangodb.conf.graph.db: scalligraph
     |gremlin.arangodb.conf.graph.name: $randomName
     |gremlin.arangodb.conf.arangodb.password: gremlin
     |gremlin.arangodb.conf.arangodb.user: gremlin
     |gremlin.arangodb.conf.arangodb.hosts: "127.0.0.1:8529"
     |gremlin.arangodb.conf.graph.vertex: vertex
     |gremlin.arangodb.conf.graph.edge: edge
     |gremlin.arangodb.conf.graph.relation: []
     |#gremlin.arangodb.conf.graph.timeout:
     |#gremlin.arangodb.conf.graph.usessl:
     |#gremlin.arangodb.conf.graph.chunksize:
     |#gremlin.arangodb.conf.graph.connections.max:
     |#gremlin.arangodb.conf.graph.connections.ttl:
     |#gremlin.arangodb.conf.graph.acquireHostList:
     |#gremlin.arangodb.conf.graph.loadBalancingStrategy:
     |#gremlin.arangodb.conf.graph.protocol:
   """.stripMargin))
}

@Singleton
class ArangoDatabase(configuration: Configuration) extends BaseDatabase {
  import ArangoDatabase._

  val graphName: String = configuration.getOptional[String]("gremlin.arangodb.conf.graph.name").getOrElse(defaultConfiguration.get[String]("gremlin.arangodb.conf.graph.name"))
  val arangoConfig      = new Config(defaultConfiguration ++ configuration)
  private var graph     = new ArangoDBGraph(arangoConfig)

  override def createSchema(models: Seq[Model]): Unit = {
 try {
    drop()
    val vertexNames = models.collect {
      case vm: VertexModel ⇒ vm.label
    }
    val edgeNames = models.collect {
      case em: EdgeModel[_, _] ⇒ em.label
    }
    val relations = for {
      em        ← models.collect { case em: EdgeModel[_, _] ⇒ em }
      fromLabel ← if (em.fromLabel.isEmpty) vertexNames else Seq(em.fromLabel)
      toLabel   ← if (em.toLabel.isEmpty) vertexNames else Seq(em.toLabel)
      edgeLabelPrefix = if (em.fromLabel.isEmpty) s"$fromLabel-" else ""
      edgeLabelSuffix = if (em.toLabel.isEmpty) s"-$toLabel" else ""
    } yield s"$edgeLabelPrefix${em.label}$edgeLabelSuffix:$fromLabel->$toLabel"

    val schemaConfig = Configuration.from(
      Map(
        "gremlin.arangodb.conf.graph.vertex"   → vertexNames,
        "gremlin.arangodb.conf.graph.edge"     → edgeNames,
        "gremlin.arangodb.conf.graph.relation" → relations))
    logger.error(s"Create new graph with config: $schemaConfig")
    graph = new ArangoDBGraph(new Config(defaultConfiguration ++ configuration ++ schemaConfig))
    logger.debug(
      s"graph contains:\n - vertices: ${graph.vertexCollections().asScala.mkString}\n - edges: ${graph.edgeCollections().asScala.mkString}")
 } catch {
   case t: Throwable => logger.error("***ERROR***", t)
   throw t
 }
  }
  override def noTransaction[A](body: Graph ⇒ A): A = body(graph)
  override def transaction[A](body: Graph ⇒ A): A   = noTransaction(body)
  override def drop(): Unit                         = {
    val properties = ConfigurationConverter.getProperties(arangoConfig.subset("gremlin.arangodb.conf"))
    val client     = new ArangoDBGraphClient(properties, "scalligraph", 30000)
    logger.info(s"Delete graph $graphName")
    client.deleteGraph(graphName)
  }
}
