package org.thp.scalligraph.janus

import org.apache.tinkerpop.gremlin.structure.{Vertex, VertexProperty}
import org.thp.scalligraph.BadConfigurationError
import play.api.Logger

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait ImmenseTermProcessor {
  lazy val logger: Logger = Logger(getClass)
  def apply[V](vertex: Vertex, property: VertexProperty[V]): Boolean
}

object ImmenseTermProcessor {
  val defaultThreshold: Int              = 8191
  lazy val logger: Logger                = Logger(getClass)
  private val immenseTermProcessorRegex1 = """(\p{Alpha}+)""".r
  private val immenseTermProcessorRegex2 = """(\p{Alpha}+)\((.*)\)""".r
  private var registeredStrategies: Map[String, Seq[String] => ImmenseTermProcessor] =
    Map(
      "truncate" -> ((params: Seq[String]) => new TruncateField(params)),
      "delete"   -> ((params: Seq[String]) => new DeleteVertex(params))
    )

  def registerStrategy(name: String, factory: Seq[String] => ImmenseTermProcessor): Unit = {
    registeredStrategies += name -> factory
    ()
  }

  def parseStrategy(s: String): Option[ImmenseTermProcessor] =
    Option(s)
      .flatMap {
        case immenseTermProcessorRegex1(name)         => Some((name, Nil))
        case immenseTermProcessorRegex2(name, params) => Some((name, params.split(',').toList.map(_.trim).filterNot(_.isEmpty)))
        case _ =>
          logger.warn(s"Invalid format for immense term strategy: $s")
          None
      }
      .flatMap {
        case (name, params) =>
          registeredStrategies
            .get(name)
            .fold[Try[ImmenseTermProcessor]](Failure(BadConfigurationError(s"Unknown immense term strategy: $name")))(strategy =>
              Try(strategy(params))
            ) match {
            case Success(strategy) => Some(strategy)
            case Failure(error) =>
              logger.warn("Unable to instantiate immense term processor", error)
              None
          }
      }

  def process(db: JanusDatabase, immenseTermConfig: Map[String, String]): Try[Unit] = {
    val strategies = immenseTermConfig.flatMap {
      case (field, strategy) => parseStrategy(strategy).map(field -> _)
    }
    doProcess(db, strategies)
  }

  def doProcess(db: JanusDatabase, strategies: Map[String, ImmenseTermProcessor]): Try[Unit] =
    if (strategies.isEmpty) Success(())
    else
      db.tryTransaction { graph =>
        logger.info(s"Processing immense terms: ${strategies.mkString(",")}")
        graph.VV().raw.asScala.foreach { vertex =>
          strategies
            .view
            .map {
              case (field, strategy) =>
                vertex
                  .properties[Any](field)
                  .asScala
                  .exists(p => strategy.apply(vertex, p)) // true = stop
            }
            .exists(identity) // take while process return false
        }
        Success(())
      }
}

trait ImmenseStringTermFilter {
  val termSizeLimit: Int
  def collect[V](vertex: Vertex, property: VertexProperty[V]): Option[VertexProperty[String]] =
    property.value() match {
      case s: String if s.length > termSizeLimit => Some(property.asInstanceOf[VertexProperty[String]])
      case _                                     => None
    }
}

class TruncateField(val termSizeLimit: Int) extends ImmenseTermProcessor with ImmenseStringTermFilter {
  def this(params: Seq[String]) = this(params.headOption.flatMap(p => Try(p.toInt).toOption).getOrElse(ImmenseTermProcessor.defaultThreshold))
  override def apply[V](vertex: Vertex, property: VertexProperty[V]): Boolean = {
    collect(vertex, property).foreach { strProp =>
      val currentValue = strProp.value()
      logger.info(s"Truncate $vertex/${strProp.key()}: $currentValue")
      strProp.remove()
      vertex.property(strProp.key(), currentValue.substring(0, termSizeLimit))
    }
    false
  }
  override def toString: String = "truncate"
}
class DeleteVertex(val termSizeLimit: Int) extends ImmenseTermProcessor with ImmenseStringTermFilter {
  def this(params: Seq[String]) = this(params.headOption.flatMap(p => Try(p.toInt).toOption).getOrElse(ImmenseTermProcessor.defaultThreshold))
  override def apply[V](vertex: Vertex, property: VertexProperty[V]): Boolean =
    collect(vertex, property).fold(false) { strProp =>
      val currentValue = strProp.value()
      logger.info(s"Remove vertex $vertex, field ${strProp.key()}: $currentValue")
      vertex.remove()
      true
    }
  override def toString: String = "delete"
}
