package org.thp.scalligraph.models

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe ⇒ ru}

import play.api.Logger

import gremlin.scala.Graph
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import org.reflections.util.ConfigurationBuilder
import org.thp.scalligraph.auth.AuthContext

case class InitialValue[V <: Product](model: Model.Vertex[V], value: V) {

  def create()(implicit db: Database, graph: Graph, authContext: AuthContext): V with Entity =
    db.createVertex[V](graph, authContext, model, value)
}

trait Schema {
  def modelList: Seq[Model]
  def initialValues: Seq[InitialValue[_]]
  def getModel(label: String): Option[Model]                      = modelList.find(_.label == label)
  def init(implicit graph: Graph, authContext: AuthContext): Unit = ()
}

class ReflectionSchema(classLoader: ClassLoader, packages: String*) extends Schema {

  lazy val logger   = Logger(getClass)
  val rm: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)

  lazy val reflectionClasses = new Reflections(
    new ConfigurationBuilder()
      .forPackages(packages: _*)
      .addClassLoader(getClass.getClassLoader)
      .setExpandSuperTypes(true)
      .setScanners(new SubTypesScanner(false))
  )

  override lazy val modelList: Seq[Model] =
    reflectionClasses
      .getSubTypesOf(classOf[HasModel[_]])
      .asScala
      .filterNot(c ⇒ java.lang.reflect.Modifier.isAbstract(c.getModifiers))
      .map { modelClass ⇒
        val hasModel = rm.reflectModule(rm.classSymbol(modelClass).companion.companion.asModule).instance.asInstanceOf[HasModel[_]]
        logger.info(s"Loading model ${hasModel.model.label}")
        hasModel.model
      }
      .toSeq

  override lazy val initialValues: Seq[InitialValue[_]] = Nil
}
