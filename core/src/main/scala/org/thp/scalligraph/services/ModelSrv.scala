package org.thp.scalligraph.services

import scala.collection.immutable

import play.api.Logger

import javax.inject.{Inject, Provider, Singleton}
import org.thp.scalligraph.models.Model

@Singleton
class ModelSrv @Inject() (modelsProvider: Provider[immutable.Set[Model]]) {
  private[ModelSrv] lazy val logger = Logger(getClass)

  lazy val models: Set[Model]                 = modelsProvider.get
  private[ModelSrv] lazy val modelMap         = models.map(m => m.label -> m).toMap
  def apply(modelName: String): Option[Model] = modelMap.get(modelName)
  val list: Set[Model]                        = models
}
