package org.thp.scalligraph

import scala.reflect.ClassTag

import play.api.inject.guice.GuiceApplicationBuilder
import play.api.libs.concurrent.Akka
import play.api.{Application, Configuration}

import akka.actor.{Actor, ActorRef, Props}
import com.google.inject.name.Names
import com.google.inject.util.Providers
import com.typesafe.config.ConfigFactory
import javax.inject.Provider
import net.codingwell.scalaguice.{ScalaModule, ScalaMultibinder}

class AppBuilder extends ScalaModule {

  private var initialized                  = false
  private var init: Function[Unit, _]      = identity[Unit]
  private var configuration: Configuration = Configuration.empty

  override def configure(): Unit = {
    init(())
    ()
  }

  def addConfiguration(config: Configuration): AppBuilder = {
    configuration = configuration ++ config
    this
  }

  def addConfiguration(config: String): AppBuilder =
    addConfiguration(Configuration(ConfigFactory.parseString(config)))

  def bind[T: Manifest, TImpl <: T: Manifest]: AppBuilder = {
    if (initialized) throw InternalError("Bind is not permitted after app use")
    init = init.andThen(_ => bind[T].to[TImpl])
    this
  }

  def multiBind[T: Manifest](implementations: Class[_ <: T]*): AppBuilder = {
    if (initialized) throw InternalError("Bind is not permitted after app use")
    init = init.andThen { _ =>
      val multiBindings = ScalaMultibinder.newSetBinder[T](binder)
      implementations.foreach(i => multiBindings.addBinding.to(i))
    }
    this
  }

  def bindInstance[T: Manifest](instance: T): AppBuilder = {
    if (initialized) throw InternalError("Bind is not permitted after app use")
    init = init.andThen(_ => bind[T].toInstance(instance))
    this
  }

  def bindEagerly[T: Manifest]: AppBuilder = {
    if (initialized) throw InternalError("Bind is not permitted after app use")
    init = init.andThen(_ => bind[T].asEagerSingleton())
    this
  }

  def bindToProvider[T: Manifest](provider: Provider[T]): AppBuilder = {
    if (initialized) throw InternalError("Bind is not permitted after app use")
    init = init.andThen(_ => bind[T].toProvider(provider))
    this
  }

  def bindToProvider[T: Manifest, TImpl <: Provider[T]: Manifest]: AppBuilder = {
    if (initialized) throw InternalError("Bind is not permitted after app use")
    init = init.andThen(_ => bind[T].toProvider[TImpl])
    this
  }

  def bindActor[T <: Actor: ClassTag](name: String, props: Props => Props = identity): AppBuilder = {
    if (initialized) throw InternalError("Bind is not permitted after app use")
    init = init.andThen { _ =>
      bind(classOf[ActorRef])
        .annotatedWith(Names.named(name))
        .toProvider(Providers.guicify(Akka.providerOf[T](name, props)))
        .asEagerSingleton()
    }
    this
  }

  lazy val app: Application = {
    initialized = true
    GuiceApplicationBuilder(modules = Seq(this), configuration = configuration).build()
  }

  def instanceOf[T: ClassTag]: T = app.injector.instanceOf[T]
}

object AppBuilder {
  def apply() = new AppBuilder
}
