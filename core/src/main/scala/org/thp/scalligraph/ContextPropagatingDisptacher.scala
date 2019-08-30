package org.thp.scalligraph

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}

import play.api.mvc.RequestHeader

import akka.dispatch._
import com.typesafe.config.Config
import org.slf4j.MDC

/**
  * Configurator for a context propagating dispatcher.
  */
class ContextPropagatingDispatcherConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
    extends MessageDispatcherConfigurator(config, prerequisites) {

  private val instance = new ContextPropagatingDisptacher(
    this,
    config.getString("id"),
    config.getInt("throughput"),
    FiniteDuration(config.getDuration("throughput-deadline-time", TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS),
    configureExecutor(),
    FiniteDuration(config.getDuration("shutdown-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  )

  override def dispatcher(): MessageDispatcher = instance
}

/**
  * A context propagating dispatcher.
  *
  * This dispatcher propagates the current request context if it's set when it's executed.
  */
class ContextPropagatingDisptacher(
    _configurator: MessageDispatcherConfigurator,
    id: String,
    throughput: Int,
    throughputDeadlineTime: Duration,
    executorServiceFactoryProvider: ExecutorServiceFactoryProvider,
    shutdownTimeout: FiniteDuration
) extends Dispatcher(
      _configurator,
      id,
      throughput,
      throughputDeadlineTime,
      executorServiceFactoryProvider,
      shutdownTimeout
    ) { self =>

  override def prepare(): ExecutionContext = new ExecutionContext {
    // capture the context
    val context: CapturedRequestContext = RequestContext.capture()

    def execute(r: Runnable): Unit        = self.execute(() => context.withContext(r.run()))
    def reportFailure(t: Throwable): Unit = self.reportFailure(t)
  }
}

/**
  * The current request context.
  */
object RequestContext {
  private val request = new ThreadLocal[RequestHeader]()

  private def setRequest(rh: RequestHeader): Unit = {
    request.set(rh)
    MDC.put("request", f"${rh.id}%08x")
  }

  private def clear(): Unit = {
    request.remove()
    MDC.remove("request")
  }

  /**
    * Capture the current request context.
    */
  def capture(): CapturedRequestContext = new CapturedRequestContext {
    val maybeRequest: Option[RequestHeader] = getRequest

    def withContext[T](block: => T): Unit = {
      maybeRequest match {
        case Some(rh) => withRequest(rh)(block)
        case None     => block
      }
      ()
    }
  }

  /**
    * Get the current request ID.
    */
  def getRequest: Option[RequestHeader] = Option(request.get())

  /**
    * Execute the given block with the given request id.
    */
  def withRequest[T](rh: RequestHeader)(block: => T): T = {
    assert(rh != null, "RequestHeader must not be null")

    val maybeOld = getRequest
    try {
      setRequest(rh)
      block
    } finally {
      maybeOld match {
        case Some(old) => setRequest(old)
        case None      => clear()
      }
    }
  }
}

/**
  * A captured request context
  */
trait CapturedRequestContext {

  /**
    * Execute the given block with the captured request context.
    */
  def withContext[T](block: => T)
}
