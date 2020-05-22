package org.thp.scalligraph.models

import gremlin.scala._
import org.specs2.specification.core.Fragments
import play.api.test.PlaySpecification
import org.thp.scalligraph.{RichSeq, VertexEntity}
import org.thp.scalligraph.auth.{AuthContext, AuthContextImpl}
import play.api.{Configuration, Environment}
import play.api.libs.logback.LogbackLoggerConfigurator

import scala.util.{Success, Try}

class TimingStats {
  private var iteration: List[Long] = Nil
  def time[A](body: => A): A = {
    val start = System.currentTimeMillis()
    val a     = body
    val end   = System.currentTimeMillis()
    iteration = (end - start) :: iteration
    a
  }

  override def toString: String = {
    val count  = iteration.length
    val sum    = iteration.sum
    val mean   = sum.toDouble / count
    val stddev = Math.sqrt(iteration.foldLeft(0d)((acc, i) => acc + Math.pow(i.toDouble - mean, 2)) / count)
    s"Timing: $count iterations, mean: ${mean}ms, stddev: $stddev"
  }
}

@DefineIndex(IndexType.basic, "name")
@VertexEntity
case class EntityWithName(name: String, value: Int)

class PerformanceTest extends PlaySpecification {
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)
  val authContext: AuthContext = AuthContextImpl("me", "", "", "", Set.empty)
  sequential

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    "unique index" in {
      implicit val db: Database                     = dbProvider.get()
      val model: Model.Vertex[EntityWithUniqueName] = Model.vertex[EntityWithUniqueName]

      def getOrCreate(entity: EntityWithUniqueName)(implicit graph: Graph): EntityWithUniqueName with Entity =
        db.labelFilter("EntityWithUniqueName")(graph.V)
          .has(Key("name") -> entity.name)
          .headOption()
          .fold(db.createVertex(graph, authContext, model, entity))(vertex => model.toDomain(vertex))

      db.createSchema(model)
      val createTiming = new TimingStats
      (1 to 1000).toTry { i =>
        createTiming.time {
          db.tryTransaction { implicit graph =>
            Try(getOrCreate(EntityWithUniqueName(i.toString, i)))
          }
        }
      }
      val getTiming = new TimingStats
      (1 to 1000).toTry { i =>
        getTiming.time {
          db.tryTransaction { implicit graph =>
            Success(getOrCreate(EntityWithUniqueName(i.toString, i + 1)).value must_== i)
          }
        }
      }
      println(s"[unique index] creation: $createTiming")
      println(s"[unique index] retrieve: $getTiming")
      ok
    }

    "basic index" in {
      implicit val db: Database               = dbProvider.get()
      val model: Model.Vertex[EntityWithName] = Model.vertex[EntityWithName]

      def getOrCreate(entity: EntityWithName)(implicit graph: Graph): EntityWithName with Entity =
//        db.labelFilter("EntityWithName")(graph.V)
//          .has(Key("name") -> entity.name)
//          .headOption()
//          .fold(
        db.createVertex(graph, authContext, model, entity)
//          )(vertex => model.toDomain(vertex))

      db.createSchema(model)
      val createTiming = new TimingStats
      (1 to 1000).toTry { i =>
        createTiming.time {
          db.tryTransaction { implicit graph =>
            Try(getOrCreate(EntityWithName(i.toString, i)))
          }
        }
      }
      val getTiming = new TimingStats
      (990 to 1100).toTry { i =>
        getTiming.time {
          db.tryTransaction { implicit graph =>
            Success(getOrCreate(EntityWithName(i.toString, i + 1)).value)
          }
        }
      }
      val findDuplicatedTiming = new TimingStats
      findDuplicatedTiming.time {
        db.tryTransaction { implicit graph =>
          val x = db
            .labelFilter("EntityWithName")(graph.V)
            .groupCount(By(Key[String]("name")))
            .unfold[java.util.Map.Entry[String, Long]]()
            .where(x => x.selectValues.is(P.gt(1L)))
            .toList

          Success(x)
        }
      }
      println(s"[basic index] creation: $createTiming")
      println(s"[basic index] retrieve: $getTiming")
      println(s"[basic index] find duplicated: $findDuplicatedTiming")
      ok
    }

  }

}
