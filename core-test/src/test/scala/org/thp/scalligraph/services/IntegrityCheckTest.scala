package org.thp.scalligraph.services

import org.apache.tinkerpop.gremlin.structure.Vertex
import org.specs2.specification.core.Fragments
import org.thp.scalligraph.auth.{AuthContext, AuthContextImpl}
import org.thp.scalligraph.models.ModernOps._
import org.thp.scalligraph.models._
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, Graph, ProjectionBuilder, Traversal}
import org.thp.scalligraph.{AppBuilder, EntityId, EntityName}
import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.test.PlaySpecification
import play.api.{Configuration, Environment}

import scala.concurrent.duration.DurationInt
import scala.util.{Success, Try}

class IntegrityCheckTest extends PlaySpecification {
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)
  implicit val authContext: AuthContext = AuthContextImpl("me", "", EntityName(""), "", Set.empty)

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] integrity check" should {
      "copy edges vertex" in {
        val app: AppBuilder = new AppBuilder()
          .bindToProvider(dbProvider)
        implicit val db: Database = app[Database]
        val personSrv = app[PersonSrv]
        val softwareSrv = app[SoftwareSrv]
        val createdSrv = new EdgeSrv[Created, Person, Software]
        ModernDatabaseBuilder.build(app[ModernSchema])(app[Database], authContext).get
        val newLop = db.tryTransaction { implicit graph =>
          val lop = softwareSrv.create(Software("lop", "asm")).get
          val vadas = personSrv.getByName("vadas").head
          createdSrv
            .create(Created(0.1), vadas, lop)
            .map(_ => lop)
        }.get

        val dedupCheck: DedupCheck[Software] = new DedupCheck[Software] {
          override val db: Database = app[Database]
          override val service: SoftwareSrv = app[SoftwareSrv]
        }
        val duplicates = dedupCheck.getDuplicates(Seq("name"), KillSwitch.alwaysOn)
        duplicates must have size 1
        duplicates.head.map(s => s.name -> s.lang) must contain(exactly("lop" -> "java", "lop" -> "asm"))
        db.tryTransaction { implicit graph =>
          EntitySelector.lastCreatedEntity(duplicates.head).foreach {
            case (lastCreated, others) =>
              println(s"copy edge from ${others.map(v => s"$v(${v._id})").mkString(", ")} to $lastCreated(${lastCreated._id})")
              others.foreach(dedupCheck.copyEdge(_, lastCreated))
          }
          Success(())
        }

        db.roTransaction { implicit graph =>
          softwareSrv.get(newLop).createdBy.toSeq.map(_.name) must contain(exactly("vadas", "marko", "josh", "peter"))
        }
      }
    }
  }

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] do nothing if mandatory field id is present (byId)" in {
      val app: AppBuilder = new AppBuilder().bindToProvider(dbProvider)
      MeshDatabaseBuilder.build(app[MeshSchema])(app[Database], authContext).get
      val aSrv = new ASrv
      integrityTest(app)(_.by.by(_.out[BAOne]._id.fold)) { implicit graph =>
        i => {
          case (b, as) => i.singleIdLink[A]("oneA", aSrv)(_.outEdge[BAOne], _.remove).check(b, b.oneA, as)
        }
      } must beEqualTo(Map.empty)
    }
  }

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] remove entity if link has been removed (byId)" in {
      val app: AppBuilder = new AppBuilder().bindToProvider(dbProvider)
      MeshDatabaseBuilder.build(app[MeshSchema])(app[Database], authContext).get
      val aSrv = new ASrv
      app[Database].tryTransaction(implicit graph => Try(aSrv.getByName("a").remove())).get
      integrityTest(app)(_.by.by(_.out[BAOne]._id.fold)) { implicit graph =>
        i => {
          case (b, as) => i.singleIdLink[A]("oneA", aSrv)(_.outEdge[BAOne], _.remove).check(b, b.oneA, as)
        }
      } must beEqualTo(Map("B-oneA-removeOrphan" -> 1))
      val bSrv = new BSrv
      app[Database].roTransaction(implicit graph => bSrv.getByName("b1").getCount) must beEqualTo(0)
    }
  }

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] set empty entity if link has been removed (byId)" in {
      val app: AppBuilder = new AppBuilder().bindToProvider(dbProvider)
      MeshDatabaseBuilder.build(app[MeshSchema])(app[Database], authContext).get
      val aSrv = new ASrv
      app[Database].tryTransaction(implicit graph => Try(aSrv.getByName("a").remove())).get
      integrityTest(app)(_.by.by(_.out[BAOne]._id.fold)) { implicit graph =>
        i => {
          case (b, as) => i.singleIdLink[A]("oneA", aSrv)(_.outEdge[BAOne], _.set(EntityId.empty)).check(b, b.oneA, as)
        }
      } must beEqualTo(Map("B-oneA-setEmptyOrphan" -> 1))
      val bSrv = new BSrv
      app[Database].roTransaction(implicit graph => bSrv.getByName("b1").value(_.oneA).toSeq) must beEqualTo(Seq(EntityId.empty))
    }
  }

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] remove extra links (byId)" in {
      val app: AppBuilder = new AppBuilder().bindToProvider(dbProvider)
      MeshDatabaseBuilder.build(app[MeshSchema])(app[Database], authContext).get
      val aSrv = new ASrv
      val bSrv = new BSrv
      val baOneSrv = new EdgeSrv[BAOne, B, A]
      app[Database].tryTransaction { implicit graph =>
        baOneSrv.create(new BAOne, bSrv.getByName("b1").head, aSrv.getByName("b").head).get
        baOneSrv.create(new BAOne, bSrv.getByName("b1").head, aSrv.getByName("c").head)
      }.get
      integrityTest(app)(_.by.by(_.out[BAOne]._id.fold)) { implicit graph =>
        i => {
          case (b, as) => i.singleIdLink[A]("oneA", aSrv)(_.outEdge[BAOne], _.set(EntityId.empty)).check(b, b.oneA, as)
        }
      } must beEqualTo(Map("B-A-unlink" -> 2))

      app[Database].roTransaction { implicit graph =>
        val aIds = bSrv
          .getByName("b1")
          .value(_.oneA)
          .toSeq
        aSrv.getByIds(aIds: _*).value(_.name).toSeq
      } must beEqualTo(Seq("a"))
    }
  }

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] update field (keep last created) (byId)" in {
      val app: AppBuilder = new AppBuilder().bindToProvider(dbProvider)
      MeshDatabaseBuilder.build(app[MeshSchema])(app[Database], authContext).get
      val aSrv = new ASrv
      val bSrv = new BSrv
      val baOneSrv = new EdgeSrv[BAOne, B, A]
      app[Database].tryTransaction { implicit graph =>
        bSrv.getByName("b1").update(_.oneA, EntityId("foo")).iterate()
        baOneSrv.create(new BAOne, bSrv.getByName("b1").head, aSrv.getByName("b").head).get
        baOneSrv.create(new BAOne, bSrv.getByName("b1").head, aSrv.getByName("c").head)
      }.get
      integrityTest(app)(_.by.by(_.out[BAOne]._id.fold)) { implicit graph =>
        i => {
          case (b, as) =>
            i.singleIdLink[A]("oneA", aSrv)(_.outEdge[BAOne], _.set(EntityId.empty), _ => EntitySelector.lastCreatedEntity).check(b, b.oneA, as)
        }
      } must beEqualTo(Map("B-A-unlink" -> 2, "B-A-setField" -> 1))

      app[Database].roTransaction { implicit graph =>
        val aIds = bSrv
          .getByName("b1")
          .value(_.oneA)
          .toSeq
        aSrv.getByIds(aIds: _*).value(_.name).toSeq
      } must beEqualTo(Seq("c"))
    }
  }

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] update field (keep first created) (byId)" in {
      val app: AppBuilder = new AppBuilder().bindToProvider(dbProvider)
      MeshDatabaseBuilder.build(app[MeshSchema])(app[Database], authContext).get
      val aSrv = new ASrv
      val bSrv = new BSrv
      val baOneSrv = new EdgeSrv[BAOne, B, A]
      app[Database].tryTransaction { implicit graph =>
        bSrv.getByName("b1").update(_.oneA, EntityId("foo")).iterate()
        baOneSrv.create(new BAOne, bSrv.getByName("b1").head, aSrv.getByName("b").head).get
        baOneSrv.create(new BAOne, bSrv.getByName("b1").head, aSrv.getByName("c").head)
      }.get
      integrityTest(app)(_.by.by(_.out[BAOne]._id.fold)) { implicit graph =>
        i => {
          case (b, as) =>
            i.singleIdLink[A]("oneA", aSrv)(_.outEdge[BAOne], _.set(EntityId.empty), _ => EntitySelector.firstCreatedEntity)
              .check(b, b.oneA, as)
        }
      } must beEqualTo(Map("B-A-unlink" -> 2, "B-A-setField" -> 1))

      app[Database].roTransaction { implicit graph =>
        val aIds = bSrv
          .getByName("b1")
          .value(_.oneA)
          .toSeq
        aSrv.getByIds(aIds: _*).value(_.name).toSeq
      } must beEqualTo(Seq("a"))
    }
  }

  /* use name field */
  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] do nothing if mandatory field id is present (byName)" in {
      val app: AppBuilder = new AppBuilder().bindToProvider(dbProvider)
      MeshDatabaseBuilder.build(app[MeshSchema])(app[Database], authContext).get
      val aSrv = new ASrv
      integrityTest(app)(_.by.by(_.out[BAName].v[A].value(_.name).fold)) { implicit graph =>
        i => {
          case (b, as) => i.singleLink[A, String]("aName", aSrv.getByName(_).head, _.name)(_.outEdge[BAName], _.remove).check(b, b.aName, as)
        }
      } must beEqualTo(Map.empty)
    }
  }

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] remove entity if link has been removed (byName)" in {
      val app: AppBuilder = new AppBuilder().bindToProvider(dbProvider)
      MeshDatabaseBuilder.build(app[MeshSchema])(app[Database], authContext).get
      val aSrv = new ASrv
      app[Database].tryTransaction(implicit graph => Try(aSrv.getByName("c").remove())).get
      integrityTest(app)(_.by.by(_.out[BAName].v[A].value(_.name).fold)) { implicit graph =>
        i => {
          case (b, as) => i.singleLink[A, String]("aName", aSrv.getByName(_).head, _.name)(_.outEdge[BAName], _.remove).check(b, b.aName, as)
        }
      } must beEqualTo(Map("B-aName-removeOrphan" -> 1))
      val bSrv = new BSrv
      app[Database].roTransaction(implicit graph => bSrv.getByName("b1").getCount) must beEqualTo(0)
    }
  }

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] remove extra links (byName)" in {
      val app: AppBuilder = new AppBuilder().bindToProvider(dbProvider)
      MeshDatabaseBuilder.build(app[MeshSchema])(app[Database], authContext).get
      val aSrv = new ASrv
      val bSrv = new BSrv
      val baNameSrv = new EdgeSrv[BAName, B, A]
      app[Database].tryTransaction { implicit graph =>
        baNameSrv.create(new BAName, bSrv.getByName("b1").head, aSrv.getByName("a").head).get
        baNameSrv.create(new BAName, bSrv.getByName("b1").head, aSrv.getByName("b").head)
      }.get
      integrityTest(app)(_.by.by(_.out[BAName].v[A].value(_.name).fold)) { implicit graph =>
        i => {
          case (b, as) =>
            i.singleLink[A, String]("aName", aSrv.getByName(_).head, _.name)(_.outEdge[BAName], _.remove).check(b, b.aName, as)
        }
      } must beEqualTo(Map("B-A-unlink" -> 2))

      app[Database].roTransaction { implicit graph =>
        bSrv
          .getByName("b1")
          .value(_.aName)
          .toSeq
      } must beEqualTo(Seq("c"))
    }
  }

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] update field (keep last created) (byName)" in {
      val app: AppBuilder = new AppBuilder().bindToProvider(dbProvider)
      MeshDatabaseBuilder.build(app[MeshSchema])(app[Database], authContext).get
      val aSrv = new ASrv
      val bSrv = new BSrv
      val baNameSrv = new EdgeSrv[BAName, B, A]
      app[Database].tryTransaction { implicit graph =>
        bSrv.getByName("b1").update(_.aName, "foo").iterate()
        baNameSrv.create(new BAName, bSrv.getByName("b1").head, aSrv.getByName("a").head).get
        baNameSrv.create(new BAName, bSrv.getByName("b1").head, aSrv.getByName("b").head)
      }.get
      integrityTest(app)(_.by.by(_.out[BAName].v[A].value(_.name).fold)) { implicit graph =>
        i => {
          case (b, as) =>
            i.singleLink[A, String]("aName", aSrv.getByName(_).head, _.name)(
              _.outEdge[BAName],
              _.remove,
              { bb =>
                aa =>
                  val baNames = bSrv.get(bb).outE[BAName].filter(_.inV.getByIds(aa.map(_._id): _*)).project(_.by.by(_.inV.v[A])).toSeq
                  if (baNames.isEmpty) None
                  else {
                    val selected = baNames.maxBy(_._1._createdAt)._2
                    val (x, y) = baNames.map(_._2).span(_._id != selected._id)
                    Some((selected, x ++ y.tail))
                  }
              }
            ).check(b, b.aName, as)
        }
      } must beEqualTo(Map("B-A-unlink" -> 2, "B-A-setField" -> 1))

      app[Database].roTransaction { implicit graph =>
        bSrv
          .getByName("b1")
          .value(_.aName)
          .toSeq
      } must beEqualTo(Seq("b"))
    }
  }

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] update field (keep first created) (byName)" in {
      val app: AppBuilder = new AppBuilder().bindToProvider(dbProvider)
      MeshDatabaseBuilder.build(app[MeshSchema])(app[Database], authContext).get
      val aSrv = new ASrv
      val bSrv = new BSrv
      val baNameSrv = new EdgeSrv[BAName, B, A]
      app[Database].tryTransaction { implicit graph =>
        bSrv.getByName("b1").update(_.aName, "foo").iterate()
        baNameSrv.create(new BAName, bSrv.getByName("b1").head, aSrv.getByName("a").head).get
        baNameSrv.create(new BAName, bSrv.getByName("b1").head, aSrv.getByName("b").head)
      }.get
      integrityTest(app)(_.by.by(_.out[BAName].v[A].value(_.name).fold)) { implicit graph =>
        i => {
          case (b, as) =>
            i.singleLink[A, String]("aName", aSrv.getByName(_).head, _.name)(
              _.outEdge[BAName],
              _.remove,
              _ => EntitySelector.firstCreatedEntity
            ).check(b, b.aName, as)
        }
      } must beEqualTo(Map("B-A-unlink" -> 2, "B-A-setField" -> 1))

      app[Database].roTransaction { implicit graph =>
        bSrv
          .getByName("b1")
          .value(_.aName)
          .toSeq
      } must beEqualTo(Seq("a"))
    }
  }

  def integrityTest[D <: Product, G](
                                      app: AppBuilder
                                    )(
                                      proj: ProjectionBuilder[Nil.type, B with Entity, Vertex, Converter[B with Entity, Vertex]] => ProjectionBuilder[
                                        D,
                                        B with Entity,
                                        Vertex,
                                        Converter[B with Entity, Vertex]
                                      ]
                                    )(check: Graph => IntegrityCheckOps[B] => D => Map[String, Long]): Map[String, Long] = {
    val ic = new GlobalCheck[B] with IntegrityCheckOps[B] {
      override val db: Database = app[Database]
      override val service: VertexSrv[B] = new BSrv

      override def globalCheck(traversal: Traversal.V[ENTITY])(implicit graph: Graph): Map[String, Long] =
             traversal
                    .project(proj)
                    .toIterator
                    .map(check(graph)(this))
                    .reduceOption(_ <+> _)
                    .getOrElse(Map.empty)
    }
    val result = ic.runGlobalCheck(5.minutes, KillSwitch.alwaysOn) - "duration"
    (ic.runGlobalCheck(5.minutes, KillSwitch.alwaysOn) - "duration") must beEqualTo(Map.empty)
    result
  }
}
