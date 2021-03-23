package org.thp.scalligraph.janus

import akka.actor.ExtendedActorSystem
import akka.actor.typed.{ActorRef, ActorRefResolver}
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.serialization.Serializer
import play.api.libs.json.{Json, OFormat, Reads, Writes}

import java.io.NotSerializableException

class JanusClusterSerializer(system: ExtendedActorSystem) extends Serializer {
  import JanusClusterManagerActor._

  private val actorRefResolver = ActorRefResolver(system.toTyped)

  implicit def actorRefReads[T]: Reads[ActorRef[T]]    = Reads.StringReads.map(actorRefResolver.resolveActorRef)
  implicit def actorRefWrites[T]: Writes[ActorRef[T]]  = Writes.StringWrites.contramap[ActorRef[T]](actorRefResolver.toSerializationFormat)
  implicit val joinClusterFormat: OFormat[JoinCluster] = Json.format[JoinCluster]

  override def identifier: Int = 775347820

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case joinCluster: JoinCluster                             => 0.toByte +: Json.toJson(joinCluster).toString.getBytes
      case ClusterInitStart                                     => Array(1)
      case ClusterReady                                         => Array(2)
      case ClusterInitialisedConfigurationIgnored(indexBackend) => 3.toByte +: indexBackend.getBytes
      case ClusterInitialised                                   => Array(4)
      case _                                                    => throw new NotSerializableException
    }

  override def includeManifest: Boolean = false

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef =
    bytes(0) match {
      case 0 => Json.parse(bytes.tail).as[JoinCluster]
      case 1 => ClusterInitStart
      case 2 => ClusterReady
      case 3 => ClusterInitialisedConfigurationIgnored(Json.parse(bytes.tail).as[String])
      case 4 => ClusterInitialised
      case _ => throw new NotSerializableException
    }
}
