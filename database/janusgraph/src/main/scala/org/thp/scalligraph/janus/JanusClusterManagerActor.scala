package org.thp.scalligraph.janus

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.cluster.typed.{ClusterSingleton, SingletonActor}

object JanusClusterManagerActor {
  sealed trait Command
  sealed trait Result
  case class JoinCluster(replyTo: ActorRef[Result], indexBackend: String, indexLocation: String) extends Command
  case object ClusterInitStart                                                                   extends Result
  case object ClusterReady                                                                       extends Command
  case class ClusterInitialisedConfigurationIgnored(indexBackend: String)                        extends Result
  case object ClusterInitialised                                                                 extends Result

  def getOrCreateClusterManagerActor(system: ActorSystem): ActorRef[Command] = {
    val singletonManager = ClusterSingleton(system.toTyped)
    singletonManager.init(SingletonActor(Behaviors.supervise(create()).onFailure[Exception](SupervisorStrategy.stop), "JanusClusterManager"))
  }

  def create(): Behavior[Command] =
    Behaviors.receiveMessage {
      case JoinCluster(replyTo, indexBackend, parameter) =>
        replyTo ! ClusterInitStart
        clusterInitialising(indexBackend, parameter)
      case ClusterReady => Behaviors.same
    }

  def clusterInitialising(
      installedIndexBackend: String,
      installedIndexLocation: String,
      goodConfMembers: Seq[ActorRef[Result]] = Nil,
      badConfMembers: Seq[ActorRef[Result]] = Nil
  ): Behavior[Command] =
    Behaviors.receiveMessage {
      case JoinCluster(replyTo, indexBackend, indexLocation) if indexBackend == installedIndexBackend && indexLocation == installedIndexLocation =>
        clusterInitialising(installedIndexBackend, installedIndexLocation, goodConfMembers :+ replyTo, badConfMembers)
      case JoinCluster(replyTo, _, _) =>
        clusterInitialising(installedIndexBackend, installedIndexLocation, goodConfMembers, badConfMembers :+ replyTo)
      case ClusterReady =>
        goodConfMembers.foreach(_ ! ClusterInitialised)
        badConfMembers.foreach(_ ! ClusterInitialisedConfigurationIgnored(installedIndexBackend))
        clusterInitialised(installedIndexBackend, installedIndexLocation)
    }

  def clusterInitialised(installedIndexBackend: String, installedIndexLocation: String): Behavior[Command] =
    Behaviors.receiveMessage {
      case JoinCluster(replyTo, indexBackend, indexLocation) if indexBackend == installedIndexBackend && indexLocation == installedIndexLocation =>
        replyTo ! ClusterInitialised
        Behaviors.same
      case JoinCluster(replyTo, _, _) =>
        replyTo ! ClusterInitialisedConfigurationIgnored(installedIndexBackend)
        Behaviors.same
      case ClusterReady =>
        Behaviors.same

    }
}
