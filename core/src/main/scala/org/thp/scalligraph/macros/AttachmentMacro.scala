package org.thp.scalligraph.macros

import org.thp.scalligraph.controllers.Attachment

import scala.reflect.macros.blackbox

trait AttachmentMacro {
  val c: blackbox.Context

  import c.universe._

  /**
    * Create a function that save all attachment of an E
    * @tparam E
    * @return
    */
  // TODO save attachment in subfields
  def mkAttachmentSaver[E: WeakTypeTag]: Tree = {
    val eType = weakTypeOf[E]
    val saver =
      eType.typeSymbol.asClass.primaryConstructor.typeSignature.paramLists.head
        .foldLeft[Tree](q"(e: $eType) ⇒ scala.concurrent.Future.successful(e)") {
          case (fe, element) if element.asTerm.typeSignature <:< typeOf[Attachment] ⇒
            val elementName = element.asTerm.name
            q"""
              import scala.concurrent.Future
              import org.thp.scalligraph.models.{ FAttachment, FFile }

              $fe.andThen { futureE ⇒
                futureE.flatMap(e ⇒ e.$elementName match {
                  case f: FFile       ⇒ attachmentSrv.save(f).map { a ⇒ e.copy($elementName = a) }
                  case _: FAttachment ⇒ Future.successful(e)
                })
              }
            """
          case (fe, element) if element.asTerm.typeSignature <:< typeOf[Option[Attachment]] ⇒
            val elementName = element.asTerm.name
            q"""
              import scala.concurrent.Future
              import org.thp.scalligraph.models.{ FAttachment, FFile }

              $fe.andThen { futureE ⇒
                futureE.flatMap { e ⇒
                  e.$elementName.fold(Future.successful(e)) {
                    case f: FFile       ⇒ attachmentSrv.save(f).map { a ⇒ e.copy($elementName = Some(a)) }
                    case _: FAttachment ⇒ Future.successful(e)
                  }
                }
              }
            """
          case (fe, _) ⇒ fe
        }
    q"(attachmentSrv: org.thp.scalligraph.services.AttachmentSrv) ⇒ $saver"
  }

  // TODO save attachment in subfields
  def mkUpdateAttachmentSaver[E: WeakTypeTag]: Tree = {
    val eType = weakTypeOf[E]
    val saver =
      eType.typeSymbol.asClass.primaryConstructor.typeSignature.paramLists.head
        .foldLeft[Tree](
          q"(ops: Map[org.thp.scalligraph.FPath, org.thp.scalligraph.controllers.UpdateOps.Type]) ⇒ scala.concurrent.Future.successful(ops)") {
          case (fo, element) if element.asTerm.typeSignature <:< typeOf[Attachment] ⇒
            val elementName = element.asTerm.name.toString
            q"""
              import org.thp.scalligraph.controllers.UpdateOps.SetAttribute

              $fo.andThen { futureO ⇒
                futureO.flatMap { o ⇒
                  val path = FPath($elementName)
                  o.get(path)
                    .collect {
                      case SetAttribute(file: FFile) ⇒ attachmentSrv.save(file).map(attachment ⇒ o.updated(path, SetAttribute(attachment)))
                    }
                    .getOrElse(Future.successful(o))
                }
              }
            """
          case (fo, element) if element.asTerm.typeSignature <:< typeOf[Option[Attachment]] ⇒
            val elementName = element.asTerm.name.toString
            q"""
              import org.thp.scalligraph.controllers.UpdateOps.SetAttribute

              $fo.andThen { futureO ⇒
                futureO.flatMap { o ⇒
                  val path = FPath($elementName)
                  o.get(path)
                    .collect {
                      case SetAttribute(Some(file: FFile)) ⇒ attachmentSrv.save(file).map(attachment ⇒ o.updated(path, SetAttribute(attachment)))
                    }
                    .getOrElse(Future.successful(o))
                }
              }
            """
          case (fo, _) ⇒ fo
        }
    q"(attachmentSrv: org.thp.scalligraph.services.AttachmentSrv) ⇒ $saver"
  }

}
