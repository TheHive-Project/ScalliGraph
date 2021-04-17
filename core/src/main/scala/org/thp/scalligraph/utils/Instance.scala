package org.thp.scalligraph.utils

import play.api.mvc.RequestHeader

import java.rmi.dgc.VMID
import java.util.concurrent.atomic.AtomicInteger

object Instance {
  val id: String                           = (new VMID).toString
  val counter                              = new AtomicInteger(0)
  def getRequestId(request: RequestHeader) = s"$id:${request.id}"
  def getInternalId                        = s"$id::${counter.incrementAndGet}"
}
