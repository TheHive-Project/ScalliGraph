package org.thp.scalligraph.controllers

import java.nio.file.Path

import org.thp.scalligraph.{FPath, FPathElem, FPathElemInSeq, FPathEmpty, FPathSeq}
import play.api.Logger
import play.api.libs.json._
import play.api.mvc._

import scala.collection.immutable

sealed trait Field {
  def get(pathElement: String): Field = FUndefined
  def set(path: FPath, field: Field): Field =
    if (path.isEmpty) field else sys.error(s"$this.set($path, $field)")
}

object Field {
  private[Field] lazy val logger = Logger(getClass)
  def apply(json: JsValue): Field = json match {
    case JsString(s)  ⇒ FString(s)
    case JsNumber(n)  ⇒ FNumber(n.toLong)
    case JsBoolean(b) ⇒ FBoolean(b)
    case JsObject(o)  ⇒ FObject(o.mapValues(Field.apply).toMap)
    case JsArray(a)   ⇒ FSeq(a.map(Field.apply).toList)
    case JsNull       ⇒ FNull
  }
  def apply(request: Request[AnyContent]): Field = {
    def queryFields: FObject =
      FObject(
        request.queryString
          .filterNot(_._1.isEmpty())
          .mapValues(FAny.apply))

    request.body match {
      case AnyContentAsFormUrlEncoded(data) ⇒
        FObject(data.mapValues(v ⇒ FAny(v))) ++ queryFields
      case AnyContentAsText(txt) ⇒
        logger.warn(s"Request body has unrecognized format (text), it is ignored:\n$txt")
        queryFields
      case AnyContentAsXml(xml) ⇒
        logger.warn(s"Request body has unrecognized format (xml), it is ignored:\n$xml")
        queryFields
      case AnyContentAsJson(json: JsObject) ⇒
        Field(json).asInstanceOf[FObject] ++ queryFields
      case AnyContentAsMultipartFormData(MultipartFormData(dataParts, files, badParts)) ⇒
        if (badParts.nonEmpty)
          logger.warn("Request body contains invalid parts")
        val dataFields = dataParts
          .getOrElse("_json", Nil)
          .headOption
          .map { s ⇒
            Json
              .parse(s)
              .as[JsObject]
              .value
              .toMap
              .mapValues(Field.apply)
          }
          .getOrElse(Map.empty)
        files.foldLeft(queryFields ++ FObject(dataFields)) {
          case (obj, MultipartFormData.FilePart(key, filename, contentType, file)) ⇒
            obj.set(FPath(key), FFile(filename.split("[/\\\\]").last, file, contentType.getOrElse("application/octet-stream")))
        }
      case AnyContentAsRaw(raw) ⇒
        if (raw.size > 0)
          logger.warn(s"Request $request has unrecognized body format (raw), it is ignored:\n$raw")
        queryFields
      case AnyContentAsEmpty ⇒ queryFields
      case other ⇒
        sys.error(s"invalid request body : $other (${other.getClass})")
    }
  }

  implicit val fieldWrites: Writes[Field] = Writes[Field](field ⇒ JsString(field.toString))
}

case class FString(value: String)   extends Field
case class FNumber(value: Long)     extends Field
case class FBoolean(value: Boolean) extends Field
case class FSeq(values: List[Field]) extends Field {
  override def set(path: FPath, field: Field): Field = path match {
    case FPathSeq(_, FPathEmpty) ⇒ FSeq(values :+ field)
    case FPathElemInSeq(_, index, tail) ⇒
      FSeq(
        values
          .patch(
            index,
            Seq(
              values
                .applyOrElse(index, (_: Int) ⇒ FUndefined)
                .set(tail, field)),
            1))
  }
}
object FSeq {
  def apply(value1: Field, values: Field*): FSeq = new FSeq(value1 :: values.toList)
  def apply()                                    = new FSeq(Nil)
}
case object FNull                                                       extends Field
case object FUndefined                                                  extends Field
case class FAny(value: Seq[String])                                     extends Field
case class FFile(filename: String, filepath: Path, contentType: String) extends Field

object FObject {
  def empty                                   = new FObject(Map.empty)
  def apply(elems: (String, Field)*): FObject = new FObject(Map(elems: _*))
  def apply(map: Map[String, Field]): FObject = new FObject(map)
  def apply(o: JsObject): FObject =
    new FObject(o.value.mapValues(Field.apply).toMap)
}
case class FObject(fields: immutable.Map[String, Field]) extends Field { // Map[String, Field] with immutable.MapLike[String, Field, FObject] with
  def empty: FObject = FObject.empty

  def iterator: Iterator[(String, Field)] = fields.iterator

  def +(kv: (String, Field)): FObject = new FObject(fields + kv)

  def -(k: String) = new FObject(fields - k)

  def ++(other: FObject): FObject = new FObject(fields ++ other.fields)

  override def set(path: FPath, field: Field): FObject =
    path match {
      case FPathElem(p, tail) ⇒
        fields.get(p) match {
          case Some(FSeq(_))        ⇒ sys.error(s"$this.set($path, $field)")
          case Some(f)              ⇒ FObject(fields.updated(p, f.set(tail, field)))
          case None if tail.isEmpty ⇒ FObject(fields.updated(p, field))
          case None                 ⇒ FObject(fields.updated(p, FObject().set(tail, field)))
        }
      case FPathSeq(p, tail) if tail.isEmpty ⇒
        fields.get(p) match {
          case Some(FSeq(s)) ⇒ FObject(fields.updated(p, FSeq(s :+ field)))
          case None          ⇒ FObject(fields.updated(p, FSeq(List(field))))
          case _             ⇒ sys.error(s"$this.set($path, $field)")
        }
      case FPathElemInSeq(p, idx, tail) ⇒
        fields.get(p) match {
          case Some(FSeq(s)) if s.isDefinedAt(idx) ⇒
            FObject(fields.updated(p, FSeq(s.patch(idx, Seq(s(idx).set(tail, field)), 1))))
          case Some(FSeq(s)) if s.length == idx ⇒
            FObject(fields.updated(p, FSeq(s :+ field)))
          case _ ⇒ sys.error(s"$this.set($path, $field)")
        }
      case _ ⇒ sys.error(s"$this.set($path, $field)")
    }

  override def get(path: String): Field = fields.getOrElse(path, FUndefined)

  def getString(path: String): Option[String] = fields.get(path).collect {
    case FString(s)     ⇒ s
    case FAny(s :: Nil) ⇒ s
  }
}
