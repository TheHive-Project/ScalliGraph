package org.thp.scalligraph.steps

import scala.language.higherKinds
import gremlin.scala.{GremlinScala, Vertex}
import java.lang.{Double => JDouble, Long => JLong}
import java.util.{UUID, Collection => JCollection, List => JList, Map => JMap, Set => JSet}

import scala.collection.JavaConverters._
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{__, DefaultGraphTraversal, GraphTraversal}
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.models.Entity

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class StepLabel[D, G, C <: Converter[D, G]] {
  val name: String               = UUID.randomUUID().toString
  private var _converter: Try[C] = Failure(InternalError(s"StepLabel $name is use before set"))

  def converter: C                = _converter.get
  def setConverter(conv: C): Unit = _converter = Success(conv)
}
trait BiConverter[D, G] extends Converter[D, G] {
  def reverse: Converter[G, D]
}
trait Converter[+D, -G] extends (G => D) {
  def untypedApply(g: Any): Any = apply(g.asInstanceOf[G])
}
object Converter {
  type any = Converter[Nothing, Any]
  def identity[A]: IdentityConverter[A]  = new IdentityConverter[A]
  val long: Converter[Long, JLong]       = _.toLong
  val double: Converter[Double, JDouble] = _.toDouble
  type CCollection[D, G, C <: Converter[D, G]] = Poly1Converter[Seq[D], JCollection[G], D, G, C]
  type CList[D, G, C <: Converter[D, G]]       = Poly1Converter[Seq[D], JList[G], D, G, C]
  def clist[D, G, C <: Converter[D, G]](converter: C): CList[D, G, C] = new Poly1Converter[Seq[D], JList[G], D, G, C] {
    override val subConverter: C = converter
    override def apply(l: JList[G]): Seq[D] = converter match {
      case _: IdentityConverter[_] => l.asScala.asInstanceOf[Seq[D]]
      case c                       => l.asScala.map(c)
    }
  }
  val cid: Converter[String, AnyRef] = _.toString
  type CMap[DK, DV, GK, GV, CK <: Converter[DK, GK], CV <: Converter[DV, GV]] =
    Poly2Converter[Map[DK, Seq[DV]], JMap[GK, JCollection[GV]], DK, Seq[DV], GK, JCollection[GV], CK, CCollection[DV, GV, CV]]
      with Poly1Converter[
        Map[DK, Seq[DV]],
        JMap[GK, JCollection[GV]],
        (DK, Seq[DV]),
        JMap.Entry[GK, JCollection[GV]],
        CMapEntry[DK, GK, DV, GV, CK, CV]
      ]
  type CMapEntry[DK, GK, DV, GV, CK <: Converter[DK, GK], CV <: Converter[DV, GV]] =
    Poly2Converter[(DK, Seq[DV]), JMap.Entry[GK, JCollection[GV]], DK, Seq[DV], GK, JCollection[GV], CK, CCollection[DV, GV, CV]]
      with Converter[(DK, Seq[DV]), JMap.Entry[GK, JCollection[GV]]]
  def cmap[DK, DV, GK, GV, CK <: Converter[DK, GK], CV <: Converter[DV, GV]](kConverter: CK, vConverter: CV): CMap[DK, DV, GK, GV, CK, CV] =
    /*
              (m: JMap[GK, JCollection[GV]]) =>
            if (keyByResult.converter.isIdentity && valueByResult.converter.isIdentity)
              m.asScala.mapValues(_.asScala.toSeq).toMap.asInstanceOf[Map[DK, Seq[DV]]]
            else if (keyByResult.converter.isIdentity)
              m.asScala
                .mapValues(_.asScala.toSeq.map(valueByResult.converter))
                .toMap
                .asInstanceOf[Map[DK, Seq[DV]]] //(m: JMap[KG, JCollection[VG]]) => m.asScala.mapValues(_.asScala))
            else if (valueByResult.converter.isIdentity)
              m.asScala.map { case (k, v) => keyByResult.converter(k) -> v.asScala.toSeq }.toMap.asInstanceOf[Map[DK, Seq[DV]]]
            else
              m.asScala.map { case (k, v) => keyByResult.converter(k) -> v.asScala.map(valueByResult.converter).toSeq }.toMap

     */
    ???
}

class IdentityConverter[A] extends Converter[A, A] {
  override def apply(a: A): A = a
}
trait Poly1Converter[+SD, -SG, D, G, C <: Converter[D, G]] extends Converter[SD, SG] {
  val subConverter: C
}
trait Poly2Converter[+SD, -SG, DK, DV, GK, GV, CK <: Converter[DK, GK], CV <: Converter[DV, GV]] {
  val subConverterKey: CK
  val subConverterValue: CV
}

//class IdentityGraphConverterMapper[-F <: GraphConverter[_, _], +T <: GraphConverter[_, _]] extends GraphConverterMapper[F, T] {
//  override def apply(c: F): T = c.asInstanceOf[T]
//}
//
//object IdentityGraphConverterMapper {
//  implicit def default[A]: IdentityGraphConverterMapper[A, A] = new IdentityGraphConverterMapper[A, A]
//}
//
//class ToIdentityGraphConverterMapper[-F <: GraphConverter[_, _], A, B] extends GraphConverterMapper[F, GraphConverter[A, B]] {
//  override def apply(c: F): T = GraphConverter.identity[T]
//}
abstract class GraphConverterMapper[-F <: Converter[_, _], +T <: Converter[_, _]] extends (F => T) {
  def untypedApply(from: Any): Converter[_, _] = apply(from.asInstanceOf[F])
}
object GraphConverterMapper {
  def apply[D, G](f: G => D): GraphConverterMapper[_, Converter[D, G]] = _ => f(_)
  def identity[A <: Converter[_, _]]: GraphConverterMapper[A, A]       = Predef.identity(_)
  def toIdentity[A]: GraphConverterMapper[_, Converter[A, A]]          = _ => Converter.identity[A]
  def toList: GraphConverterMapper[_, _] = {
    case _: IdentityConverter[_] => (_: Any).asInstanceOf[JList[_]].asScala
    case conv                    => (_: Any).asInstanceOf[JList[Any]].asScala.map(conv.asInstanceOf[Any => Any])
  }
}

object Traversal {
  type VERTEX[E] = Traversal[E with Entity, Vertex, Converter[E with Entity, Vertex]]
  def apply[T: ClassTag](raw: GremlinScala[T]) = new Traversal[T, T, Converter[T, T]](raw, identity[T])
}

//class UntypedTraversal(val traversal: GraphTraversal[_, _], val converter: Converter[_, _]) {
//  def onGraphTraversal[A, B](f: GraphTraversal[A, B] => GraphTraversal[_, _], convMap: GraphConverterMapper[_, _]) =
//    new UntypedTraversal(f(traversal.asInstanceOf[GraphTraversal[A, B]]), convMap.untypedApply(converter))
//  override def clone(): UntypedTraversal =
//    traversal match {
//      case dgt: DefaultGraphTraversal[_, _] => new UntypedTraversal(dgt.clone, converter)
//    }
//}
//
//object UntypedTraversal {
//  def start: UntypedTraversal = new UntypedTraversal(__.start(), Converter.identity)
//}

class Traversal[+D, G, C <: Converter[D, G]](val raw: GremlinScala[G], val converter: C) {
//  def typeName: String                                              = classTag[G].runtimeClass.getSimpleName
  def deepRaw: GraphTraversal[_, G]                                    = raw.traversal
  def onRaw(f: GremlinScala[G] => GremlinScala[G]): Traversal[D, G, C] = new Traversal[D, G, C](f(raw), converter)
  def onRawMap[D2, G2, C2 <: Converter[D2, G2]](
      f: GremlinScala[G] => GremlinScala[G2],
      convMap: GraphConverterMapper[C, C2]
  ) = new Traversal[D2, G2, C2](f(raw), convMap(converter))
  def onDeepRaw(f: GraphTraversal[_, G] => GraphTraversal[_, G]): Traversal[D, G, C] =
    new Traversal[D, G, C](new GremlinScala[G](f(raw.traversal)), converter)
  def onDeepRawMap[D2, G2, C2 <: Converter[D2, G2]](
      f: GraphTraversal[_, G] => GraphTraversal[_, G2],
      convMap: GraphConverterMapper[C, C2]
  ): Traversal[D2, G2, C2] =
    new Traversal[D2, G2, C2](new GremlinScala[G2](f(raw.traversal)), convMap(converter))
  def map[DD](f: D => DD): Traversal[DD, G, Converter[DD, G]] =
    new Traversal[DD, G, Converter[DD, G]](raw, g => converter.andThen(f).apply(g))
  def graphMap[DD, GG, CC <: Converter[DD, GG]](d: G => GG, convMap: GraphConverterMapper[C, CC]): Traversal[DD, GG, CC] =
    new Traversal[DD, GG, CC](raw.map(d), convMap(converter))
  def start = new Traversal[D, G, C](gremlin.scala.__[G], converter)

//  def mapping: UniMapping[D]
//  def converter: Converter.Aux[D, G] = mapping

  def toDomain(g: G): D = converter(g)
//  def cast[DD, GG](m: Mapping[_, DD, GG]): Option[Traversal[DD, GG]] =
//    if (m.isCompatibleWith(mapping)) Some(new Traversal(raw.asInstanceOf[GremlinScala[GG]], m))
//    else None

  def mapAsNumber(
      f: Traversal[Number, Number, IdentityConverter[Number]] => Traversal[Number, Number, IdentityConverter[Number]]
  ): Traversal[D, G, C]                                                                                                                = ???
  def mapAsComparable(f: Traversal[Comparable[_], Comparable[G], _] => Traversal[Comparable[_], Comparable[G], _]): Traversal[D, G, C] = ???
//  def changeMapping[DD](m: Mapping[_, DD, G]): Traversal[DD, G] = new Traversal(raw, m.toDomain)
  override def clone(): Traversal[D, G, C] = new Traversal[D, G, C](raw.clone, converter)
}
//
////trait KeyTypeInfo[FROM, K, Dk, GK, CK <: Converter[]]
//class TypeInfo[FROM, C <: Poly1Converter[_, _, _, _, CC], CC <: Converter[DUNFOLD, GUNFOLD], DK, GK, DV, GV, DKEY, GKEY, DVALUE, GVALUE, DUNFOLD, GUNFOLD] {
////  type K
////  type V
////  type KEY
////  type DKEY
//////  def keyMap[DK](c: K => DK)(k: KEY): DKEY[DK]
////  type VALUE
////  type DVALUE
//////  def valueMap[DV](c: V => DV)(v: VALUE): DVALUE[DV]
////  type UNFOLD
////  type DUNFOLD
//////  def unfoldConverter[DUNFOLD](ck: K => DK, cv: V => DV)(u: UNFOLD): DUNFOLD[DK, DV]
//
//  def unfoldConverter(
//      c: C
//  ): CC = c.subConverter
//
//}
//
//object TypeInfo {
//  type Id[A] = A
////  type Aux[FROM, _K, _V, _KEY, _DKEY, _VALUE, _DVALUE, _UNFOLD, _DUNFOLD] = TypeInfo[FROM] {
////    type GK          = _K
////    type GV          = _V
////    type GKEY        = _KEY
////    type DKEY[KK]    = _DKEY[KK]
////    type GVALUE      = _VALUE
////    type DVALUE[VV]  = _DVALUE[VV]
////    type GUNFOLD     = _UNFOLD
////    type DUNFOLD[KV] = _DUNFOLD[KV]
////
////  }
//  implicit def ListTypeInfo[DUNFOLD, GUNFOLD, C <: Poly1Converter[_, _, _, _, CC], CC <: Converter[DUNFOLD, GUNFOLD]]
//      : TypeInfo[JCollection[GUNFOLD], C, CC, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, DUNFOLD, GUNFOLD] =
//    new TypeInfo[JCollection[GUNFOLD], C, CC, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, DUNFOLD, GUNFOLD]
//
//  implicit def MapKeyColumn[DUNFOLDK, GUNFOLDK, DUNFOLDV, GUNFOLDV, C <: Poly1Converter[_, _, _, _, CC], CC <: Converter[
//    (DUNFOLDK, DUNFOLDV),
//    JMap.Entry[GUNFOLDK, GUNFOLDV]
//  ]]: TypeInfo[JMap[GUNFOLDK, GUNFOLDV], C, CC, DUNFOLDK, GUNFOLDK, DUNFOLDV, GUNFOLDV, JSet[GUNFOLDK], Set[DUNFOLDK], JCollection[GUNFOLDV], Seq[
//    DUNFOLDV
//  ], (DUNFOLDK, DUNFOLDV), JMap.Entry[GUNFOLDK, GUNFOLDV]] =
//    new TypeInfo[JMap[GUNFOLDK, GUNFOLDV], C, CC, DUNFOLDK, GUNFOLDK, DUNFOLDV, GUNFOLDV, JSet[GUNFOLDK], Set[DUNFOLDK], JCollection[GUNFOLDV], Seq[
//      DUNFOLDV
//    ], (DUNFOLDK, DUNFOLDV), JMap.Entry[GUNFOLDK, GUNFOLDV]]
//  /*
//  [FROM,  C <: org.thp.scalligraph.steps.Poly1Converter[_, _, _, _, CC],CC <: org.thp.scalligraph.steps.Converter[DUNFOLD,GUNFOLD],DK,GK,DV,GV,DKEY,GKEY,DVALUE,GVALUE,DUNFOLD,GUNFOLD]
//  ]]: TypeInfo[JMap[GUNFOLDK, GUNFOLDV], C, CC, DUNFOLDK, GUNFOLDK, DUNFOLDV, GUNFOLDV, JSet[GUNFOLDK], Set[DUNFOLDK], JCollection[GUNFOLDV], Seq[
//
//
//  Error:(228, 7) type arguments
//  [java.util.Map[GUNFOLDK,GUNFOLDV],Poly1Converter[_, _, DUNFOLD, GUNFOLD, CC],CC,DUNFOLDK,GUNFOLDK,DUNFOLDV,GUNFOLDV,java.util.Set[GUNFOLDK],Set[DUNFOLDK],java.util.Collection[GUNFOLDV],Seq[DUNFOLDV],java.util.Map.Entry[GUNFOLDK,GUNFOLDV],(DUNFOLDK, DUNFOLDV)]
//   [FROM,C <: org.thp.scalligraph.steps.Poly1Converter[_, _, _, _, CC],CC <: org.thp.scalligraph.steps.Converter[DUNFOLD,GUNFOLD],DK,GK,DV,GV,DKEY,GKEY,DVALUE,GVALUE,DUNFOLD,GUNFOLD]
//
//Error:(227, 7) type arguments
//[java.util.Map[GUNFOLDK,GUNFOLDV],C,CC,DUNFOLDK,GUNFOLDK,DUNFOLDV,GUNFOLDV,java.util.Set[GUNFOLDK],Set[DUNFOLDK],java.util.Collection[GUNFOLDV],Seq[DUNFOLDV],java.util.Map.Entry[GUNFOLDK,GUNFOLDV],(DUNFOLDK, DUNFOLDV)] do not conform to trait TypeInfo's type parameter bounds
//[FROM,C <: org.thp.scalligraph.steps.Poly1Converter[_, _, DUNFOLD, GUNFOLD, CC],CC <: org.thp.scalligraph.steps.Converter[DUNFOLD,GUNFOLD],DK,GK,DV,GV,DKEY,GKEY,DVALUE,GVALUE,DUNFOLD,GUNFOLD]
//  ]]: TypeInfo[JMap[GUNFOLDK, GUNFOLDV], C, CC, DUNFOLDK, GUNFOLDK, DUNFOLDV, GUNFOLDV, JSet[GUNFOLDK], Set[DUNFOLDK], JCollection[GUNFOLDV], Seq[
//   */
//  //      def keyMap[DK](c: C)(k: JSet[K])(implicit ev: C <:< Poly2Converter[Map, JMap, DK, _, K, _, _ <: Converter[DK, K], _]): Set[DK] =
////        k.asScala.map(c.subConverterKey).toSet
////      def valueMap[DV](c: V => DV)(v: JCollection[V]): Seq[DV] = v.asScala.map(c).toSeq
////      def unfoldConverter[DV](c: C)(
////          implicit ev: C <:< Poly1Converter[Map[_, _], JMap[_, _], DV, V, _ <: Converter[DV, V]]
//////          implicit ev: C <:< Poly2Converter[Map, JMap, DK, _, K, _, _ <: Converter[DK, K], _ <: Converter[DV, V]]
////      ): Converter[DV, JMap.Entry[K, V]] =
////        entry => c.subConverterKey(entry.getKey) -> c.subConverterValue(entry.getValue)
//
//  //
////  implicit def MapEntryKeyColumn[K, V]: Aux[JMap.Entry[K, V], K, V, Nothing] =
////    new TypeInfo[JMap.Entry[K, V]] {
////      override type KEY    = K
////      override type VALUE  = V
////      override type UNFOLD = Nothing
//
////    }
////  implicit val PathKeyColumn: Aux[Path, JList[JSet[String]], JList[Any], Nothing] = new TypeInfo[Path] {
////    type KEY             = JList[JSet[String]]
////    type VALUE           = JList[Any]
////    override type UNFOLD = Nothing
////  }
//}
