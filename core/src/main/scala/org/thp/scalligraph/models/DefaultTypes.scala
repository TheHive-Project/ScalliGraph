//package org.thp.scalligraph.models
//
//import gremlin.scala.dsl.Converter
//
//object DefaultTypes {
//  implicit def seqConverter[A, AGraphType](implicit aConverter: Converter.Aux[A, AGraphType]): Converter[Seq[A]] =
//    new Converter[Seq[A]] {
//      type GraphType = Seq[AGraphType]
//
//      def toDomain(aGraphs: Seq[AGraphType]): Seq[A] =
//        aGraphs.map(aConverter.toDomain)
//
//      def toGraph(as: Seq[A]): Seq[AGraphType] = as.map(aConverter.toGraph)
//    }
//
//  implicit class RichConverter[D](val converter: Converter[D]) {
//    def sequence: Converter.Aux[Seq[D], Seq[converter.GraphType]] =
//      new Converter[Seq[D]] {
//        override type GraphType = Seq[converter.GraphType]
//
//        override def toDomain(graphValue: Seq[converter.GraphType]): Seq[D] =
//          graphValue.map(converter.toDomain)
//
//        override def toGraph(domainValue: Seq[D]): Seq[converter.GraphType] =
//          domainValue.map(converter.toGraph)
//      }
//
//    def optional(implicit ev: Null <:< converter.GraphType): Converter.Aux[Option[D], converter.GraphType] =
//      new Converter[Option[D]] {
//        override type GraphType = converter.GraphType
//
//        override def toDomain(graphValue: converter.GraphType): Option[D] =
//          Option(graphValue).map(converter.toDomain)
//
//        override def toGraph(domainValue: Option[D]): converter.GraphType =
//          domainValue.map(converter.toGraph).orNull
//      }
//  }
//}
