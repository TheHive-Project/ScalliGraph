package org.thp.scalligraph.controllers

import play.api.libs.json._

trait Outputer[M] {
  type D
  def toOutput(m: M): Output[D]
}

object Outputer {
  type Aux[M, DD] = Outputer[M] { type D = DD }

  def apply[M, DD: Writes](f: M => DD): Outputer.Aux[M, DD] = new Outputer[M] {
    override type D = DD
    override def toOutput(m: M): Output[D] = Output(f(m))
  }
  implicit class OutputOps[O, D](o: O)(implicit outputer: Outputer.Aux[O, D]) {
    def toJson: JsValue = outputer.toOutput(o).toJson
    def toOutput: D     = outputer.toOutput(o).toOutput
  }
  implicit def seqOutputer[M](implicit aOutputer: Outputer[M]): Outputer.Aux[Seq[M], Seq[aOutputer.D]] = new Outputer[Seq[M]] {
    override type D = Seq[aOutputer.D]
    override def toOutput(m: Seq[M]): Output[D] = {
      val o = m.map(aOutputer.toOutput(_).toOutput)
      val j = JsArray(m.map(aOutputer.toOutput(_).toJson))
      Output[D](o, j)
    }
  }
  implicit def listOutputer[M](implicit aOutputer: Outputer[M]): Outputer.Aux[List[M], List[aOutputer.D]] = new Outputer[List[M]] {
    override type D = List[aOutputer.D]
    override def toOutput(m: List[M]): Output[D] = {
      val o = m.map(aOutputer.toOutput(_).toOutput)
      val j = JsArray(m.map(aOutputer.toOutput(_).toJson))
      Output[D](o, j)
    }

  }
  implicit def setOutputer[M](implicit aOutputer: Outputer[M]): Outputer.Aux[Set[M], Set[aOutputer.D]] = new Outputer[Set[M]] {
    override type D = Set[aOutputer.D]
    override def toOutput(m: Set[M]): Output[D] = {
      val o = m.map(aOutputer.toOutput(_).toOutput)
      val j = JsArray(m.map(aOutputer.toOutput(_).toJson).toSeq)
      Output[D](o, j)
    }
  }
}

class Output[O](_toOutput: => O, _toJson: => JsValue) {
  type OUT = O
  lazy val toOutput: O     = _toOutput
  lazy val toJson: JsValue = _toJson
}

object Output {
  def apply[O](native: => O, json: => JsValue): Output[O] = new Output[O](native, json)

  def apply[O](native: => O)(implicit writes: Writes[O]): Output[O] = {
    lazy val n = native
    new Output[O](n, writes.writes(n))
  }
}
