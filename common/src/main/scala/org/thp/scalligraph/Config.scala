package org.thp.scalligraph

import java.lang.{Boolean ⇒ JBoolean, Byte ⇒ JByte, Double ⇒ JDouble, Float ⇒ JFloat, Long ⇒ JLong, Short ⇒ JShort}
import java.math.BigInteger
import java.util.{Properties, Iterator ⇒ JIterator, List ⇒ JList}

import org.apache.commons.configuration.{Configuration ⇒ ApacheConfig}
import play.api.{Configuration ⇒ PlayConfig}

import scala.collection.JavaConverters._

class Config(config: PlayConfig) extends ApacheConfig {
  override def subset(prefix: String): ApacheConfig                                                 = ???
  override def isEmpty: Boolean                                                                     = ???
  override def containsKey(key: String): Boolean                                                    = config.keys.contains(key)
  override def addProperty(key: String, value: scala.Any): Unit                                     = ???
  override def setProperty(key: String, value: scala.Any): Unit                                     = ???
  override def clearProperty(key: String): Unit                                                     = ???
  override def clear(): Unit                                                                        = ???
  override def getProperty(key: String): AnyRef                                                     = config.get[String](key)
  override def getKeys(prefix: String): JIterator[String]                                           = ???
  override def getKeys: JIterator[String]                                                           = config.keys.toIterator.asJava
  override def getProperties(key: String): Properties                                               = ???
  override def getBoolean(key: String): Boolean                                                     = ???
  override def getBoolean(key: String, defaultValue: Boolean): Boolean                              = ???
  override def getBoolean(key: String, defaultValue: JBoolean): JBoolean                            = ???
  override def getByte(key: String): Byte                                                           = ???
  override def getByte(key: String, defaultValue: Byte): Byte                                       = ???
  override def getByte(key: String, defaultValue: JByte): JByte                                     = ???
  override def getDouble(key: String): Double                                                       = ???
  override def getDouble(key: String, defaultValue: Double): Double                                 = ???
  override def getDouble(key: String, defaultValue: JDouble): JDouble                               = ???
  override def getFloat(key: String): Float                                                         = ???
  override def getFloat(key: String, defaultValue: Float): Float                                    = ???
  override def getFloat(key: String, defaultValue: JFloat): JFloat                                  = ???
  override def getInt(key: String): Int                                                             = ???
  override def getInt(key: String, defaultValue: Int): Int                                          = ???
  override def getInteger(key: String, defaultValue: Integer): Integer                              = ???
  override def getLong(key: String): Long                                                           = config.get[Long](key)
  override def getLong(key: String, defaultValue: Long): Long                                       = config.getOptional[Long](key).getOrElse(defaultValue)
  override def getLong(key: String, defaultValue: JLong): JLong                                     = config.getOptional[Long](key).fold(defaultValue)(_.asInstanceOf[JLong])
  override def getShort(key: String): Short                                                         = config.get[Int](key).toShort
  override def getShort(key: String, defaultValue: Short): Short                                    = config.getOptional[Int](key).fold(defaultValue)(_.toShort)
  override def getShort(key: String, defaultValue: JShort): JShort                                  = config.getOptional[Int](key).fold(defaultValue)(_.toShort.asInstanceOf[JShort])
  override def getBigDecimal(key: String): java.math.BigDecimal                                     = ???
  override def getBigDecimal(key: String, defaultValue: java.math.BigDecimal): java.math.BigDecimal = ???
  override def getBigInteger(key: String): BigInteger                                               = ???
  override def getBigInteger(key: String, defaultValue: BigInteger): BigInteger                     = ???
  override def getString(key: String): String                                                       = config.get[String](key)
  override def getString(key: String, defaultValue: String): String                                 = config.getOptional[String](key).getOrElse(defaultValue)
  override def getStringArray(key: String): Array[String]                                           = config.get[Seq[String]](key).toArray
  override def getList(key: String): JList[AnyRef]                                                  = ???
  override def getList(key: String, defaultValue: JList[_]): JList[AnyRef]                          = ???
}
