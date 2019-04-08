package org.example

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.avro.io.{BinaryEncoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificData, SpecificDatumReader, SpecificDatumWriter, SpecificRecord}

import scala.reflect.ClassTag
import scala.util.Try
import scala.collection.JavaConversions._

object Main {
  def main(args: Array[String]): Unit = {
    val version1 = new v1.DNode("p", "n")
    val version2 = new v2.DNode("p", "n", 0l)
    val version3 =
      new v3.DNode("p", "n", 0l, ByteBuffer.wrap(Array(2.asInstanceOf[Byte])))
    compareWith[v1.DNode](version2) // case where-in we have more data than we need
    compareWith[v2.DNode](version1) // need the patch for 1 extra field(s)
    compareWith[v3.DNode](version1) // need the patch for 2 extra field(s)
    compareWith[v3.DNode](version3) // happy path case


    val siteTagsV1 = new v1.SiteTags(Map("t1" -> new v1.TagConfig("t1", 1L, 2L)))
    val siteTagsV2 = new v2.SiteTags(Map("t1" -> new v2.TagConfig("t1", 1L, 2L, 3L)))

    compareWith[v1.SiteTags](siteTagsV2)
    compareWith[v2.SiteTags](siteTagsV1)
  }

  def compareWith[To <: SpecificRecord: ClassTag](
      from: SpecificRecord): Unit = {
    val fromBytes = serialize(from)
    val to = deserialize[To](fromBytes)
    println(s"from : $from --> to: $to")
  }

  def serialize(obj: SpecificRecord): Array[Byte] = {
    val writer = new SpecificDatumWriter[SpecificRecord](obj.getSchema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(obj, encoder)
    encoder.flush()
    out.close()
    out.toByteArray
  }

  def deserialize[T <: SpecificRecord: ClassTag](bytes: Array[Byte]): T = {
    val classTag = implicitly[ClassTag[T]]
    val reader = new SpecificDatumReader[T](
      SpecificData.get.getSchema(classTag.runtimeClass))
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val newObj = classTag.runtimeClass.asInstanceOf[Class[T]].newInstance()
    Try(reader.read(newObj, decoder)).getOrElse(newObj)
  }
}
