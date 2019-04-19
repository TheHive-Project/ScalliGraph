package org.thp.scalligraph.controllers

import java.io.File
import java.nio.file.Path

import play.api.libs.Files
import play.api.libs.Files.TemporaryFileCreator
import play.api.mvc.MultipartFormData.FilePart
import play.api.mvc.{AnyContentAsMultipartFormData, Headers, MultipartFormData}
import play.api.test.{FakeRequest, NoTemporaryFileCreator, PlaySpecification}

import org.specs2.mock.Mockito

case class FakeTemporaryFile(name: String) extends Files.TemporaryFile {
  def file                                       = new File(name)
  def path: Path                                 = file.toPath
  def temporaryFileCreator: TemporaryFileCreator = NoTemporaryFileCreator
}
object FakeTemporaryFile {
  def apply(): Files.TemporaryFile = FakeTemporaryFile("temporaryFileName")
}
class FieldsTest extends PlaySpecification with Mockito {
  "Field" should {
    "be built from HTTP request with file" in {
      val file      = FakeTemporaryFile()
      val dataParts = Map("ignore" → Seq("x", "xx"), "_json" → Seq("""{"f1":"v1"}"""))
      val files     = Seq(FilePart("attachment", "myfile.txt", Some("text/plain"), file))
      val request   = FakeRequest("GET", "/", Headers.create(), body = AnyContentAsMultipartFormData(MultipartFormData(dataParts, files, Nil)))

      Field(request) must_=== FObject("f1" → FString("v1"), "attachment" → FFile("myfile.txt", file.path, "text/plain"))
    }

    "be built from HTTP request with file in sub field" in {
      val file      = FakeTemporaryFile()
      val dataParts = Map("ignore" → Seq("x", "xx"), "_json" → Seq("""{"f1":{"a":"v1"}}"""))
      val files     = Seq(FilePart("f1.b", "myfile.txt", Some("text/plain"), file))
      val request   = FakeRequest("GET", "/", Headers.create(), body = AnyContentAsMultipartFormData(MultipartFormData(dataParts, files, Nil)))

      Field(request) must_=== FObject("f1" → FObject("a" → FString("v1"), "b" → FFile("myfile.txt", file.path, "text/plain")))
    }

    "be built from HTTP request with file in sub field 2" in {
      val file      = FakeTemporaryFile()
      val dataParts = Map("ignore" → Seq("x", "xx"), "_json" → Seq("""{"f1":{"a":"v1"}}"""))
      val files     = Seq(FilePart("f2.b", "myfile.txt", Some("text/plain"), file))
      val request   = FakeRequest("GET", "/", Headers.create(), body = AnyContentAsMultipartFormData(MultipartFormData(dataParts, files, Nil)))

      Field(request) must_=== FObject("f1" → FObject("a" → FString("v1")), "f2" → FObject("b" → FFile("myfile.txt", file.path, "text/plain")))
    }

    "be built from HTTP request with file in seq field" in {
      val file      = FakeTemporaryFile()
      val dataParts = Map("ignore" → Seq("x", "xx"), "_json" → Seq("""{"f1":{"a":"v1", "b": []}}"""))
      val files     = Seq(FilePart("f1.b[]", "myfile.txt", Some("text/plain"), file))
      val request   = FakeRequest("GET", "/", Headers.create(), body = AnyContentAsMultipartFormData(MultipartFormData(dataParts, files, Nil)))

      Field(request) must_=== FObject("f1" → FObject("a" → FString("v1"), "b" → FSeq(List(FFile("myfile.txt", file.path, "text/plain")))))
    }

    "be built from HTTP request with file in seq field 2" in {
      val file      = FakeTemporaryFile()
      val dataParts = Map("ignore" → Seq("x", "xx"), "_json" → Seq("""{"f1":{"a":"v1", "b": ["a", "b"]}}"""))
      val files     = Seq(FilePart("f1.b[]", "myfile.txt", Some("text/plain"), file))
      val request   = FakeRequest("GET", "/", Headers.create(), body = AnyContentAsMultipartFormData(MultipartFormData(dataParts, files, Nil)))

      Field(request) must_=== FObject(
        "f1" → FObject("a" → FString("v1"), "b" → FSeq(List(FString("a"), FString("b"), FFile("myfile.txt", file.path, "text/plain")))))
    }

    "be built from HTTP request with file in seq field 3" in {
      val file      = FakeTemporaryFile()
      val dataParts = Map("ignore" → Seq("x", "xx"), "_json" → Seq("""{"f1":{"a":"v1", "b": ["a", "b", "c"]}}"""))
      val files     = Seq(FilePart("f1.b[1]", "myfile.txt", Some("text/plain"), file))
      val request   = FakeRequest("GET", "/", Headers.create(), body = AnyContentAsMultipartFormData(MultipartFormData(dataParts, files, Nil)))

      Field(request) must_=== FObject(
        "f1" → FObject("a" → FString("v1"), "b" → FSeq(List(FString("a"), FFile("myfile.txt", file.path, "text/plain"), FString("c")))))
    }
  }
}
