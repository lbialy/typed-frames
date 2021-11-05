// using scala 3.1.1-RC1
// using lib org.apache.spark:spark-core_2.13:3.2.0
// using lib org.apache.spark:spark-sql_2.13:3.2.0
// using lib io.github.vincenzobaz::spark-scala3:0.1.3-spark3.2.0-SNAPSHOT

package org.virtuslab.example

import scala3encoders.given

import org.apache.spark.sql.SparkSession

case class Foo(a: String, b: Int)
case class Bar(b: Int, c: String, d: Baz)
case class Baz(x: Int)

case class FooBar(a: String, b: Int, c: String)

object HellSpark {
  def main(args: Array[String]): Unit = {
    implicit lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("spark test example")
        .getOrCreate()
    }

    import spark.implicits._

    // import TypedSpark.{toTypedDF, toTypedDFNamed, SchemaFor, $, TypedColumn, asDF, +} // No idea why `import TypedSpark.given` or `import TypedSpark.schemaFromMirror` is not needed
    import org.virtuslab.typedspark.TypedSpark.*

    val ints = Seq(1, 2, 3, 4).toTypedDF["i"]
    ints.show()

    val strings = Seq("abc", "def").toTypedDFNamed("s")
    strings.show()

    val foos = Seq(
      Foo("aaaa", 1),
      Foo("bbbb", 2)
    ).toTypedDF

    foos.show()

    foos.select($.b.named["b1"]).show()

    val afterSelect = foos.select($.a, ($.b + $.b).named["bb"])

    afterSelect.show()

    afterSelect.select($.bb.named["bbb"]).show()

    // afterSelect.select($.bc.named["bbb"]).show() // <- This won't compile

    spark.stop()
  }
}
