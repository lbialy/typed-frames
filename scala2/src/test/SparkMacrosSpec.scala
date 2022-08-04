package org.virtuslab.typedframes

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

case class Spouse(first_name: String)
case class User(
  email: String,
  first_name: Option[String],
  last_name: Option[String],
  birthday: Option[Date],
  spouse: Option[Spouse]
)

class SparkMacrosSpec extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession =
    SparkSession
    .builder()
    .master("local")
    .appName("Types test")
    .getOrCreate()

  import columns._

  import spark.implicits._

  val users = List(
    User("johnwayne@yahoo.com", Some("John"), Some("Gacy"), Some(Date.valueOf("1942-03-17")), None),
    User(
      "nero_augustus_germanicus@rome.it",
      Some("Nero"),
      Some("Germanicus"),
      None,
      Some(Spouse("Poppea"))
    ),
    User("jack_the_ripper@london.co.uk", Some("Jack"), Some("The Ripper"), Some(Date.valueOf("1862-12-03")), None)
  )

  "stringifying columns macro" should "generate matching column names as String instances" in {
    val $userStrCols: StringColumns[User] = $cols[User].str
    $userStrCols(_.email) shouldBe "email"
    $userStrCols(_.first_name) shouldBe "first_name"
    $userStrCols(_.last_name) shouldBe "last_name"
    $userStrCols(_.birthday) shouldBe "birthday"
  }

  "generic column (ColumnName, $\"col\" interpolator) columns macro" should "generate correct Spark columns" in {
    val $userCols: ColumnNames[User] = $cols[User]
    val df: DataFrame = spark.createDataFrame(users)

    $userCols(_.email).toString() shouldBe "email"
    $userCols(_.first_name).toString() shouldBe "first_name"
    $userCols(_.last_name).toString() shouldBe "last_name"
    $userCols(_.birthday).toString() shouldBe "birthday"

    val names = df.select($userCols(_.birthday)).as[String].collect()
    names should contain theSameElementsAs List("1942-03-17", null, "1862-12-03")
  }

  it should "handle nested columns" in {
    val $userCols: ColumnNames[User] = $cols[User]
    val df: DataFrame = spark.createDataFrame(users)

    val spouseFirstNames = df.select($userCols(_.spouse.get.first_name)).as[String].collect()
    spouseFirstNames should contain theSameElementsAs List(null, "Poppea", null)
  }

  "dataframe-driven columns macro" should "generate correct Spark columns" in {
    val df: DataFrame = spark.createDataFrame(users)
    val $userCols: ColumnNames[User] = $cols[User]

    $userCols(_.email, df).toString() shouldBe "email"
    $userCols(_.first_name, df).toString() shouldBe "first_name"
    $userCols(_.last_name, df).toString() shouldBe "last_name"
    $userCols(_.birthday, df).toString() shouldBe "birthday"

    val names = df.select($userCols(_.first_name)).as[String].collect()
    names should contain theSameElementsAs List("John", "Nero", "Jack")
  }

  "dataset-driven column macro" should "generate correct Spark columns" in {
    val ds: Dataset[User] = spark.createDataset(users)
    val $userCols: ColumnNames[User] = $cols[User]

    $userCols(_.email, ds).toString() shouldBe "email"
    $userCols(_.first_name, ds).toString() shouldBe "first_name"
    $userCols(_.last_name, ds).toString() shouldBe "last_name"
    $userCols(_.birthday, ds).toString() shouldBe "birthday"

    val names = ds.select($userCols(_.last_name)).as[String].collect()
    names should contain theSameElementsAs List("Gacy", "Germanicus", "The Ripper")
  }

  "all columns macro" should "generate a list of column name strings from a case class" in {
    val allCols = $cols[User].str.all

    allCols should contain theSameElementsAs List("email", "first_name", "last_name", "birthday", "spouse")
  }

  "all spark column name macro" should "generate a list of column name instances from a case class" in {
    val $allCols = $cols[User].$all

    $allCols.map(_.toString()) should contain theSameElementsAs List(
      "email",
      "first_name",
      "last_name",
      "birthday",
      "spouse"
    )
  }

}
