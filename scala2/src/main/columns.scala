package org.virtuslab.typedframes

import org.apache.spark.sql.{Column, ColumnName, Dataset}

import scala.language.experimental.macros

/**
  * Welcome to org.virtuslab.typedframes documentation!
  *
  * Purpose of this package is to provide a compile-time safety of column names used with Spark DataFrames.
  * 'Why would you do this when you have Datasets?' you ask? Well, mostly because Datasets are slower due to
  * deserialization, not all operations are available using Dataset typed API and, last but definitely not
  * least - joinWith generates tuples with high overhead, join flattens that and is awesome but brakes your
  * case class schema while doing that. This package allows you to use DataFrames in a bit more sensible way -
  * you can still use case classes for schema but Spark can be completely unaware of that. The only thing we are
  * doing is providing a type-safe way to get a [[org.apache.spark.sql.Column]] for a case class field using
  * macro deconstructing an AST of a scala lambda function (you can also get a [[java.lang.String]] if you need it!).
  *
  * How to use this then?
  *
  * There is only one entry to the api: the $cols function which only requires a type of your case class:
  * {{{
  *   case class Sale(id: UUID, product_id: UUID, amount: BigDecimal, customer_id: UUID, datetime: Timestamp)
  *   val $salesCols = $cols[Sale]
  * }}}
  * The possible further calls are:
  * {{{
  *   val productIdsColumn: org.apache.spark.sql.Column = $salesCols(_.product_id) // this is a scala lambda
  *
  *   val amountCol: String = $salesCols.str(_.amount) // amountCol == "amount"
  *
  *   val saleDS: Dataset[Sale] = ???
  *
  *   // customerIdColumn preserves column lineage information (it's a 'SELECT customer_id FROM saleDS' in SQL parlance)
  *   val customerIdColumn: org.apache.spark.sql.Column = $salesCols(_.customer_id, saleDS)
  *
  *   val columns: List[org.apache.spark.sql.Column] = $salesCols.$all
  *
  *   val strColumns: List[String] = $salesCols.str.all
  * }}}
  *
  * A few examples:
  * Let's assume:
  * {{{
  *   import org.virtuslab.typedframes.columns._
  *   case class User(first_name: String, last_name: String, email: String)
  *   case class Recovery(recovery_email: String, token: String)
  * }}}
  * * raw strings
  * {{{
  *   val df1: DataFrame = ???
  *   val df2: DataFrame = ???
  *   df1.join(df2, Seq("email")) // stringly typed
  * }}}
  * -> replacement:
  * {{{
  *   val df1: DataFrame = ???
  *   val df2: DataFrame = ???
  *   val $usrCols = $cols[User]
  *   df1.join(df2, Seq( $usrCols.str(_.email) )) // typed
  * }}}
  * * stringly typed columns
  * {{{
  *   val df: DataFrame = ???
  *   df.filter($"first_name" =!= "Simon") // stringly typed
  * }}}
  * -> replacement:
  * {{{
  *   val df: DataFrame = ???
  *   val $usrCols = $cols[User]
  *   df.filter($usrCols(_.first_name) =!= "Simon") // typed
  * }}}
  * * dataframe-addressed strings
  * {{{
  *   val df1: Dataset[User] = ???
  *   val df2: Dataset[Recovery] = ???
  *   df1.join(df2, df1("email") === df2("recovery_email"))
  * }}}
  * -> replacement:
  * {{{
  *   case class Recovery(recovery_email: String, token: String)
  *   val df1: Dataset[User] = ???
  *   val df2: Dataset[Recovery] = ???
  *   val $usrCols = $cols[User]
  *   val $recCols = $cols[Recovery]
  *   df1.join(df2, $usrCols(_.email, df1) === $recCols(_.recovery_email, df2))
  *   // alternatively
  *   df1.join(df2, df1($usrCols.str(_.email)) === df2($recCols.str(_.recovery_email)))
  * }}}
  *
  */
object columns {

  class StringColumns[A <: Product] private[typedframes] () {
    def apply[B](selector: A => B): String = macro internal.ColumnsBlackboxMacros.colNameMacroImpl
    def all: List[String] = macro internal.ColumnsBlackboxMacros.allColNameMacroImpl[A]
  }

  class ColumnNames[A <: Product] private[typedframes] () {
    def apply[B](selector: A => B): ColumnName = macro internal.ColumnsBlackboxMacros.colNameColumnNameMacroImpl

    def apply[B, C](selector: A => B, df: Dataset[C]): Column =
      macro internal.ColumnsBlackboxMacros.colNameDataFrameTwoArgMacroImpl

    val str = new StringColumns[A]
    def $all: List[ColumnName] = macro internal.ColumnsBlackboxMacros.allColNameColumnNameMacroImpl[A]
  }

  def $cols[A <: Product]: ColumnNames[A] = new ColumnNames[A]

}
