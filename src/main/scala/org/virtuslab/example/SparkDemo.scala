package org.virtuslab.typedframes
package example

import scala3encoders.given

import org.apache.spark.sql.SparkSession

object SparkDemo {
  def main(args: Array[String]): Unit = {
    implicit lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("spark test example")
        .getOrCreate()
    }

    import spark.implicits._
    import org.virtuslab.typedframes.api.{*, given}
    import org.virtuslab.typedframes.functions.*
    
    case class City(name: String, population: Int)
    case class Country(name: String, continent: String, capital: String, population: Int, gdp: Int)

    val cities = Seq(
      City("Warsaw", 1794532),
      City("Krakow", 769595),
      City("Paris", 11142303),
      City("Washington", 718355),
      City("London", 	9540576),
      City("Ottawa", 	1422635)
    ).toTypedDF

    val countries = Seq(
      Country("United Kingdom", "Europe", "London", 67886011, 39532),
      Country("France", "Europe", "Paris", 65273511, 39827),
      Country("USA", "North America", "Washington", 331002651, 59939),
      Country("Poland", "Europe", "Warsaw", 37846611, 13871),
      Country("Canada", "North America", "Ottawa", 37742154, 44841)
    ).toTypedDF

    countries.join(cities).on($.countries.capital === $.cities.name)
      .select(
        $.countries.name,
        $.continent,
        $.cities.population.as("capital population")
      ).show()

    countries.groupBy($.continent).agg(avg($.gdp).as("avg gdp")).show()

    spark.stop()
  }
}