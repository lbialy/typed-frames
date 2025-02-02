# TypedFrames

TypedFrames is a Scala 3 wrapper around Spark API which allows writing typesafe and boilerplate-free but still efficient Spark code.

## How is it possible to write Spark applications in Scala 3?

Starting from the release of 3.2.0, Spark is cross-compiled also for Scala 2.13, which opens a way to using Spark from Scala 3 code, as Scala 3 projects can depend on Scala 2.13 artifacts.

However, one might run into problems when trying to call a method requiring an implicit instance of Spark's `Encoder` type. Derivation of instances of `Encoder` relies on presence of a `TypeTag` for a given type. However `TypeTag`s are not generated by Scala 3 compiler anymore (and there are no plans to support this) so instances of `Encoder` cannot be automatically synthesized in most cases.

TypedFrames tries to work around this problem by using its own encoders (unrelated to Spark's `Encoder` type) generated using Scala 3's new metaprogramming API.

## How does TypedFrames make things typesafe and efficient at the same time?

TypedFrames provides thin (but strongly typed) wrappers around `DataFrame`s, which track types and names of columns at compile time but let Catalyst perform all of its optimizations at runtime.

TypedFrames uses structural types rather than case classes as data models, which gives us a lot of flexibility (no need to explicitly define a new case class when a column is added/removed/renamed!) but we still get compilation errors when we try to refer to a column which doesn't exist or can't be used in a given context.

## Building and running locally

This project is built using [scala-cli](https://scala-cli.virtuslab.org/).
