package org.virtuslab.typedframes
package types

import scala.quoted._
import scala.deriving.Mirror
import org.apache.spark.sql
import MacroHelpers.TupleSubtype

sealed trait DataType

object DataType:
  type Subtype[T <: DataType] = T

  sealed trait NotNull extends DataType

  type NumericType = ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType
  type NumericOptType = ByteOptType | ShortOptType | IntegerOptType | LongOptType | FloatOptType | DoubleOptType

  def asNotNull(t: Type[?])(using Quotes) = t match
    case '[BooleanOptType] => Type.of[BooleanType]
    case '[StringOptType] => Type.of[StringType]
    case '[ByteOptType] => Type.of[ByteType]
    case '[ShortOptType] => Type.of[ShortType]
    case '[IntegerOptType] => Type.of[IntegerType]
    case '[LongOptType] => Type.of[LongType]
    case '[FloatOptType] => Type.of[FloatType]
    case '[DoubleOptType] => Type.of[DoubleType]
    case '[StructOptType[schema]] => Type.of[StructType[schema]]

  def commonNumericType[T1 : Type, T2 : Type](using Quotes) = 
    val notNull = Type.of[(T1, T2)] match
      case '[(NotNull, NotNull)] => true
      case _ => false
    val baseType = Type.of[(T1, T2)] match
      case '[(DoubleOptType, ?)] | '[(?, DoubleOptType)] => Type.of[DoubleOptType]
      case '[(FloatOptType, ?)] | '[(?, FloatOptType)] => Type.of[FloatOptType]
      case '[(LongOptType, ?)] | '[(?, LongOptType)] => Type.of[LongOptType]
      case '[(IntegerOptType, ?)] | '[(?, IntegerOptType)] => Type.of[IntegerOptType]
      case '[(ShortOptType, ?)] | '[(?, ShortOptType)] => Type.of[ShortOptType]
      case '[(ByteOptType, ?)] | '[(?, ByteOptType)] => Type.of[ByteOptType]

      if notNull then asNotNull(baseType) else baseType

  trait Encoder[-A]:
    type ColumnType <: DataType
    def encode(value: A): Any
    def decode(value: Any): Any
    def catalystType: sql.types.DataType
    def isNullable: Boolean

  trait PrimitiveEncoder[-A] extends Encoder[A]

  trait PrimitiveNullableEncoder[-A] extends PrimitiveEncoder[Option[A]]:
    def encode(value: Option[A]) = value.orNull
    def decode(value: Any) = Option(value)
    def isNullable = true

  trait PrimitiveNonNullableEncoder[-A] extends PrimitiveEncoder[A]:
    def encode(value: A) = value
    def decode(value: Any) = value
    def isNullable = true


  object Encoder:
    type Aux[-A, E <: DataType] = Encoder[A] { type ColumnType = E }

    inline given boolean: PrimitiveNonNullableEncoder[Boolean] with
      type ColumnType = BooleanType
      def catalystType = sql.types.BooleanType
    inline given booleanOpt: PrimitiveNullableEncoder[Boolean] with
      type ColumnType = BooleanOptType
      def catalystType = sql.types.BooleanType

    inline given string: PrimitiveNonNullableEncoder[String] with
      type ColumnType = StringType
      def catalystType = sql.types.StringType
    inline given stringOpt: PrimitiveNullableEncoder[String] with
      type ColumnType = StringOptType
      def catalystType = sql.types.StringType

    inline given byte: PrimitiveNonNullableEncoder[Byte] with
      type ColumnType = ByteType
      def catalystType = sql.types.ByteType
    inline given byteOpt: PrimitiveNullableEncoder[Byte] with
      type ColumnType = ByteOptType
      def catalystType = sql.types.ByteType

    inline given short: PrimitiveNonNullableEncoder[Short] with
      type ColumnType = ShortType
      def catalystType = sql.types.ShortType
    inline given shortOpt: PrimitiveNullableEncoder[Short] with
      type ColumnType = ShortOptType
      def catalystType = sql.types.ShortType

    inline given int: PrimitiveNonNullableEncoder[Int] with
      type ColumnType = IntegerType
      def catalystType = sql.types.IntegerType
    inline given intOpt: PrimitiveNullableEncoder[Int] with
      type ColumnType = IntegerOptType
      def catalystType = sql.types.IntegerType

    inline given long: PrimitiveNonNullableEncoder[Long] with
      type ColumnType = LongType
      def catalystType = sql.types.LongType
    inline given longOpt: PrimitiveNullableEncoder[Long] with
      type ColumnType = IntegerOptType
      def catalystType = sql.types.LongType

    inline given float: PrimitiveNonNullableEncoder[Float] with
      type ColumnType = FloatType
      def catalystType = sql.types.FloatType
    inline given floatOpt: PrimitiveNullableEncoder[Float] with
      type ColumnType = FloatOptType
      def catalystType = sql.types.FloatType

    inline given double: PrimitiveNonNullableEncoder[Double] with
      type ColumnType = DoubleType
      def catalystType = sql.types.DoubleType
    inline given doubleOpt: PrimitiveNullableEncoder[Double] with
      type ColumnType = DoubleOptType
      def catalystType = sql.types.DoubleType

    export StructEncoder.{fromMirror, optFromMirror}

  trait StructEncoder[-A] extends Encoder[A]:
    type StructSchema <: Tuple
    type ColumnType = StructType[StructSchema]
    override def catalystType: sql.types.StructType
    override def encode(a: A): sql.Row

  object StructEncoder:
    type Aux[-A, E <: Tuple] = StructEncoder[A] { type StructSchema = E }

    private case class ColumnInfo(
      labelType: Type[?],
      labelValue: String,
      decodedType: Type[?],
      encoder: Expr[Encoder[?]]
    )

    private def getColumnSchemaType(using quotes: Quotes)(subcolumnInfos: List[ColumnInfo]): Type[?] =
      subcolumnInfos match
        case Nil => Type.of[EmptyTuple]
        case info :: tail =>
          info.labelType match
            case '[Name.Subtype[lt]] =>
              info.encoder match
                case '{ ${encoder}: Encoder.Aux[tpe, DataType.Subtype[e]] } => 
                  getColumnSchemaType(tail) match
                    case '[TupleSubtype[tailType]] => 
                      Type.of[(lt := e) *: tailType]

    private def getSubcolumnInfos(elemLabels: Type[?], elemTypes: Type[?])(using Quotes): List[ColumnInfo] =
      import quotes.reflect.Select
      elemLabels match
        case '[EmptyTuple] => Nil
        case '[label *: labels] =>
          val labelValue = Type.valueOfConstant[label].get.toString
          elemTypes match
            case '[tpe *: types] =>
              Expr.summon[Encoder[tpe]] match
                case Some(encoderExpr) =>
                  ColumnInfo(Type.of[label], labelValue, Type.of[tpe], encoderExpr) :: getSubcolumnInfos(Type.of[labels], Type.of[types])
                case _ => quotes.reflect.report.throwError(s"Could not summon encoder for ${Type.show[tpe]}")

    transparent inline given fromMirror[A]: StructEncoder[A] = ${ fromMirrorImpl[A] }

    def fromMirrorImpl[A : Type](using q: Quotes): Expr[StructEncoder[A]] =
      Expr.summon[Mirror.Of[A]].getOrElse(throw new Exception(s"Could not find Mirror when generating encoder for ${Type.show[A]}")) match
        case '{ ${mirror}: Mirror.ProductOf[A] { type MirroredElemLabels = elementLabels; type MirroredElemTypes = elementTypes } } =>
          val subcolumnInfos = getSubcolumnInfos(Type.of[elementLabels], Type.of[elementTypes])
          val columnSchemaType = getColumnSchemaType(subcolumnInfos)

          val structFieldExprs = subcolumnInfos.map { info =>
            '{ sql.types.StructField(${Expr(info.labelValue)}, ${info.encoder}.catalystType, ${info.encoder}.isNullable) }
          }
          val structFields = Expr.ofSeq(structFieldExprs)

          def rowElements(value: Expr[A]) =
            subcolumnInfos.map { info =>
              import quotes.reflect.*
              info.decodedType match
                case '[t] =>
                  '{ ${info.encoder}.asInstanceOf[Encoder[t]].encode(${ Select.unique(value.asTerm, info.labelValue).asExprOf[t] }) }
            }

          def rowElementsTuple(row: Expr[sql.Row]): Expr[Tuple] =
            val elements = subcolumnInfos.zipWithIndex.map { (info, idx) =>
              given Quotes = q
              '{ ${info.encoder}.decode(${row}.get(${Expr(idx)})) }
            }
            Expr.ofTupleFromSeq(elements)

          columnSchemaType match
            case '[TupleSubtype[t]] =>  
              '{
                (new StructEncoder[A] {
                  override type StructSchema = t
                  override def catalystType = sql.types.StructType(${ structFields })
                  override def isNullable = false
                  override def encode(a: A) =
                    sql.Row.fromSeq(${ Expr.ofSeq(rowElements('a)) })
                  override def decode(a: Any) =
                    ${mirror}.fromProduct(${ rowElementsTuple('{a.asInstanceOf[sql.Row]}) })
                }): StructEncoder[A] { type StructSchema = t }
              }
    end fromMirrorImpl

    inline given optFromMirror[A](using encoder: StructEncoder[A]): (Encoder[Option[A]] { type ColumnType = StructOptType[encoder.StructSchema] }) =
      new Encoder[Option[A]]:
        override type ColumnType = StructOptType[encoder.StructSchema]
        override def encode(value: Option[A]): Any = value.map(encoder.encode).orNull
        override def decode(value: Any): Any = Option(encoder.decode)
        override def catalystType = encoder.catalystType
        override def isNullable = true

sealed class BooleanOptType extends DataType
final class BooleanType extends BooleanOptType, DataType.NotNull

sealed class StringOptType extends DataType
final class StringType extends StringOptType, DataType.NotNull

sealed class ByteOptType extends DataType
final class ByteType extends ByteOptType, DataType.NotNull

sealed class ShortOptType extends DataType
final class ShortType extends ShortOptType, DataType.NotNull

sealed class IntegerOptType extends DataType
final class IntegerType extends IntegerOptType, DataType.NotNull

sealed class LongOptType extends DataType
final class LongType extends LongOptType, DataType.NotNull

sealed class FloatOptType extends DataType
final class FloatType extends FloatOptType, DataType.NotNull

class DoubleOptType extends DataType
final class DoubleType extends DoubleOptType, DataType.NotNull

sealed class StructOptType[Schema <: Tuple] extends DataType
final class StructType[Schema <: Tuple] extends StructOptType[Schema], DataType.NotNull
