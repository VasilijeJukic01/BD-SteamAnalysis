package utils

import scala.util.Try

object StringExtensions {
  implicit class RichString(val s: String) extends AnyVal {
    def toIntOption: Option[Int] = Try(s.toInt).toOption
    def toBooleanOption: Option[Boolean] = Try(s.toBoolean).toOption
    def toFloatOption: Option[Float] = Try(s.toFloat).toOption
  }
}