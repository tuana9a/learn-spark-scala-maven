package com.tuana9a

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

object Helper {
  val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  def genTimestamp(milliseconds: Long): Timestamp = Timestamp.valueOf(format.format(new Date(milliseconds)))

  def genId(max: Int): Int = (Math.random * max).toInt
}
