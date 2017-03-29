package com.box.castle.core

import java.io.File
import scala.util.control.NonFatal



package object util {
  def join(a: String, b: String): String = {
    new File(new File(a), b).getPath
  }
}
