package com.adevinta.zc.config

final case class Secret(value: String) {
  override def toString: String = "****"
}
