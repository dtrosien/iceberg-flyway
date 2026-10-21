package com.example.icebergflyway

import org.scalatest.{Inside, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

abstract class UnitSpec extends AnyFlatSpec with should.Matchers with OptionValues with Inside
