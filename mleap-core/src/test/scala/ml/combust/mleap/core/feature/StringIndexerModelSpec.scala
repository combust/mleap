package ml.combust.mleap.core.feature

import org.scalatest.FunSpec
import org.scalatest.prop.TableDrivenPropertyChecks

/**
  * Created by hwilkins on 1/21/16.
  */
class StringIndexerModelSpec extends FunSpec with TableDrivenPropertyChecks {
  describe("#apply") {
    it("returns the index of the string") {
      val indexer = StringIndexerModel(Array("hello", "there", "dude"))

      assert(indexer("hello") == 0.0)
      assert(indexer("there") == 1.0)
      assert(indexer("dude") == 2.0)
    }

    it("returns the index of Optional string") {
      val indexer = StringIndexerModel(Array("hello", "there", "dude"), inputNullable = true)

      assert(indexer(Some("hello")) == 0.0)
      assert(indexer(Some("there")) == 1.0)
      assert(indexer(Some("dude")) == 2.0)
    }

    it("throws NullPointerException when encounters NULL/None and handleInvalid is not keep") {
      val indexer = StringIndexerModel(Array("hello"))
      val nullLabels = Table(null, None)

      forAll(nullLabels) { (label: Any) =>
        intercept[NullPointerException] {
          indexer(label)
        }
      }
    }

    it("throws NoSuchElementException when encounters unseen label and handleInvalid is not keep") {
      val indexer = StringIndexerModel(Array("hello"))
      val unseenLabels = Table("unknown1", Some("unknown2"))

      forAll(unseenLabels) { (label: Any) =>
        intercept[NoSuchElementException] {
          indexer(label)
        }
      }
    }

    it("returns default index for HandleInvalid.keep mode") {
      val indexer = StringIndexerModel(Array("hello", "there", "dude"), handleInvalid = HandleInvalid.Keep)
      val invalidLabels = Table("unknown", Some("other unknown"), null, None)

      forAll(invalidLabels) { (label: Any) =>
        assert(indexer(label) == 3.0)
      }
    }
  }
}
