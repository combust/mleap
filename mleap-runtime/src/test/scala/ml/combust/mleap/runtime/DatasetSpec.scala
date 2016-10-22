package ml.combust.mleap.runtime

import org.scalatest.FunSpec

/** Base trait for testing implementations of Dataset.
  *
  * @tparam D dataset type
  */
trait DatasetSpec[D <: Dataset] extends FunSpec {
  val row1 = Row(42, "hey")
  val row2 = Row(13, "there")
  val dataset = create(row1, row2)

  def create(rows: Row *): D

  def dataset(name: String): Unit = {
    describe(name) {
      describe("#update") {
        it("creates a new dataset with updated rows") {
          val dataset2 = dataset.update(r => Row(55, r.get(1)))
          val rowData = dataset2.toArray

          assert(rowData(0).toSeq == Seq(55, "hey"))
          assert(rowData(1).toSeq == Seq(55, "there"))
        }
      }

      describe("#withValue") {
        describe("with a user defined function") {
          it("created a new dataset with the calculated value from the user defined function") {
            val dataset2 = dataset.withValue(r => r.get(1), r => r.get(0)) {
              (v1: String, v2: Int) => s"$v1:$v2"
            }
            val data = dataset2.toArray.map(r => r.getString(2))

            assert(data sameElements Array("hey:42", "there:13"))
          }
        }
      }

      describe("#withValues") {
        it("creates a new dataset with additional values in every row") {
          val dataset2 = dataset.withValues(r => Row(s"${r.getString(1)}:${r.getInt(0)}", 78 + r.getInt(0)))
          val data = dataset2.toArray.map(r => (r.getString(2), r.getInt(3)))

          assert(data sameElements Array(("hey:42", 120), ("there:13", 91)))
        }
      }

      describe("#selectIndices") {
        it("creates a new dataset with the selected indices") {
          val dataset2 = dataset.selectIndices(0)
          for(r <- dataset2.toArray) {
            assert(r.toArray.length == 1)
          }
          val data = dataset2.toArray.map(r => r.getInt(0))

          assert(data sameElements Array(42, 13))
        }
      }

      describe("#dropIndex") {
        it("creates a new dataset without an index") {
          val dataset2 = dataset.dropIndex(0)
          for(r <- dataset2.toArray) {
            assert(r.toArray.length == 1)
          }
          val data = dataset2.toArray.map(r => r.getString(0))

          assert(data sameElements Array("hey", "there"))
        }
      }

      describe("#toLocal") {
        it("converts to a LocalDataset") {
          val local = dataset.toLocal
          val localData = local.toArray.clone().flatMap(_.toArray)
          val datasetData = dataset.toArray.flatMap(_.toArray)

          assert(local.isInstanceOf[LocalDataset])
          assert(localData sameElements datasetData)
        }
      }

      describe("#toArray") {
        it("converts the dataset to an Array[Row]") {
          val arr = dataset.toArray.flatMap(_.toArray)
          val expected = Array(42, "hey", 13, "there")

          assert(arr sameElements expected)
        }
      }
    }
  }
}

class LocalDatasetSpec extends DatasetSpec[LocalDataset] {
  override def create(rows: Row *): LocalDataset = LocalDataset(rows.toArray)

  it should behave like dataset("LocalDataset")
}
