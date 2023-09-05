package ml.combust.mleap.executor.repository

import java.net.URI

import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.Transformer
import scala.util.Using

import scala.concurrent.{ExecutionContext, Future}

/** Downloads bundles using a Repository and then
  * loads the file into memory for execution.
  *
  * @param repository repository to use as source of bundles
  * @param diskEc execution context for disk operations
  * @param context MleapContext to use for loading bundles
  */
class RepositoryBundleLoader(repository: Repository,
                             implicit val diskEc: ExecutionContext)
                            (implicit context: MleapContext = MleapContext.defaultContext) {
  def loadBundle(uri: URI): Future[Bundle[Transformer]] = {

    repository.downloadBundle(uri).flatMap {
      path =>
        Future.fromTry(Using(BundleFile(path.toFile)) { bf =>
          bf.loadMleapBundle()
        }.flatten)
    }
  }
}
