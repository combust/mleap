package ml.combust.mleap.executor.repository

import java.net.URI

import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.Transformer
import resource._

import scala.concurrent.{ExecutionContext, Future}

/** Downloads bundles using a Repository and then
  * loads the file into memory for execution.
  *
  * @param repository repository to use as source of bundles
  * @param diskEc execution context for reading bundles
  * @param context MleapContext to use for loading bundles
  */
class RepositoryBundleLoader(repository: Repository,
                             diskEc: ExecutionContext)
                            (implicit context: MleapContext = MleapContext.defaultContext) {
  def loadBundle(uri: URI): Future[Bundle[Transformer]] = {
    implicit val ec: ExecutionContext = diskEc

    repository.downloadBundle(uri).flatMap {
      path =>
        Future.fromTry((for(bf <- managed(BundleFile(path.toFile))) yield {
          bf.loadMleapBundle()
        }).tried.flatMap(identity))
    }
  }
}
