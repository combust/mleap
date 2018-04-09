package ml.combust.mleap.springboot

import ml.combust.mleap.executor.MleapExecutor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation._

@RestController
@RequestMapping
class ScoringController(@Autowired val mleapExecutor: MleapExecutor) {

  @GetMapping(path = Array("/bundleMeta"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def getBundleMeta(@RequestParam uri: String) = {
    // todo
  }
}