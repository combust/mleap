import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.ExecCmd
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._

object DockerConfig {
  val baseSettings = Seq(Docker / daemonUser  := "root",
    dockerExposedPorts := Seq(65327, 65328),
    // https://github.com/docker-library/openjdk/issues/505
    dockerBaseImage := "eclipse-temurin:11-jre-jammy",
    dockerRepository := Some("combustml"),
    dockerBuildOptions := Seq("-t", dockerAlias.value.toString),
    dockerCommands := dockerCommands.value.filterNot {
      case ExecCmd("RUN", args @ _*) => args.contains("chown")
      case cmd => false
    })
}