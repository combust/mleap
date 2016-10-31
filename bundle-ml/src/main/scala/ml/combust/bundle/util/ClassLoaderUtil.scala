package ml.combust.bundle.util

/**
  * Created by hollinwilkins on 10/31/16.
  */
object ClassLoaderUtil {
  def resolveClassLoader(classLoader: Option[ClassLoader] = None): ClassLoader = {
    classLoader.orElse(Some(Thread.currentThread().getContextClassLoader)).
      getOrElse(getClass.getClassLoader)
  }
}
