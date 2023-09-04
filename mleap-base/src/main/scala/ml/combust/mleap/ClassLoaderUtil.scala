package ml.combust.mleap


import scala.jdk.OptionConverters.RichOptional

/**
 * Created by hollinwilkins on 10/31/16.
 */
object ClassLoaderUtil {
  private val walker: StackWalker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)

  /**
   * refer to the [[StackWalker]]'s `getCallerClass` javadoc.
   */
  private val MAGIC = 2

  def findClassLoader(baseName: String): ClassLoader = {
    def findCaller: Option[ClassLoader] = {

      walker.walk(s => s.skip(MAGIC).dropWhile(clz => {
        val c = clz.getDeclaringClass.getCanonicalName
        c != null &&
          (c.startsWith(baseName) ||
            c.startsWith("scala.Option") ||
            c.startsWith("ml.combust.mleap.ClassLoaderUtil"))
        })
        .findFirst()
        .toScala
        .map(_.getDeclaringClass.getClassLoader))
    }

    Option(Thread.currentThread.getContextClassLoader) orElse
      findCaller getOrElse
      walker.getCallerClass.getClassLoader
  }
}
