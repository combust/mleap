package ml.combust.mleap

import scala.util.control.NonFatal

/**
  * Created by hollinwilkins on 10/31/16.
  */
object ClassLoaderUtil {
  def findClassLoader(baseName: String): ClassLoader = {
    def findCaller(get: Int ⇒ Class[_]): ClassLoader =
      Iterator.from(2 /*is the magic number, promise*/ ).map(get) dropWhile { c ⇒
        c != null &&
          (c.getName.startsWith(baseName) ||
            c.getName.startsWith("scala.Option") ||
            c.getName.startsWith("scala.collection.Iterator") ||
            c.getName.startsWith("ml.combust.bundle.util.ClassLoaderUtil"))
      } next () match {
        case null => getClass.getClassLoader
        case c => c.getClassLoader
      }

    Option(Thread.currentThread.getContextClassLoader) orElse
      (getCallerClass map findCaller) getOrElse
      getClass.getClassLoader
  }

  val getCallerClass: Option[Int ⇒ Class[_]] = {
    try {
      val c = Class.forName("sun.reflect.Reflection")
      val m = c.getMethod("getCallerClass", Array(classOf[Int]): _*)
      Some((i: Int) ⇒ m.invoke(null, Array[AnyRef](i.asInstanceOf[java.lang.Integer]): _*).asInstanceOf[Class[_]])
    } catch {
      case NonFatal(e) ⇒ None
    }
  }
}
