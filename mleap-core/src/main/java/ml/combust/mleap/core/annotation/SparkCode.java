package ml.combust.mleap.core.annotation;

/** Annotation used to mark where we took code from Spark.
 */
public @interface SparkCode {
    public String uri();
}
