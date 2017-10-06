package ml.combust.mleap.runtime.javadsl;

import ml.combust.bundle.dsl.Bundle;
import ml.combust.mleap.runtime.MleapContext;
import ml.combust.mleap.runtime.frame.Transformer;

import java.io.File;

/**
 * Created by hollinwilkins on 4/21/17.
 */
public class BundleBuilder {
    private BundleBuilderSupport support = new BundleBuilderSupport();

    public BundleBuilder() { }

    public Bundle<Transformer> load(File file, MleapContext context) {
        return support.load(file, context);
    }

    public void save(Transformer transformer, File file, MleapContext context) {
        support.save(transformer, file, context);
    }
}
