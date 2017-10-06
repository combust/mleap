package ml.combust.mleap.runtime.javadsl;

import ml.combust.bundle.BundleRegistry;
import ml.combust.bundle.BundleRegistry$;
import ml.combust.mleap.runtime.MleapContext;
import ml.combust.mleap.runtime.MleapContext$;

/**
 * Created by hollinwilkins on 4/21/17.
 */
public class ContextBuilder {
    public ContextBuilder() { }

    public BundleRegistry loadRegistry(String path) {
        return BundleRegistry$.MODULE$.apply(path);
    }

    public MleapContext createMleapContext() {
        return MleapContext$.MODULE$.apply();
    }

    public MleapContext createMleapContext(BundleRegistry registry) {
        return MleapContext$.MODULE$.apply(registry);
    }
}
