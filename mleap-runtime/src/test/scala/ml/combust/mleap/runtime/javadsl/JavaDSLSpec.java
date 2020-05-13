package ml.combust.mleap.runtime.javadsl;

import ml.combust.mleap.core.feature.HandleInvalid$;
import ml.combust.mleap.core.feature.StringIndexerModel;
import ml.combust.mleap.core.types.*;
import ml.combust.mleap.runtime.MleapContext;
import ml.combust.mleap.runtime.frame.DefaultLeapFrame;
import ml.combust.mleap.runtime.frame.Row;
import ml.combust.mleap.runtime.frame.RowTransformer;
import ml.combust.mleap.runtime.frame.Transformer;
import ml.combust.mleap.runtime.transformer.feature.StringIndexer;
import org.apache.spark.ml.linalg.Vectors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import ml.combust.mleap.tensor.ByteString;
import scala.collection.JavaConversions;
import scala.collection.immutable.ListMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JavaDSLSpec {

    LeapFrameBuilder frameBuilder = new LeapFrameBuilder();
    LeapFrameSupport leapFrameSupport = new LeapFrameSupport();
    RowTransformerSupport rowTransformerSupport = new RowTransformerSupport();
    TensorSupport tensorSupport = new TensorSupport();

    Row row = frameBuilder.createRow(true, "hello", (byte) 1,
            (short) 2, 3, (long) 4, 34.5f, 44.5, new ByteString("hello_there".getBytes()),
            Arrays.asList(23, 44, 55), Vectors.dense(new double[]{23, 3, 4}));

    StringIndexer stringIndexer = new StringIndexer(
            "string_indexer",
            new NodeShape(new ListMap<>(), new ListMap<>()).
                    withStandardInput("string").
                    withStandardOutput("string_index"),
            new StringIndexerModel(JavaConversions.asScalaBuffer(Collections.singletonList("hello")).toSeq(),
                    HandleInvalid$.MODULE$.fromString("error", true)));

    DefaultLeapFrame buildFrame() {
        List<StructField> fields = Arrays.asList(frameBuilder.createField("bool", frameBuilder.createBoolean()),
                frameBuilder.createField("string", frameBuilder.createString()),
                frameBuilder.createField("byte", frameBuilder.createByte()),
                frameBuilder.createField("short", frameBuilder.createShort()),
                frameBuilder.createField("int", frameBuilder.createInt()),
                frameBuilder.createField("long", frameBuilder.createLong()),
                frameBuilder.createField("float", frameBuilder.createFloat()),
                frameBuilder.createField("double", frameBuilder.createDouble()),
                frameBuilder.createField("byte_string", frameBuilder.createByteString()),
                frameBuilder.createField("list", frameBuilder.createList(frameBuilder.createBasicLong())),
                frameBuilder.createField("tensor", frameBuilder.createTensor(frameBuilder.createBasicDouble())));
        StructType schema = frameBuilder.createSchema(fields);
        return frameBuilder.createFrame(schema, Arrays.asList(row));
    }

    @Test
    public void buildALeapFrameWithAllDataTypes() {
        DefaultLeapFrame frame = buildFrame();
        StructType schema = frame.schema();

        assertEquals(schema.getField("bool").get(), frameBuilder.createField("bool", frameBuilder.createBoolean()));
        assertEquals(schema.getField("string").get(), frameBuilder.createField("string", frameBuilder.createString()));
        assertEquals(schema.getField("byte").get(), frameBuilder.createField("byte", frameBuilder.createByte()));
        assertEquals(schema.getField("short").get(), frameBuilder.createField("short", frameBuilder.createShort()));
        assertEquals(schema.getField("int").get(), frameBuilder.createField("int", frameBuilder.createInt()));
        assertEquals(schema.getField("long").get(), frameBuilder.createField("long", frameBuilder.createLong()));
        assertEquals(schema.getField("float").get(), frameBuilder.createField("float", frameBuilder.createFloat()));
        assertEquals(schema.getField("double").get(), frameBuilder.createField("double", frameBuilder.createDouble()));
        assertEquals(schema.getField("byte_string").get(), frameBuilder.createField("byte_string", frameBuilder.createByteString()));
        assertEquals(schema.getField("list").get(), frameBuilder.createField("list", frameBuilder.createList(frameBuilder.createBasicLong())));
        assertEquals(schema.getField("tensor").get(), frameBuilder.createField("tensor", frameBuilder.createTensor(frameBuilder.createBasicDouble())));

        Row row = frame.dataset().head();
        assertTrue(row.getBool(0));
        assertEquals(row.getString(1), "hello");
        assertEquals(row.getByte(2), (byte) 1);
        assertEquals(row.getShort(3), (short) 2);
        assertEquals(row.getInt(4), 3);
        assertEquals(row.getLong(5), 4L);
        assertEquals(row.getFloat(6), 34.5f, 0.0000000000001);
        assertEquals(row.getDouble(7), 44.5, 0.0000000000001);
        assertEquals(row.getByteString(8), new ByteString("hello_there".getBytes()));
        assertEquals(row.getList(9), Arrays.asList(23, 44, 55));
        List<Double> tensorValues = tensorSupport.toArray(row.getTensor(10));
        assertEquals(tensorValues, Arrays.asList(23d, 3d, 4d));
    }

    @Test
    public void transformASingleRowUsingARowTransformer() {
        DefaultLeapFrame frame = buildFrame();
        RowTransformer rowTransformer = stringIndexer.transform(rowTransformerSupport.createRowTransformer(frame.schema())).get();
        Row result = rowTransformer.transform(row);
        assertEquals(result.getDouble(11), 0.0, 0.0000000000001);
    }

    @Test
    public void createTensorFieldWithDimension() {
        StructField tensorField = frameBuilder.createField("tensor", frameBuilder.createTensor(frameBuilder.createBasicByte(), Arrays.asList(1, 2), true));
        assertEquals(((TensorType)tensorField.dataType()).dimensions().get(), JavaConversions.asScalaBuffer(Arrays.asList(1, 2)).toSeq());
    }

    @Test
    public void collectAllRowsToJavaList() {
        DefaultLeapFrame frame = buildFrame();
        List<Row> rows = leapFrameSupport.collect(frame);
        assertEquals(rows.size(), 1);
    }

    @Test
    public void selectFieldsGivenJavaList() {
        DefaultLeapFrame frame = buildFrame();
        DefaultLeapFrame smallerFrame = leapFrameSupport.select(frame, Arrays.asList("string", "bool"));
        assertTrue(smallerFrame.schema().getField("bool").nonEmpty());
        assertTrue(smallerFrame.schema().getField("string").nonEmpty());
        assertTrue(smallerFrame.schema().getField("int").isEmpty());
    }

    @Test
    public void dropFieldsGivenJavaList() {
        DefaultLeapFrame frame = buildFrame();
        DefaultLeapFrame smallerFrame = leapFrameSupport.drop(frame, Arrays.asList("string", "bool"));
        assertTrue(smallerFrame.schema().getField("bool").isEmpty());
        assertTrue(smallerFrame.schema().getField("string").isEmpty());
        assertTrue(smallerFrame.schema().getField("int").nonEmpty());
    }

    @Test
    public void getFieldsFromSchema() {
      DefaultLeapFrame frame = buildFrame();
      List<StructField> fields = leapFrameSupport.getFields(frame.schema());
      assertEquals(fields.size(), 11);
    }

    @Test
    public void serializeDeserializeMLeapTransformerAndTransform() throws IOException {
        File file = new File(Files.createTempDirectory("mleap").toFile(), "model.zip");
        MleapContext context = new ContextBuilder().createMleapContext();
        BundleBuilder bundleBuilder = new BundleBuilder();
        bundleBuilder.save(stringIndexer, file, context);
        Transformer transformer = bundleBuilder.load(file, context).root();
        DefaultLeapFrame frame = buildFrame();

        DefaultLeapFrame frame2 = transformer.transform(frame).get();
        assertEquals(leapFrameSupport.select(frame2, Arrays.asList("string_index")).dataset().head().getDouble(0), 0.0, 0.0000000000001);
    }
}
