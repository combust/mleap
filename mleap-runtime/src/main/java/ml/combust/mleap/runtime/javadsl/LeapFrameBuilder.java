package ml.combust.mleap.runtime.javadsl;

import ml.combust.mleap.runtime.frame.DefaultLeapFrame;
import ml.combust.mleap.runtime.frame.Row;
import ml.combust.mleap.core.types.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by hollinwilkins on 4/21/17.
 */
public class LeapFrameBuilder {
    private LeapFrameBuilderSupport support = new LeapFrameBuilderSupport();

    public LeapFrameBuilder() { }

    public DefaultLeapFrame createFrame(StructType schema, Iterable<Row> rows) {
        return new DefaultLeapFrame(schema, rows);
    }

    public StructType createSchema(Iterable<StructField> fields) {
        return StructType$.MODULE$.apply(fields).get();
    }

    public StructType createSchema(String json) {
        return support.createSchema(json);
    }

    public StructField createField(String name, DataType dt) {
        return new StructField(name, dt);
    }

    public Row createRow(Object value, Object... values) {
        List<Object> l = new ArrayList<>(1 + values.length);
        l.add(value);
        l.addAll(Arrays.asList(values));
        return createRowFromIterable(l);
    }

    public Row createRowFromIterable(Iterable<Object> values) {
        return support.createRowFromIterable(values);
    }

    public BasicType createBasicBoolean() { return support.createBoolean(); }
    public BasicType createBasicByte() { return support.createByte(); }
    public BasicType createBasicShort() { return support.createShort(); }
    public BasicType createBasicInt() { return support.createInt(); }
    public BasicType createBasicLong() { return support.createLong(); }
    public BasicType createBasicFloat() { return support.createFloat(); }
    public BasicType createBasicDouble() { return support.createDouble(); }
    public BasicType createBasicString() { return support.createString(); }
    public BasicType createBasicByteString() { return support.createByteString(); }

    public ScalarType createBoolean() { return createBoolean(true); }
    public ScalarType createBoolean(boolean isNullable) {
        return new ScalarType(support.createBoolean(), isNullable);
    }

    public ScalarType createByte() { return createByte(true); }
    public ScalarType createByte(boolean isNullable) {
        return new ScalarType(support.createByte(), isNullable);
    }

    public ScalarType createShort() { return createShort(true); }
    public ScalarType createShort(boolean isNullable) {
        return new ScalarType(support.createShort(), isNullable);
    }

    public ScalarType createInt() { return createInt(true); }
    public ScalarType createInt(boolean isNullable) {
        return new ScalarType(support.createInt(), isNullable);
    }

    public ScalarType createLong() { return createLong(true); }
    public ScalarType createLong(boolean isNullable) {
        return new ScalarType(support.createLong(), isNullable);
    }

    public ScalarType createFloat() { return createFloat(true); }
    public ScalarType createFloat(boolean isNullable) {
        return new ScalarType(support.createFloat(), isNullable);
    }

    public ScalarType createDouble() { return createDouble(true); }
    public ScalarType createDouble(boolean isNullable) {
        return new ScalarType(support.createDouble(), isNullable);
    }

    public ScalarType createString() { return createString(true); }
    public ScalarType createString(boolean isNullable) { return new ScalarType(support.createString(), isNullable); }

    public ScalarType createByteString() { return createByteString(true); }
    public ScalarType createByteString(boolean isNullable) { return new ScalarType(support.createByteString(), isNullable); }

    public TensorType createTensor(BasicType base) { return createTensor(base, true); }
    public TensorType createTensor(BasicType base, boolean isNullable) { return new TensorType(base, isNullable); }
    public TensorType createTensor(BasicType base, List<Integer> dims, boolean isNullable) {
        return new TensorType(base, support.createTensorDimensions(dims), isNullable);}

    public ListType createList(BasicType base) { return createList(base, true); }
    public ListType createList(BasicType base, boolean isNullable) {
        return new ListType(base, isNullable);
    }

    public MapType createMap(BasicType key, BasicType base) { return createMap(key, base, true); }
    public MapType createMap(BasicType key, BasicType base, boolean isNullable) {
        return new MapType(key, base, isNullable);
    }
}
