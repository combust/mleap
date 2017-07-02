package ml.combust.mleap.runtime.javadsl;

import ml.combust.mleap.core.types.*;
import ml.combust.mleap.runtime.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by hollinwilkins on 4/21/17.
 */
public class LeapFrameBuilder {
    private LeapFrameBuilderSupport support = new LeapFrameBuilderSupport();

    public LeapFrameBuilder() { }

    public DefaultLeapFrame createFrame(StructType schema, LocalDataset dataset) {
        return new DefaultLeapFrame(schema, dataset);
    }

    public StructType createSchema(Iterable<StructField> fields) {
        return StructType$.MODULE$.apply(fields).get();
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

    public LocalDataset createDataset(Iterable<Row> rows) {
        return new LocalDataset(rows);
    }

    public BooleanType createBool() { return createBool(false); }
    public BooleanType createBool(boolean isNullable) {
        return new BooleanType(isNullable);
    }

    public StringType createString() { return createString(false); }
    public StringType createString(boolean isNullable) {
        return new StringType(isNullable);
    }

    public ByteType createByte() { return createByte(false); }
    public ByteType createByte(boolean isNullable) {
        return new ByteType(isNullable);
    }

    public ShortType createShort() { return createShort(false); }
    public ShortType createShort(boolean isNullable) {
        return new ShortType(isNullable);
    }

    public IntegerType createInt() { return createInt(false); }
    public IntegerType createInt(boolean isNullable) {
        return new IntegerType(isNullable);
    }

    public LongType createLong() { return createLong(false); }
    public LongType createLong(boolean isNullable) {
        return new LongType(isNullable);
    }

    public FloatType createFloat() { return createFloat(false); }
    public FloatType createFloat(boolean isNullable) {
        return new FloatType(isNullable);
    }

    public DoubleType createDouble() { return createDouble(false); }
    public DoubleType createDouble(boolean isNullable) {
        return new DoubleType(isNullable);
    }

    public ByteStringType createByteString() { return createByteString(false); }
    public ByteStringType createByteString(boolean isNullable) {
        return new ByteStringType(isNullable);
    }

    public TensorType createTensor(BasicType base) { return createTensor(base, false); }
    public TensorType createTensor(BasicType base, boolean isNullable) { return new TensorType(base, isNullable); }

    public ListType createList(DataType base) { return createList(base, false); }
    public ListType createList(DataType base, boolean isNullable) {
        return new ListType(base, isNullable);
    }
}
