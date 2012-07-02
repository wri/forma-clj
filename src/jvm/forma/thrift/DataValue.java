/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package forma.thrift;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataValue extends org.apache.thrift.TUnion<DataValue, DataValue._Fields> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("DataValue");
  private static final org.apache.thrift.protocol.TField DOUBLE_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("doubleVal", org.apache.thrift.protocol.TType.DOUBLE, (short)1);
  private static final org.apache.thrift.protocol.TField INT_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("intVal", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField LONG_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("longVal", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField SHORT_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("shortVal", org.apache.thrift.protocol.TType.I16, (short)4);
  private static final org.apache.thrift.protocol.TField FIRE_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("fireVal", org.apache.thrift.protocol.TType.STRUCT, (short)5);
  private static final org.apache.thrift.protocol.TField TIME_SERIES_FIELD_DESC = new org.apache.thrift.protocol.TField("timeSeries", org.apache.thrift.protocol.TType.STRUCT, (short)6);
  private static final org.apache.thrift.protocol.TField VALS_FIELD_DESC = new org.apache.thrift.protocol.TField("vals", org.apache.thrift.protocol.TType.STRUCT, (short)7);
  private static final org.apache.thrift.protocol.TField FORMA_FIELD_DESC = new org.apache.thrift.protocol.TField("forma", org.apache.thrift.protocol.TType.STRUCT, (short)8);

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DOUBLE_VAL((short)1, "doubleVal"),
    INT_VAL((short)2, "intVal"),
    LONG_VAL((short)3, "longVal"),
    SHORT_VAL((short)4, "shortVal"),
    FIRE_VAL((short)5, "fireVal"),
    TIME_SERIES((short)6, "timeSeries"),
    VALS((short)7, "vals"),
    FORMA((short)8, "forma");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // DOUBLE_VAL
          return DOUBLE_VAL;
        case 2: // INT_VAL
          return INT_VAL;
        case 3: // LONG_VAL
          return LONG_VAL;
        case 4: // SHORT_VAL
          return SHORT_VAL;
        case 5: // FIRE_VAL
          return FIRE_VAL;
        case 6: // TIME_SERIES
          return TIME_SERIES;
        case 7: // VALS
          return VALS;
        case 8: // FORMA
          return FORMA;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.DOUBLE_VAL, new org.apache.thrift.meta_data.FieldMetaData("doubleVal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.INT_VAL, new org.apache.thrift.meta_data.FieldMetaData("intVal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.LONG_VAL, new org.apache.thrift.meta_data.FieldMetaData("longVal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.SHORT_VAL, new org.apache.thrift.meta_data.FieldMetaData("shortVal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I16)));
    tmpMap.put(_Fields.FIRE_VAL, new org.apache.thrift.meta_data.FieldMetaData("fireVal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, FireValue.class)));
    tmpMap.put(_Fields.TIME_SERIES, new org.apache.thrift.meta_data.FieldMetaData("timeSeries", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TimeSeries.class)));
    tmpMap.put(_Fields.VALS, new org.apache.thrift.meta_data.FieldMetaData("vals", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ArrayValue.class)));
    tmpMap.put(_Fields.FORMA, new org.apache.thrift.meta_data.FieldMetaData("forma", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, FormaValue.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(DataValue.class, metaDataMap);
  }

  public DataValue() {
    super();
  }

  public DataValue(_Fields setField, Object value) {
    super(setField, value);
  }

  public DataValue(DataValue other) {
    super(other);
  }
  public DataValue deepCopy() {
    return new DataValue(this);
  }

  public static DataValue doubleVal(double value) {
    DataValue x = new DataValue();
    x.setDoubleVal(value);
    return x;
  }

  public static DataValue intVal(int value) {
    DataValue x = new DataValue();
    x.setIntVal(value);
    return x;
  }

  public static DataValue longVal(long value) {
    DataValue x = new DataValue();
    x.setLongVal(value);
    return x;
  }

  public static DataValue shortVal(short value) {
    DataValue x = new DataValue();
    x.setShortVal(value);
    return x;
  }

  public static DataValue fireVal(FireValue value) {
    DataValue x = new DataValue();
    x.setFireVal(value);
    return x;
  }

  public static DataValue timeSeries(TimeSeries value) {
    DataValue x = new DataValue();
    x.setTimeSeries(value);
    return x;
  }

  public static DataValue vals(ArrayValue value) {
    DataValue x = new DataValue();
    x.setVals(value);
    return x;
  }

  public static DataValue forma(FormaValue value) {
    DataValue x = new DataValue();
    x.setForma(value);
    return x;
  }


  @Override
  protected void checkType(_Fields setField, Object value) throws ClassCastException {
    switch (setField) {
      case DOUBLE_VAL:
        if (value instanceof Double) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Double for field 'doubleVal', but got " + value.getClass().getSimpleName());
      case INT_VAL:
        if (value instanceof Integer) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Integer for field 'intVal', but got " + value.getClass().getSimpleName());
      case LONG_VAL:
        if (value instanceof Long) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Long for field 'longVal', but got " + value.getClass().getSimpleName());
      case SHORT_VAL:
        if (value instanceof Short) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Short for field 'shortVal', but got " + value.getClass().getSimpleName());
      case FIRE_VAL:
        if (value instanceof FireValue) {
          break;
        }
        throw new ClassCastException("Was expecting value of type FireValue for field 'fireVal', but got " + value.getClass().getSimpleName());
      case TIME_SERIES:
        if (value instanceof TimeSeries) {
          break;
        }
        throw new ClassCastException("Was expecting value of type TimeSeries for field 'timeSeries', but got " + value.getClass().getSimpleName());
      case VALS:
        if (value instanceof ArrayValue) {
          break;
        }
        throw new ClassCastException("Was expecting value of type ArrayValue for field 'vals', but got " + value.getClass().getSimpleName());
      case FORMA:
        if (value instanceof FormaValue) {
          break;
        }
        throw new ClassCastException("Was expecting value of type FormaValue for field 'forma', but got " + value.getClass().getSimpleName());
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected Object standardSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TField field) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(field.id);
    if (setField != null) {
      switch (setField) {
        case DOUBLE_VAL:
          if (field.type == DOUBLE_VAL_FIELD_DESC.type) {
            Double doubleVal;
            doubleVal = iprot.readDouble();
            return doubleVal;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case INT_VAL:
          if (field.type == INT_VAL_FIELD_DESC.type) {
            Integer intVal;
            intVal = iprot.readI32();
            return intVal;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case LONG_VAL:
          if (field.type == LONG_VAL_FIELD_DESC.type) {
            Long longVal;
            longVal = iprot.readI64();
            return longVal;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case SHORT_VAL:
          if (field.type == SHORT_VAL_FIELD_DESC.type) {
            Short shortVal;
            shortVal = iprot.readI16();
            return shortVal;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case FIRE_VAL:
          if (field.type == FIRE_VAL_FIELD_DESC.type) {
            FireValue fireVal;
            fireVal = new FireValue();
            fireVal.read(iprot);
            return fireVal;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case TIME_SERIES:
          if (field.type == TIME_SERIES_FIELD_DESC.type) {
            TimeSeries timeSeries;
            timeSeries = new TimeSeries();
            timeSeries.read(iprot);
            return timeSeries;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case VALS:
          if (field.type == VALS_FIELD_DESC.type) {
            ArrayValue vals;
            vals = new ArrayValue();
            vals.read(iprot);
            return vals;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case FORMA:
          if (field.type == FORMA_FIELD_DESC.type) {
            FormaValue forma;
            forma = new FormaValue();
            forma.read(iprot);
            return forma;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        default:
          throw new IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      return null;
    }
  }

  @Override
  protected void standardSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case DOUBLE_VAL:
        Double doubleVal = (Double)value_;
        oprot.writeDouble(doubleVal);
        return;
      case INT_VAL:
        Integer intVal = (Integer)value_;
        oprot.writeI32(intVal);
        return;
      case LONG_VAL:
        Long longVal = (Long)value_;
        oprot.writeI64(longVal);
        return;
      case SHORT_VAL:
        Short shortVal = (Short)value_;
        oprot.writeI16(shortVal);
        return;
      case FIRE_VAL:
        FireValue fireVal = (FireValue)value_;
        fireVal.write(oprot);
        return;
      case TIME_SERIES:
        TimeSeries timeSeries = (TimeSeries)value_;
        timeSeries.write(oprot);
        return;
      case VALS:
        ArrayValue vals = (ArrayValue)value_;
        vals.write(oprot);
        return;
      case FORMA:
        FormaValue forma = (FormaValue)value_;
        forma.write(oprot);
        return;
      default:
        throw new IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected Object tupleSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, short fieldID) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(fieldID);
    if (setField != null) {
      switch (setField) {
        case DOUBLE_VAL:
          Double doubleVal;
          doubleVal = iprot.readDouble();
          return doubleVal;
        case INT_VAL:
          Integer intVal;
          intVal = iprot.readI32();
          return intVal;
        case LONG_VAL:
          Long longVal;
          longVal = iprot.readI64();
          return longVal;
        case SHORT_VAL:
          Short shortVal;
          shortVal = iprot.readI16();
          return shortVal;
        case FIRE_VAL:
          FireValue fireVal;
          fireVal = new FireValue();
          fireVal.read(iprot);
          return fireVal;
        case TIME_SERIES:
          TimeSeries timeSeries;
          timeSeries = new TimeSeries();
          timeSeries.read(iprot);
          return timeSeries;
        case VALS:
          ArrayValue vals;
          vals = new ArrayValue();
          vals.read(iprot);
          return vals;
        case FORMA:
          FormaValue forma;
          forma = new FormaValue();
          forma.read(iprot);
          return forma;
        default:
          throw new IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      return null;
    }
  }

  @Override
  protected void tupleSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case DOUBLE_VAL:
        Double doubleVal = (Double)value_;
        oprot.writeDouble(doubleVal);
        return;
      case INT_VAL:
        Integer intVal = (Integer)value_;
        oprot.writeI32(intVal);
        return;
      case LONG_VAL:
        Long longVal = (Long)value_;
        oprot.writeI64(longVal);
        return;
      case SHORT_VAL:
        Short shortVal = (Short)value_;
        oprot.writeI16(shortVal);
        return;
      case FIRE_VAL:
        FireValue fireVal = (FireValue)value_;
        fireVal.write(oprot);
        return;
      case TIME_SERIES:
        TimeSeries timeSeries = (TimeSeries)value_;
        timeSeries.write(oprot);
        return;
      case VALS:
        ArrayValue vals = (ArrayValue)value_;
        vals.write(oprot);
        return;
      case FORMA:
        FormaValue forma = (FormaValue)value_;
        forma.write(oprot);
        return;
      default:
        throw new IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TField getFieldDesc(_Fields setField) {
    switch (setField) {
      case DOUBLE_VAL:
        return DOUBLE_VAL_FIELD_DESC;
      case INT_VAL:
        return INT_VAL_FIELD_DESC;
      case LONG_VAL:
        return LONG_VAL_FIELD_DESC;
      case SHORT_VAL:
        return SHORT_VAL_FIELD_DESC;
      case FIRE_VAL:
        return FIRE_VAL_FIELD_DESC;
      case TIME_SERIES:
        return TIME_SERIES_FIELD_DESC;
      case VALS:
        return VALS_FIELD_DESC;
      case FORMA:
        return FORMA_FIELD_DESC;
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TStruct getStructDesc() {
    return STRUCT_DESC;
  }

  @Override
  protected _Fields enumForId(short id) {
    return _Fields.findByThriftIdOrThrow(id);
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }


  public double getDoubleVal() {
    if (getSetField() == _Fields.DOUBLE_VAL) {
      return (Double)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'doubleVal' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setDoubleVal(double value) {
    setField_ = _Fields.DOUBLE_VAL;
    value_ = value;
  }

  public int getIntVal() {
    if (getSetField() == _Fields.INT_VAL) {
      return (Integer)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'intVal' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setIntVal(int value) {
    setField_ = _Fields.INT_VAL;
    value_ = value;
  }

  public long getLongVal() {
    if (getSetField() == _Fields.LONG_VAL) {
      return (Long)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'longVal' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setLongVal(long value) {
    setField_ = _Fields.LONG_VAL;
    value_ = value;
  }

  public short getShortVal() {
    if (getSetField() == _Fields.SHORT_VAL) {
      return (Short)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'shortVal' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setShortVal(short value) {
    setField_ = _Fields.SHORT_VAL;
    value_ = value;
  }

  public FireValue getFireVal() {
    if (getSetField() == _Fields.FIRE_VAL) {
      return (FireValue)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'fireVal' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setFireVal(FireValue value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.FIRE_VAL;
    value_ = value;
  }

  public TimeSeries getTimeSeries() {
    if (getSetField() == _Fields.TIME_SERIES) {
      return (TimeSeries)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'timeSeries' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setTimeSeries(TimeSeries value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.TIME_SERIES;
    value_ = value;
  }

  public ArrayValue getVals() {
    if (getSetField() == _Fields.VALS) {
      return (ArrayValue)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'vals' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setVals(ArrayValue value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.VALS;
    value_ = value;
  }

  public FormaValue getForma() {
    if (getSetField() == _Fields.FORMA) {
      return (FormaValue)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'forma' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setForma(FormaValue value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.FORMA;
    value_ = value;
  }

  public boolean isSetDoubleVal() {
    return setField_ == _Fields.DOUBLE_VAL;
  }


  public boolean isSetIntVal() {
    return setField_ == _Fields.INT_VAL;
  }


  public boolean isSetLongVal() {
    return setField_ == _Fields.LONG_VAL;
  }


  public boolean isSetShortVal() {
    return setField_ == _Fields.SHORT_VAL;
  }


  public boolean isSetFireVal() {
    return setField_ == _Fields.FIRE_VAL;
  }


  public boolean isSetTimeSeries() {
    return setField_ == _Fields.TIME_SERIES;
  }


  public boolean isSetVals() {
    return setField_ == _Fields.VALS;
  }


  public boolean isSetForma() {
    return setField_ == _Fields.FORMA;
  }


  public boolean equals(Object other) {
    if (other instanceof DataValue) {
      return equals((DataValue)other);
    } else {
      return false;
    }
  }

  public boolean equals(DataValue other) {
    return other != null && getSetField() == other.getSetField() && getFieldValue().equals(other.getFieldValue());
  }

  @Override
  public int compareTo(DataValue other) {
    int lastComparison = org.apache.thrift.TBaseHelper.compareTo(getSetField(), other.getSetField());
    if (lastComparison == 0) {
      return org.apache.thrift.TBaseHelper.compareTo(getFieldValue(), other.getFieldValue());
    }
    return lastComparison;
  }


  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    hcb.append(this.getClass().getName());
    org.apache.thrift.TFieldIdEnum setField = getSetField();
    if (setField != null) {
      hcb.append(setField.getThriftFieldId());
      Object value = getFieldValue();
      if (value instanceof org.apache.thrift.TEnum) {
        hcb.append(((org.apache.thrift.TEnum)getFieldValue()).getValue());
      } else {
        hcb.append(value);
      }
    }
    return hcb.toHashCode();
  }
  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }


  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }


}
