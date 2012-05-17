/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package forma.schema;

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

public class Chunk implements org.apache.thrift.TBase<Chunk, Chunk._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Chunk");

  private static final org.apache.thrift.protocol.TField DATASET_FIELD_DESC = new org.apache.thrift.protocol.TField("dataset", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TRES_FIELD_DESC = new org.apache.thrift.protocol.TField("tres", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField SRES_FIELD_DESC = new org.apache.thrift.protocol.TField("sres", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField H_FIELD_DESC = new org.apache.thrift.protocol.TField("h", org.apache.thrift.protocol.TType.I32, (short)4);
  private static final org.apache.thrift.protocol.TField V_FIELD_DESC = new org.apache.thrift.protocol.TField("v", org.apache.thrift.protocol.TType.I32, (short)5);
  private static final org.apache.thrift.protocol.TField ID_FIELD_DESC = new org.apache.thrift.protocol.TField("id", org.apache.thrift.protocol.TType.I32, (short)6);
  private static final org.apache.thrift.protocol.TField SIZE_FIELD_DESC = new org.apache.thrift.protocol.TField("size", org.apache.thrift.protocol.TType.I32, (short)7);
  private static final org.apache.thrift.protocol.TField DATA_FIELD_DESC = new org.apache.thrift.protocol.TField("data", org.apache.thrift.protocol.TType.STRUCT, (short)8);
  private static final org.apache.thrift.protocol.TField DATE_FIELD_DESC = new org.apache.thrift.protocol.TField("date", org.apache.thrift.protocol.TType.STRING, (short)9);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ChunkStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ChunkTupleSchemeFactory());
  }

  public String dataset; // required
  public String tres; // required
  public String sres; // required
  public int h; // required
  public int v; // required
  public int id; // required
  public int size; // required
  public Data data; // required
  public String date; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DATASET((short)1, "dataset"),
    TRES((short)2, "tres"),
    SRES((short)3, "sres"),
    H((short)4, "h"),
    V((short)5, "v"),
    ID((short)6, "id"),
    SIZE((short)7, "size"),
    DATA((short)8, "data"),
    DATE((short)9, "date");

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
        case 1: // DATASET
          return DATASET;
        case 2: // TRES
          return TRES;
        case 3: // SRES
          return SRES;
        case 4: // H
          return H;
        case 5: // V
          return V;
        case 6: // ID
          return ID;
        case 7: // SIZE
          return SIZE;
        case 8: // DATA
          return DATA;
        case 9: // DATE
          return DATE;
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

  // isset id assignments
  private static final int __H_ISSET_ID = 0;
  private static final int __V_ISSET_ID = 1;
  private static final int __ID_ISSET_ID = 2;
  private static final int __SIZE_ISSET_ID = 3;
  private BitSet __isset_bit_vector = new BitSet(4);
  private _Fields optionals[] = {_Fields.DATE};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.DATASET, new org.apache.thrift.meta_data.FieldMetaData("dataset", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TRES, new org.apache.thrift.meta_data.FieldMetaData("tres", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.SRES, new org.apache.thrift.meta_data.FieldMetaData("sres", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.H, new org.apache.thrift.meta_data.FieldMetaData("h", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.V, new org.apache.thrift.meta_data.FieldMetaData("v", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.ID, new org.apache.thrift.meta_data.FieldMetaData("id", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.SIZE, new org.apache.thrift.meta_data.FieldMetaData("size", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.DATA, new org.apache.thrift.meta_data.FieldMetaData("data", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Data.class)));
    tmpMap.put(_Fields.DATE, new org.apache.thrift.meta_data.FieldMetaData("date", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Chunk.class, metaDataMap);
  }

  public Chunk() {
  }

  public Chunk(
    String dataset,
    String tres,
    String sres,
    int h,
    int v,
    int id,
    int size,
    Data data)
  {
    this();
    this.dataset = dataset;
    this.tres = tres;
    this.sres = sres;
    this.h = h;
    setHIsSet(true);
    this.v = v;
    setVIsSet(true);
    this.id = id;
    setIdIsSet(true);
    this.size = size;
    setSizeIsSet(true);
    this.data = data;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Chunk(Chunk other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetDataset()) {
      this.dataset = other.dataset;
    }
    if (other.isSetTres()) {
      this.tres = other.tres;
    }
    if (other.isSetSres()) {
      this.sres = other.sres;
    }
    this.h = other.h;
    this.v = other.v;
    this.id = other.id;
    this.size = other.size;
    if (other.isSetData()) {
      this.data = new Data(other.data);
    }
    if (other.isSetDate()) {
      this.date = other.date;
    }
  }

  public Chunk deepCopy() {
    return new Chunk(this);
  }

  @Override
  public void clear() {
    this.dataset = null;
    this.tres = null;
    this.sres = null;
    setHIsSet(false);
    this.h = 0;
    setVIsSet(false);
    this.v = 0;
    setIdIsSet(false);
    this.id = 0;
    setSizeIsSet(false);
    this.size = 0;
    this.data = null;
    this.date = null;
  }

  public String getDataset() {
    return this.dataset;
  }

  public Chunk setDataset(String dataset) {
    this.dataset = dataset;
    return this;
  }

  public void unsetDataset() {
    this.dataset = null;
  }

  /** Returns true if field dataset is set (has been assigned a value) and false otherwise */
  public boolean isSetDataset() {
    return this.dataset != null;
  }

  public void setDatasetIsSet(boolean value) {
    if (!value) {
      this.dataset = null;
    }
  }

  public String getTres() {
    return this.tres;
  }

  public Chunk setTres(String tres) {
    this.tres = tres;
    return this;
  }

  public void unsetTres() {
    this.tres = null;
  }

  /** Returns true if field tres is set (has been assigned a value) and false otherwise */
  public boolean isSetTres() {
    return this.tres != null;
  }

  public void setTresIsSet(boolean value) {
    if (!value) {
      this.tres = null;
    }
  }

  public String getSres() {
    return this.sres;
  }

  public Chunk setSres(String sres) {
    this.sres = sres;
    return this;
  }

  public void unsetSres() {
    this.sres = null;
  }

  /** Returns true if field sres is set (has been assigned a value) and false otherwise */
  public boolean isSetSres() {
    return this.sres != null;
  }

  public void setSresIsSet(boolean value) {
    if (!value) {
      this.sres = null;
    }
  }

  public int getH() {
    return this.h;
  }

  public Chunk setH(int h) {
    this.h = h;
    setHIsSet(true);
    return this;
  }

  public void unsetH() {
    __isset_bit_vector.clear(__H_ISSET_ID);
  }

  /** Returns true if field h is set (has been assigned a value) and false otherwise */
  public boolean isSetH() {
    return __isset_bit_vector.get(__H_ISSET_ID);
  }

  public void setHIsSet(boolean value) {
    __isset_bit_vector.set(__H_ISSET_ID, value);
  }

  public int getV() {
    return this.v;
  }

  public Chunk setV(int v) {
    this.v = v;
    setVIsSet(true);
    return this;
  }

  public void unsetV() {
    __isset_bit_vector.clear(__V_ISSET_ID);
  }

  /** Returns true if field v is set (has been assigned a value) and false otherwise */
  public boolean isSetV() {
    return __isset_bit_vector.get(__V_ISSET_ID);
  }

  public void setVIsSet(boolean value) {
    __isset_bit_vector.set(__V_ISSET_ID, value);
  }

  public int getId() {
    return this.id;
  }

  public Chunk setId(int id) {
    this.id = id;
    setIdIsSet(true);
    return this;
  }

  public void unsetId() {
    __isset_bit_vector.clear(__ID_ISSET_ID);
  }

  /** Returns true if field id is set (has been assigned a value) and false otherwise */
  public boolean isSetId() {
    return __isset_bit_vector.get(__ID_ISSET_ID);
  }

  public void setIdIsSet(boolean value) {
    __isset_bit_vector.set(__ID_ISSET_ID, value);
  }

  public int getSize() {
    return this.size;
  }

  public Chunk setSize(int size) {
    this.size = size;
    setSizeIsSet(true);
    return this;
  }

  public void unsetSize() {
    __isset_bit_vector.clear(__SIZE_ISSET_ID);
  }

  /** Returns true if field size is set (has been assigned a value) and false otherwise */
  public boolean isSetSize() {
    return __isset_bit_vector.get(__SIZE_ISSET_ID);
  }

  public void setSizeIsSet(boolean value) {
    __isset_bit_vector.set(__SIZE_ISSET_ID, value);
  }

  public Data getData() {
    return this.data;
  }

  public Chunk setData(Data data) {
    this.data = data;
    return this;
  }

  public void unsetData() {
    this.data = null;
  }

  /** Returns true if field data is set (has been assigned a value) and false otherwise */
  public boolean isSetData() {
    return this.data != null;
  }

  public void setDataIsSet(boolean value) {
    if (!value) {
      this.data = null;
    }
  }

  public String getDate() {
    return this.date;
  }

  public Chunk setDate(String date) {
    this.date = date;
    return this;
  }

  public void unsetDate() {
    this.date = null;
  }

  /** Returns true if field date is set (has been assigned a value) and false otherwise */
  public boolean isSetDate() {
    return this.date != null;
  }

  public void setDateIsSet(boolean value) {
    if (!value) {
      this.date = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case DATASET:
      if (value == null) {
        unsetDataset();
      } else {
        setDataset((String)value);
      }
      break;

    case TRES:
      if (value == null) {
        unsetTres();
      } else {
        setTres((String)value);
      }
      break;

    case SRES:
      if (value == null) {
        unsetSres();
      } else {
        setSres((String)value);
      }
      break;

    case H:
      if (value == null) {
        unsetH();
      } else {
        setH((Integer)value);
      }
      break;

    case V:
      if (value == null) {
        unsetV();
      } else {
        setV((Integer)value);
      }
      break;

    case ID:
      if (value == null) {
        unsetId();
      } else {
        setId((Integer)value);
      }
      break;

    case SIZE:
      if (value == null) {
        unsetSize();
      } else {
        setSize((Integer)value);
      }
      break;

    case DATA:
      if (value == null) {
        unsetData();
      } else {
        setData((Data)value);
      }
      break;

    case DATE:
      if (value == null) {
        unsetDate();
      } else {
        setDate((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case DATASET:
      return getDataset();

    case TRES:
      return getTres();

    case SRES:
      return getSres();

    case H:
      return Integer.valueOf(getH());

    case V:
      return Integer.valueOf(getV());

    case ID:
      return Integer.valueOf(getId());

    case SIZE:
      return Integer.valueOf(getSize());

    case DATA:
      return getData();

    case DATE:
      return getDate();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case DATASET:
      return isSetDataset();
    case TRES:
      return isSetTres();
    case SRES:
      return isSetSres();
    case H:
      return isSetH();
    case V:
      return isSetV();
    case ID:
      return isSetId();
    case SIZE:
      return isSetSize();
    case DATA:
      return isSetData();
    case DATE:
      return isSetDate();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Chunk)
      return this.equals((Chunk)that);
    return false;
  }

  public boolean equals(Chunk that) {
    if (that == null)
      return false;

    boolean this_present_dataset = true && this.isSetDataset();
    boolean that_present_dataset = true && that.isSetDataset();
    if (this_present_dataset || that_present_dataset) {
      if (!(this_present_dataset && that_present_dataset))
        return false;
      if (!this.dataset.equals(that.dataset))
        return false;
    }

    boolean this_present_tres = true && this.isSetTres();
    boolean that_present_tres = true && that.isSetTres();
    if (this_present_tres || that_present_tres) {
      if (!(this_present_tres && that_present_tres))
        return false;
      if (!this.tres.equals(that.tres))
        return false;
    }

    boolean this_present_sres = true && this.isSetSres();
    boolean that_present_sres = true && that.isSetSres();
    if (this_present_sres || that_present_sres) {
      if (!(this_present_sres && that_present_sres))
        return false;
      if (!this.sres.equals(that.sres))
        return false;
    }

    boolean this_present_h = true;
    boolean that_present_h = true;
    if (this_present_h || that_present_h) {
      if (!(this_present_h && that_present_h))
        return false;
      if (this.h != that.h)
        return false;
    }

    boolean this_present_v = true;
    boolean that_present_v = true;
    if (this_present_v || that_present_v) {
      if (!(this_present_v && that_present_v))
        return false;
      if (this.v != that.v)
        return false;
    }

    boolean this_present_id = true;
    boolean that_present_id = true;
    if (this_present_id || that_present_id) {
      if (!(this_present_id && that_present_id))
        return false;
      if (this.id != that.id)
        return false;
    }

    boolean this_present_size = true;
    boolean that_present_size = true;
    if (this_present_size || that_present_size) {
      if (!(this_present_size && that_present_size))
        return false;
      if (this.size != that.size)
        return false;
    }

    boolean this_present_data = true && this.isSetData();
    boolean that_present_data = true && that.isSetData();
    if (this_present_data || that_present_data) {
      if (!(this_present_data && that_present_data))
        return false;
      if (!this.data.equals(that.data))
        return false;
    }

    boolean this_present_date = true && this.isSetDate();
    boolean that_present_date = true && that.isSetDate();
    if (this_present_date || that_present_date) {
      if (!(this_present_date && that_present_date))
        return false;
      if (!this.date.equals(that.date))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_dataset = true && (isSetDataset());
    builder.append(present_dataset);
    if (present_dataset)
      builder.append(dataset);

    boolean present_tres = true && (isSetTres());
    builder.append(present_tres);
    if (present_tres)
      builder.append(tres);

    boolean present_sres = true && (isSetSres());
    builder.append(present_sres);
    if (present_sres)
      builder.append(sres);

    boolean present_h = true;
    builder.append(present_h);
    if (present_h)
      builder.append(h);

    boolean present_v = true;
    builder.append(present_v);
    if (present_v)
      builder.append(v);

    boolean present_id = true;
    builder.append(present_id);
    if (present_id)
      builder.append(id);

    boolean present_size = true;
    builder.append(present_size);
    if (present_size)
      builder.append(size);

    boolean present_data = true && (isSetData());
    builder.append(present_data);
    if (present_data)
      builder.append(data);

    boolean present_date = true && (isSetDate());
    builder.append(present_date);
    if (present_date)
      builder.append(date);

    return builder.toHashCode();
  }

  public int compareTo(Chunk other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Chunk typedOther = (Chunk)other;

    lastComparison = Boolean.valueOf(isSetDataset()).compareTo(typedOther.isSetDataset());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDataset()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dataset, typedOther.dataset);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTres()).compareTo(typedOther.isSetTres());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTres()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tres, typedOther.tres);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSres()).compareTo(typedOther.isSetSres());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSres()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sres, typedOther.sres);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetH()).compareTo(typedOther.isSetH());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetH()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.h, typedOther.h);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetV()).compareTo(typedOther.isSetV());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetV()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.v, typedOther.v);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetId()).compareTo(typedOther.isSetId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.id, typedOther.id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSize()).compareTo(typedOther.isSetSize());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSize()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.size, typedOther.size);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetData()).compareTo(typedOther.isSetData());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetData()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.data, typedOther.data);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDate()).compareTo(typedOther.isSetDate());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDate()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.date, typedOther.date);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Chunk(");
    boolean first = true;

    sb.append("dataset:");
    if (this.dataset == null) {
      sb.append("null");
    } else {
      sb.append(this.dataset);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("tres:");
    if (this.tres == null) {
      sb.append("null");
    } else {
      sb.append(this.tres);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("sres:");
    if (this.sres == null) {
      sb.append("null");
    } else {
      sb.append(this.sres);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("h:");
    sb.append(this.h);
    first = false;
    if (!first) sb.append(", ");
    sb.append("v:");
    sb.append(this.v);
    first = false;
    if (!first) sb.append(", ");
    sb.append("id:");
    sb.append(this.id);
    first = false;
    if (!first) sb.append(", ");
    sb.append("size:");
    sb.append(this.size);
    first = false;
    if (!first) sb.append(", ");
    sb.append("data:");
    if (this.data == null) {
      sb.append("null");
    } else {
      sb.append(this.data);
    }
    first = false;
    if (isSetDate()) {
      if (!first) sb.append(", ");
      sb.append("date:");
      if (this.date == null) {
        sb.append("null");
      } else {
        sb.append(this.date);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bit_vector = new BitSet(1);
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ChunkStandardSchemeFactory implements SchemeFactory {
    public ChunkStandardScheme getScheme() {
      return new ChunkStandardScheme();
    }
  }

  private static class ChunkStandardScheme extends StandardScheme<Chunk> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Chunk struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // DATASET
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.dataset = iprot.readString();
              struct.setDatasetIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TRES
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.tres = iprot.readString();
              struct.setTresIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SRES
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.sres = iprot.readString();
              struct.setSresIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // H
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.h = iprot.readI32();
              struct.setHIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // V
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.v = iprot.readI32();
              struct.setVIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.id = iprot.readI32();
              struct.setIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // SIZE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.size = iprot.readI32();
              struct.setSizeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 8: // DATA
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.data = new Data();
              struct.data.read(iprot);
              struct.setDataIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 9: // DATE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.date = iprot.readString();
              struct.setDateIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, Chunk struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.dataset != null) {
        oprot.writeFieldBegin(DATASET_FIELD_DESC);
        oprot.writeString(struct.dataset);
        oprot.writeFieldEnd();
      }
      if (struct.tres != null) {
        oprot.writeFieldBegin(TRES_FIELD_DESC);
        oprot.writeString(struct.tres);
        oprot.writeFieldEnd();
      }
      if (struct.sres != null) {
        oprot.writeFieldBegin(SRES_FIELD_DESC);
        oprot.writeString(struct.sres);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(H_FIELD_DESC);
      oprot.writeI32(struct.h);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(V_FIELD_DESC);
      oprot.writeI32(struct.v);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(ID_FIELD_DESC);
      oprot.writeI32(struct.id);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(SIZE_FIELD_DESC);
      oprot.writeI32(struct.size);
      oprot.writeFieldEnd();
      if (struct.data != null) {
        oprot.writeFieldBegin(DATA_FIELD_DESC);
        struct.data.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.date != null) {
        if (struct.isSetDate()) {
          oprot.writeFieldBegin(DATE_FIELD_DESC);
          oprot.writeString(struct.date);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ChunkTupleSchemeFactory implements SchemeFactory {
    public ChunkTupleScheme getScheme() {
      return new ChunkTupleScheme();
    }
  }

  private static class ChunkTupleScheme extends TupleScheme<Chunk> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Chunk struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetDataset()) {
        optionals.set(0);
      }
      if (struct.isSetTres()) {
        optionals.set(1);
      }
      if (struct.isSetSres()) {
        optionals.set(2);
      }
      if (struct.isSetH()) {
        optionals.set(3);
      }
      if (struct.isSetV()) {
        optionals.set(4);
      }
      if (struct.isSetId()) {
        optionals.set(5);
      }
      if (struct.isSetSize()) {
        optionals.set(6);
      }
      if (struct.isSetData()) {
        optionals.set(7);
      }
      if (struct.isSetDate()) {
        optionals.set(8);
      }
      oprot.writeBitSet(optionals, 9);
      if (struct.isSetDataset()) {
        oprot.writeString(struct.dataset);
      }
      if (struct.isSetTres()) {
        oprot.writeString(struct.tres);
      }
      if (struct.isSetSres()) {
        oprot.writeString(struct.sres);
      }
      if (struct.isSetH()) {
        oprot.writeI32(struct.h);
      }
      if (struct.isSetV()) {
        oprot.writeI32(struct.v);
      }
      if (struct.isSetId()) {
        oprot.writeI32(struct.id);
      }
      if (struct.isSetSize()) {
        oprot.writeI32(struct.size);
      }
      if (struct.isSetData()) {
        struct.data.write(oprot);
      }
      if (struct.isSetDate()) {
        oprot.writeString(struct.date);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Chunk struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(9);
      if (incoming.get(0)) {
        struct.dataset = iprot.readString();
        struct.setDatasetIsSet(true);
      }
      if (incoming.get(1)) {
        struct.tres = iprot.readString();
        struct.setTresIsSet(true);
      }
      if (incoming.get(2)) {
        struct.sres = iprot.readString();
        struct.setSresIsSet(true);
      }
      if (incoming.get(3)) {
        struct.h = iprot.readI32();
        struct.setHIsSet(true);
      }
      if (incoming.get(4)) {
        struct.v = iprot.readI32();
        struct.setVIsSet(true);
      }
      if (incoming.get(5)) {
        struct.id = iprot.readI32();
        struct.setIdIsSet(true);
      }
      if (incoming.get(6)) {
        struct.size = iprot.readI32();
        struct.setSizeIsSet(true);
      }
      if (incoming.get(7)) {
        struct.data = new Data();
        struct.data.read(iprot);
        struct.setDataIsSet(true);
      }
      if (incoming.get(8)) {
        struct.date = iprot.readString();
        struct.setDateIsSet(true);
      }
    }
  }

}

