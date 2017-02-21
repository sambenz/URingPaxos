/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package ch.usi.da.dmap.thrift.gen;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
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

public class RangeResponse implements org.apache.thrift.TBase<RangeResponse, RangeResponse._Fields>, java.io.Serializable, Cloneable, Comparable<RangeResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RangeResponse");

  private static final org.apache.thrift.protocol.TField ID_FIELD_DESC = new org.apache.thrift.protocol.TField("id", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField COUNT_FIELD_DESC = new org.apache.thrift.protocol.TField("count", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField SNAPSHOT_FIELD_DESC = new org.apache.thrift.protocol.TField("snapshot", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField VALUES_FIELD_DESC = new org.apache.thrift.protocol.TField("values", org.apache.thrift.protocol.TType.STRING, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new RangeResponseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new RangeResponseTupleSchemeFactory());
  }

  public long id; // required
  public long count; // required
  public long snapshot; // required
  public ByteBuffer values; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ID((short)1, "id"),
    COUNT((short)2, "count"),
    SNAPSHOT((short)3, "snapshot"),
    VALUES((short)4, "values");

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
        case 1: // ID
          return ID;
        case 2: // COUNT
          return COUNT;
        case 3: // SNAPSHOT
          return SNAPSHOT;
        case 4: // VALUES
          return VALUES;
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
  private static final int __ID_ISSET_ID = 0;
  private static final int __COUNT_ISSET_ID = 1;
  private static final int __SNAPSHOT_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  private _Fields optionals[] = {_Fields.VALUES};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ID, new org.apache.thrift.meta_data.FieldMetaData("id", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.COUNT, new org.apache.thrift.meta_data.FieldMetaData("count", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.SNAPSHOT, new org.apache.thrift.meta_data.FieldMetaData("snapshot", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.VALUES, new org.apache.thrift.meta_data.FieldMetaData("values", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RangeResponse.class, metaDataMap);
  }

  public RangeResponse() {
  }

  public RangeResponse(
    long id,
    long count,
    long snapshot)
  {
    this();
    this.id = id;
    setIdIsSet(true);
    this.count = count;
    setCountIsSet(true);
    this.snapshot = snapshot;
    setSnapshotIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RangeResponse(RangeResponse other) {
    __isset_bitfield = other.__isset_bitfield;
    this.id = other.id;
    this.count = other.count;
    this.snapshot = other.snapshot;
    if (other.isSetValues()) {
      this.values = org.apache.thrift.TBaseHelper.copyBinary(other.values);
;
    }
  }

  public RangeResponse deepCopy() {
    return new RangeResponse(this);
  }

  @Override
  public void clear() {
    setIdIsSet(false);
    this.id = 0;
    setCountIsSet(false);
    this.count = 0;
    setSnapshotIsSet(false);
    this.snapshot = 0;
    this.values = null;
  }

  public long getId() {
    return this.id;
  }

  public RangeResponse setId(long id) {
    this.id = id;
    setIdIsSet(true);
    return this;
  }

  public void unsetId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __ID_ISSET_ID);
  }

  /** Returns true if field id is set (has been assigned a value) and false otherwise */
  public boolean isSetId() {
    return EncodingUtils.testBit(__isset_bitfield, __ID_ISSET_ID);
  }

  public void setIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __ID_ISSET_ID, value);
  }

  public long getCount() {
    return this.count;
  }

  public RangeResponse setCount(long count) {
    this.count = count;
    setCountIsSet(true);
    return this;
  }

  public void unsetCount() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __COUNT_ISSET_ID);
  }

  /** Returns true if field count is set (has been assigned a value) and false otherwise */
  public boolean isSetCount() {
    return EncodingUtils.testBit(__isset_bitfield, __COUNT_ISSET_ID);
  }

  public void setCountIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __COUNT_ISSET_ID, value);
  }

  public long getSnapshot() {
    return this.snapshot;
  }

  public RangeResponse setSnapshot(long snapshot) {
    this.snapshot = snapshot;
    setSnapshotIsSet(true);
    return this;
  }

  public void unsetSnapshot() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __SNAPSHOT_ISSET_ID);
  }

  /** Returns true if field snapshot is set (has been assigned a value) and false otherwise */
  public boolean isSetSnapshot() {
    return EncodingUtils.testBit(__isset_bitfield, __SNAPSHOT_ISSET_ID);
  }

  public void setSnapshotIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __SNAPSHOT_ISSET_ID, value);
  }

  public byte[] getValues() {
    setValues(org.apache.thrift.TBaseHelper.rightSize(values));
    return values == null ? null : values.array();
  }

  public ByteBuffer bufferForValues() {
    return values;
  }

  public RangeResponse setValues(byte[] values) {
    setValues(values == null ? (ByteBuffer)null : ByteBuffer.wrap(values));
    return this;
  }

  public RangeResponse setValues(ByteBuffer values) {
    this.values = values;
    return this;
  }

  public void unsetValues() {
    this.values = null;
  }

  /** Returns true if field values is set (has been assigned a value) and false otherwise */
  public boolean isSetValues() {
    return this.values != null;
  }

  public void setValuesIsSet(boolean value) {
    if (!value) {
      this.values = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ID:
      if (value == null) {
        unsetId();
      } else {
        setId((Long)value);
      }
      break;

    case COUNT:
      if (value == null) {
        unsetCount();
      } else {
        setCount((Long)value);
      }
      break;

    case SNAPSHOT:
      if (value == null) {
        unsetSnapshot();
      } else {
        setSnapshot((Long)value);
      }
      break;

    case VALUES:
      if (value == null) {
        unsetValues();
      } else {
        setValues((ByteBuffer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ID:
      return Long.valueOf(getId());

    case COUNT:
      return Long.valueOf(getCount());

    case SNAPSHOT:
      return Long.valueOf(getSnapshot());

    case VALUES:
      return getValues();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ID:
      return isSetId();
    case COUNT:
      return isSetCount();
    case SNAPSHOT:
      return isSetSnapshot();
    case VALUES:
      return isSetValues();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof RangeResponse)
      return this.equals((RangeResponse)that);
    return false;
  }

  public boolean equals(RangeResponse that) {
    if (that == null)
      return false;

    boolean this_present_id = true;
    boolean that_present_id = true;
    if (this_present_id || that_present_id) {
      if (!(this_present_id && that_present_id))
        return false;
      if (this.id != that.id)
        return false;
    }

    boolean this_present_count = true;
    boolean that_present_count = true;
    if (this_present_count || that_present_count) {
      if (!(this_present_count && that_present_count))
        return false;
      if (this.count != that.count)
        return false;
    }

    boolean this_present_snapshot = true;
    boolean that_present_snapshot = true;
    if (this_present_snapshot || that_present_snapshot) {
      if (!(this_present_snapshot && that_present_snapshot))
        return false;
      if (this.snapshot != that.snapshot)
        return false;
    }

    boolean this_present_values = true && this.isSetValues();
    boolean that_present_values = true && that.isSetValues();
    if (this_present_values || that_present_values) {
      if (!(this_present_values && that_present_values))
        return false;
      if (!this.values.equals(that.values))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(RangeResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetId()).compareTo(other.isSetId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.id, other.id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCount()).compareTo(other.isSetCount());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCount()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.count, other.count);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSnapshot()).compareTo(other.isSetSnapshot());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSnapshot()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.snapshot, other.snapshot);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetValues()).compareTo(other.isSetValues());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValues()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.values, other.values);
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
    StringBuilder sb = new StringBuilder("RangeResponse(");
    boolean first = true;

    sb.append("id:");
    sb.append(this.id);
    first = false;
    if (!first) sb.append(", ");
    sb.append("count:");
    sb.append(this.count);
    first = false;
    if (!first) sb.append(", ");
    sb.append("snapshot:");
    sb.append(this.snapshot);
    first = false;
    if (isSetValues()) {
      if (!first) sb.append(", ");
      sb.append("values:");
      if (this.values == null) {
        sb.append("null");
      } else {
        org.apache.thrift.TBaseHelper.toString(this.values, sb);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
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
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class RangeResponseStandardSchemeFactory implements SchemeFactory {
    public RangeResponseStandardScheme getScheme() {
      return new RangeResponseStandardScheme();
    }
  }

  private static class RangeResponseStandardScheme extends StandardScheme<RangeResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, RangeResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.id = iprot.readI64();
              struct.setIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // COUNT
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.count = iprot.readI64();
              struct.setCountIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SNAPSHOT
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.snapshot = iprot.readI64();
              struct.setSnapshotIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // VALUES
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.values = iprot.readBinary();
              struct.setValuesIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, RangeResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(ID_FIELD_DESC);
      oprot.writeI64(struct.id);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(COUNT_FIELD_DESC);
      oprot.writeI64(struct.count);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(SNAPSHOT_FIELD_DESC);
      oprot.writeI64(struct.snapshot);
      oprot.writeFieldEnd();
      if (struct.values != null) {
        if (struct.isSetValues()) {
          oprot.writeFieldBegin(VALUES_FIELD_DESC);
          oprot.writeBinary(struct.values);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RangeResponseTupleSchemeFactory implements SchemeFactory {
    public RangeResponseTupleScheme getScheme() {
      return new RangeResponseTupleScheme();
    }
  }

  private static class RangeResponseTupleScheme extends TupleScheme<RangeResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, RangeResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetId()) {
        optionals.set(0);
      }
      if (struct.isSetCount()) {
        optionals.set(1);
      }
      if (struct.isSetSnapshot()) {
        optionals.set(2);
      }
      if (struct.isSetValues()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetId()) {
        oprot.writeI64(struct.id);
      }
      if (struct.isSetCount()) {
        oprot.writeI64(struct.count);
      }
      if (struct.isSetSnapshot()) {
        oprot.writeI64(struct.snapshot);
      }
      if (struct.isSetValues()) {
        oprot.writeBinary(struct.values);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, RangeResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.id = iprot.readI64();
        struct.setIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.count = iprot.readI64();
        struct.setCountIsSet(true);
      }
      if (incoming.get(2)) {
        struct.snapshot = iprot.readI64();
        struct.setSnapshotIsSet(true);
      }
      if (incoming.get(3)) {
        struct.values = iprot.readBinary();
        struct.setValuesIsSet(true);
      }
    }
  }

}

