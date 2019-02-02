/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package ch.usi.da.paxos.thrift.gen;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)", date = "2019-02-01")
public class Decision implements org.apache.thrift.TBase<Decision, Decision._Fields>, java.io.Serializable, Cloneable, Comparable<Decision> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Decision");

  private static final org.apache.thrift.protocol.TField RING_FIELD_DESC = new org.apache.thrift.protocol.TField("ring", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField INSTANCE_FIELD_DESC = new org.apache.thrift.protocol.TField("instance", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField VALUE_FIELD_DESC = new org.apache.thrift.protocol.TField("value", org.apache.thrift.protocol.TType.STRUCT, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new DecisionStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new DecisionTupleSchemeFactory();

  public int ring; // required
  public long instance; // required
  public @org.apache.thrift.annotation.Nullable Value value; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    RING((short)1, "ring"),
    INSTANCE((short)2, "instance"),
    VALUE((short)3, "value");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // RING
          return RING;
        case 2: // INSTANCE
          return INSTANCE;
        case 3: // VALUE
          return VALUE;
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
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __RING_ISSET_ID = 0;
  private static final int __INSTANCE_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.RING, new org.apache.thrift.meta_data.FieldMetaData("ring", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.INSTANCE, new org.apache.thrift.meta_data.FieldMetaData("instance", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.VALUE, new org.apache.thrift.meta_data.FieldMetaData("value", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Value.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Decision.class, metaDataMap);
  }

  public Decision() {
  }

  public Decision(
    int ring,
    long instance,
    Value value)
  {
    this();
    this.ring = ring;
    setRingIsSet(true);
    this.instance = instance;
    setInstanceIsSet(true);
    this.value = value;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Decision(Decision other) {
    __isset_bitfield = other.__isset_bitfield;
    this.ring = other.ring;
    this.instance = other.instance;
    if (other.isSetValue()) {
      this.value = new Value(other.value);
    }
  }

  public Decision deepCopy() {
    return new Decision(this);
  }

  @Override
  public void clear() {
    setRingIsSet(false);
    this.ring = 0;
    setInstanceIsSet(false);
    this.instance = 0;
    this.value = null;
  }

  public int getRing() {
    return this.ring;
  }

  public Decision setRing(int ring) {
    this.ring = ring;
    setRingIsSet(true);
    return this;
  }

  public void unsetRing() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __RING_ISSET_ID);
  }

  /** Returns true if field ring is set (has been assigned a value) and false otherwise */
  public boolean isSetRing() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __RING_ISSET_ID);
  }

  public void setRingIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __RING_ISSET_ID, value);
  }

  public long getInstance() {
    return this.instance;
  }

  public Decision setInstance(long instance) {
    this.instance = instance;
    setInstanceIsSet(true);
    return this;
  }

  public void unsetInstance() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __INSTANCE_ISSET_ID);
  }

  /** Returns true if field instance is set (has been assigned a value) and false otherwise */
  public boolean isSetInstance() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __INSTANCE_ISSET_ID);
  }

  public void setInstanceIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __INSTANCE_ISSET_ID, value);
  }

  @org.apache.thrift.annotation.Nullable
  public Value getValue() {
    return this.value;
  }

  public Decision setValue(@org.apache.thrift.annotation.Nullable Value value) {
    this.value = value;
    return this;
  }

  public void unsetValue() {
    this.value = null;
  }

  /** Returns true if field value is set (has been assigned a value) and false otherwise */
  public boolean isSetValue() {
    return this.value != null;
  }

  public void setValueIsSet(boolean value) {
    if (!value) {
      this.value = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case RING:
      if (value == null) {
        unsetRing();
      } else {
        setRing((java.lang.Integer)value);
      }
      break;

    case INSTANCE:
      if (value == null) {
        unsetInstance();
      } else {
        setInstance((java.lang.Long)value);
      }
      break;

    case VALUE:
      if (value == null) {
        unsetValue();
      } else {
        setValue((Value)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case RING:
      return getRing();

    case INSTANCE:
      return getInstance();

    case VALUE:
      return getValue();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case RING:
      return isSetRing();
    case INSTANCE:
      return isSetInstance();
    case VALUE:
      return isSetValue();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof Decision)
      return this.equals((Decision)that);
    return false;
  }

  public boolean equals(Decision that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_ring = true;
    boolean that_present_ring = true;
    if (this_present_ring || that_present_ring) {
      if (!(this_present_ring && that_present_ring))
        return false;
      if (this.ring != that.ring)
        return false;
    }

    boolean this_present_instance = true;
    boolean that_present_instance = true;
    if (this_present_instance || that_present_instance) {
      if (!(this_present_instance && that_present_instance))
        return false;
      if (this.instance != that.instance)
        return false;
    }

    boolean this_present_value = true && this.isSetValue();
    boolean that_present_value = true && that.isSetValue();
    if (this_present_value || that_present_value) {
      if (!(this_present_value && that_present_value))
        return false;
      if (!this.value.equals(that.value))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ring;

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(instance);

    hashCode = hashCode * 8191 + ((isSetValue()) ? 131071 : 524287);
    if (isSetValue())
      hashCode = hashCode * 8191 + value.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(Decision other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetRing()).compareTo(other.isSetRing());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRing()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ring, other.ring);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetInstance()).compareTo(other.isSetInstance());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetInstance()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.instance, other.instance);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetValue()).compareTo(other.isSetValue());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValue()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.value, other.value);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("Decision(");
    boolean first = true;

    sb.append("ring:");
    sb.append(this.ring);
    first = false;
    if (!first) sb.append(", ");
    sb.append("instance:");
    sb.append(this.instance);
    first = false;
    if (!first) sb.append(", ");
    sb.append("value:");
    if (this.value == null) {
      sb.append("null");
    } else {
      sb.append(this.value);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (value != null) {
      value.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class DecisionStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public DecisionStandardScheme getScheme() {
      return new DecisionStandardScheme();
    }
  }

  private static class DecisionStandardScheme extends org.apache.thrift.scheme.StandardScheme<Decision> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Decision struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // RING
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.ring = iprot.readI32();
              struct.setRingIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // INSTANCE
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.instance = iprot.readI64();
              struct.setInstanceIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // VALUE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.value = new Value();
              struct.value.read(iprot);
              struct.setValueIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, Decision struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(RING_FIELD_DESC);
      oprot.writeI32(struct.ring);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(INSTANCE_FIELD_DESC);
      oprot.writeI64(struct.instance);
      oprot.writeFieldEnd();
      if (struct.value != null) {
        oprot.writeFieldBegin(VALUE_FIELD_DESC);
        struct.value.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class DecisionTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public DecisionTupleScheme getScheme() {
      return new DecisionTupleScheme();
    }
  }

  private static class DecisionTupleScheme extends org.apache.thrift.scheme.TupleScheme<Decision> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Decision struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetRing()) {
        optionals.set(0);
      }
      if (struct.isSetInstance()) {
        optionals.set(1);
      }
      if (struct.isSetValue()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetRing()) {
        oprot.writeI32(struct.ring);
      }
      if (struct.isSetInstance()) {
        oprot.writeI64(struct.instance);
      }
      if (struct.isSetValue()) {
        struct.value.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Decision struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.ring = iprot.readI32();
        struct.setRingIsSet(true);
      }
      if (incoming.get(1)) {
        struct.instance = iprot.readI64();
        struct.setInstanceIsSet(true);
      }
      if (incoming.get(2)) {
        struct.value = new Value();
        struct.value.read(iprot);
        struct.setValueIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

