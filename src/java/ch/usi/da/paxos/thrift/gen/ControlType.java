/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package ch.usi.da.paxos.thrift.gen;


@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)", date = "2019-02-01")
public enum ControlType implements org.apache.thrift.TEnum {
  SUBSCRIBE(0),
  UNSUBSCRIBE(1),
  PREPARE(2);

  private final int value;

  private ControlType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static ControlType findByValue(int value) { 
    switch (value) {
      case 0:
        return SUBSCRIBE;
      case 1:
        return UNSUBSCRIBE;
      case 2:
        return PREPARE;
      default:
        return null;
    }
  }
}
