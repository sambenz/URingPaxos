/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package ch.usi.da.dmap.thrift.gen;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum CommandType implements org.apache.thrift.TEnum {
  GET(0),
  PUT(1),
  REMOVE(2),
  SIZE(3),
  CLEAR(4),
  CONTAINSVALUE(5),
  FIRSTKEY(6),
  LASTKEY(7),
  PUTIFABSENT(8),
  REPLACE(9);

  private final int value;

  private CommandType(int value) {
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
  public static CommandType findByValue(int value) { 
    switch (value) {
      case 0:
        return GET;
      case 1:
        return PUT;
      case 2:
        return REMOVE;
      case 3:
        return SIZE;
      case 4:
        return CLEAR;
      case 5:
        return CONTAINSVALUE;
      case 6:
        return FIRSTKEY;
      case 7:
        return LASTKEY;
      case 8:
        return PUTIFABSENT;
      case 9:
        return REPLACE;
      default:
        return null;
    }
  }
}
