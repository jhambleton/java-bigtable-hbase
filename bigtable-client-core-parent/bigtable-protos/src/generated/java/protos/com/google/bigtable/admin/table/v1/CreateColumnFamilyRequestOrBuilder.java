// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: google/bigtable/admin/table/v1/bigtable_table_service_messages.proto

package com.google.bigtable.admin.table.v1;

public interface CreateColumnFamilyRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:google.bigtable.admin.table.v1.CreateColumnFamilyRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * The unique name of the table in which to create the new column family.
   * </pre>
   *
   * <code>optional string name = 1;</code>
   */
  java.lang.String getName();
  /**
   * <pre>
   * The unique name of the table in which to create the new column family.
   * </pre>
   *
   * <code>optional string name = 1;</code>
   */
  com.google.protobuf.ByteString
      getNameBytes();

  /**
   * <pre>
   * The name by which the new column family should be referred to within the
   * table, e.g. "foobar" rather than "&lt;table_name&gt;/columnFamilies/foobar".
   * </pre>
   *
   * <code>optional string column_family_id = 2;</code>
   */
  java.lang.String getColumnFamilyId();
  /**
   * <pre>
   * The name by which the new column family should be referred to within the
   * table, e.g. "foobar" rather than "&lt;table_name&gt;/columnFamilies/foobar".
   * </pre>
   *
   * <code>optional string column_family_id = 2;</code>
   */
  com.google.protobuf.ByteString
      getColumnFamilyIdBytes();

  /**
   * <pre>
   * The column family to create. The `name` field must be left blank.
   * </pre>
   *
   * <code>optional .google.bigtable.admin.table.v1.ColumnFamily column_family = 3;</code>
   */
  boolean hasColumnFamily();
  /**
   * <pre>
   * The column family to create. The `name` field must be left blank.
   * </pre>
   *
   * <code>optional .google.bigtable.admin.table.v1.ColumnFamily column_family = 3;</code>
   */
  com.google.bigtable.admin.table.v1.ColumnFamily getColumnFamily();
  /**
   * <pre>
   * The column family to create. The `name` field must be left blank.
   * </pre>
   *
   * <code>optional .google.bigtable.admin.table.v1.ColumnFamily column_family = 3;</code>
   */
  com.google.bigtable.admin.table.v1.ColumnFamilyOrBuilder getColumnFamilyOrBuilder();
}