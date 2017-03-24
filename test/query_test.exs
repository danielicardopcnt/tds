defmodule QueryTest do
  import Tds.TestHelper
  require Logger
  use ExUnit.Case, async: true

  @tag timeout: 50000

  setup do
    opts = Application.fetch_env!(:mssql, :opts)
    {:ok, pid} = Tds.start_link(opts)

    {:ok, [pid: pid]}
  end

  test "Decode Fixed Length Data types", context do
    query("DROP TABLE FixedLength", [])
    query("""
      CREATE TABLE FixedLength (
        TinyInt tinyint,
        Bit bit,
        SmallInt smallint,
        Int int,
        SmallDateTime smalldatetime,
        Real real,
        Money money,
        DateTime datetime,
        DateTime2 datetime2,
        Float float,
        SmallMoney smallmoney,
        BitInt bigint)
      """, [])
    query("INSERT INTO FixedLength VALUES(1, 0, 12, 100, '2014-01-10T12:30:00', 0.5, '-822,337,203,685,477.5808',
      '2014-01-11T11:34:25', '2014-01-11T11:34:25.233', 5.6, '$-214,748.3648', 1000)", []);
    assert [[1, false, 12, 100, {{2014, 01, 10},{12, 30, 0, 0}}, 0.5, -822_337_203_685_477.5808,
      {{2014, 01, 11},{11, 34, 25, 0}}, {{2014, 1, 11}, {11, 34, 25, 2330000}},
      5.6, -214_748.3648 , 1000]] == query("SELECT TOP(1) * FROM FixedLength", [])

    query("DROP TABLE FixedLength", [])
  end

  test "Decode basic types", context do
    assert [[1]] = query("SELECT 1", [])
    assert [[1]] = query("SELECT 1 as 'number'", [])
    assert [[1, 1]] = query("SELECT 1, 1", [])
    assert [[-1]] = query("SELECT -1", [])
    assert [[10000000000000]] = query("select CAST(10000000000000 AS bigint)", [])

    assert [["string"]] = query("SELECT 'string'", [])
    assert [["ẽstring"]] = query("SELECT N'ẽstring'", [])
    assert [[true, false]] = query("SELECT CAST(1 AS BIT), CAST(0 AS BIT)", [])
    assert [[<<0x82, 0x25, 0xF2, 0xA9, 0xAF, 0xBA, 0x45, 0xC5, 0xA4, 0x31, 0x86, 0xB9, 0xA8, 0x67, 0xE0, 0xF7>>]] = query("SELECT CAST('8225F2A9-AFBA-45C5-A431-86B9A867E0F7' AS uniqueidentifier)", [])
  end

  test "Decode NULL", context do
    assert [[nil]] = query("SELECT NULL", [])
    assert [[nil]] = query("SELECT CAST(NULL AS BIT)", [])
    assert [[nil]] = query("SELECT CAST(NULL AS VARCHAR)", [])
    assert [[nil]] = query("SELECT CAST(NULL AS datetime)", [])
    query("SELECT CAST('1' AS VARCHAR)", [])
  end

  test "Create Tables", context do
    query("DROP TABLE MyTable", [])
    assert :ok = query("CREATE TABLE MyTable (TableId int)", [])
    assert :ok = query("DROP TABLE dbo.MyTable", [])
  end

  test "Large Result Set", context do
    query("DROP TABLE MyTable", [])
    assert :ok = query("CREATE TABLE MyTable (TableId int)", [])
    for n <- 1..100 do
      assert :ok = query("INSERT INTO MyTable VALUES (#{n})", [])
    end

    assert Enum.count(query("SELECT * FROM MyTable", [])) == 100
    assert :ok = query("DROP TABLE dbo.MyTable", [])
  end

  test "Empty Result Set", context do
    query("DROP TABLE MyTable", [])
    query("CREATE TABLE MyTable (TableId int)", [])
    assert :ok = query("SELECT * FROM MyTable", [])
  end

  test "fail for incorrect syntax", context do
    assert %Tds.Error{} = query("busted", [])
  end

  test "connection works after failure", context do
    assert %Tds.Error{} = query("busted", [])
    assert [[1]] = query("SELECT 1", [])
  end

  test "char nulls", context do
    assert [[nil]] = query("SELECT CAST(NULL as nvarchar(255))",[])
  end

  test "schema insert", context do
    query("DROP TABLE MyTable", [])
    assert :ok = query("CREATE TABLE MyTable (version bigint NOT NULL PRIMARY KEY, inserted_at datetime NULL)", [])
    assert :ok = query("INSERT INTO MyTable (version,inserted_at) VALUES (@1,@2)",
      [%Tds.Parameter{direction: :input, name: "@1", type: nil, value: 0},
      %Tds.Parameter{direction: :input, name: "@2", type: nil, value: {{2017, 3, 22}, {2, 6, 5, 750168}}}])
    assert [[0, {_date, _time}]] = query("SELECT version, inserted_at FROM MyTable",[])
    query("DROP TABLE MyTable", [])
  end
end
