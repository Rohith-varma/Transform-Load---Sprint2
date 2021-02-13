package ca.bigdata.rohith.sprinttwo

import java.sql.DriverManager

object Main extends App {
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName) // this step loads the JDBC to JVM
  val ConnectionString: String = "jdbc:hive2://quickstart.cloudera:10000/winter2020_rohith;user=rohith;"
  val connection = DriverManager.getConnection(ConnectionString) //creates JDBC connection
  val query = connection.createStatement()

  query.executeUpdate("DROP TABLE IF EXISTS winter2020_rohith.ext_sta_temp")
  query.executeUpdate("DROP TABLE IF EXISTS winter2020_rohith.ext_sys_temp")
  query.executeUpdate("DROP TABLE IF EXISTS winter2020_rohith.system_information")
  query.executeUpdate("DROP TABLE IF EXISTS winter2020_rohith.ext_station_information")
  query.executeUpdate("DROP TABLE IF EXISTS winter2020_rohith.enriched_station_information")

  println("Please wait I just dropped your tables")

  query.executeUpdate(
    """CREATE EXTERNAL TABLE IF NOT EXISTS winter2020_rohith.ext_sys_temp(
      |str STRING
      |)
      |LOCATION '/user/winter2020/rohith/final_project/external/system_information/'
      |""".stripMargin)

  query.executeUpdate(
    """CREATE EXTERNAL TABLE IF NOT EXISTS winter2020_rohith.ext_sta_temp(
      |str STRING
      |)
      |LOCATION '/user/winter2020/rohith/final_project/external/station_information/'
      |""".stripMargin)

  query.executeUpdate(
    """CREATE TABLE IF NOT EXISTS system_information as
      |SELECT get_json_object(str,'$.data.system_id') as system_id,get_json_object(str,'$.data.timezone')
      |as timezone from ext_sys_temp""".stripMargin)

  query.executeUpdate(
    """CREATE EXTERNAL TABLE IF NOT EXISTS ext_station_information(
      |station_id   INT,
      |name         STRING,
      |short_name   STRING,
      |lat          DOUBLE,
      |lon          DOUBLE,
      |capacity     INT
      |)
      |""".stripMargin)

  query.executeUpdate(
    """INSERT OVERWRITE TABLE ext_station_information
      |SELECT get_json_object(str,concat('$.data.stations[',d.i,'].station_id'))
      |as station_id,
      |get_json_object(str,concat('$.data.stations[',d.i,'].name')) as name,
      |get_json_object(str,concat('$.data.stations[',d.i,'].short_name')) as short_name,
      |get_json_object(str,concat('$.data.stations[',d.i,'].lat')) as lat,
      |get_json_object(str,concat('$.data.stations[',d.i,'].lon')) as lon,
      |get_json_object(str,concat('$.data.stations[',d.i,'].capacity')) as capacity
      |from ext_sta_temp
      |LATERAL VIEW posexplode(split(get_json_object(str,'$.data'),',')) d as i,t
      |""".stripMargin)

  println("Few more seconds,I just converted your multi-nested JSON to CSV")

  query.executeUpdate(
    """CREATE TABLE IF NOT EXISTS enriched_station_information(
      |system_id STRING,
      |timezone STRING,
      |station_id INT,
      |name STRING,
      |short_name STRING,
      |lat DOUBLE,
      |lon DOUBLE,
      |capacity INT
      |)
      |ROW FORMAT DELIMITED
      |STORED AS TEXTFILE""".stripMargin)

  query.executeUpdate(
    """INSERT OVERWRITE TABLE enriched_station_information
      |select * from system_information CROSS JOIN station_information""".stripMargin)

  println("We're done,enriched your data and loaded onto Hive table")

  query.close()
  connection.close()
}
