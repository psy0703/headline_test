package common

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

import scala.collection.mutable

/**
  * @Author: Cedaris
  * @Date: 2019/6/19 11:03
  */
object HBaseUtils extends Serializable {

  /**
    * 设置HBaseConfiguration
    * @param quorum
    * @param port
    */
  def getHBaseConfiguration(quorum:String, port:String): Configuration ={
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",quorum)
    conf.set("hbase.zookeeper.property.clientPort",port)

    conf
  }


  def getHBaseConnection() = {
    val conf = HBaseConfiguration.create
    /*conf.set("hbase.zookeeper.property.clientPort", Constants.ZOOKEEPER_CLIENT_PORT)
    conf.set("hbase.zookeeper.quorum", Constants.ZOOKEEPER_QUORUM)
    conf.set("hbase.master", Constants.HBASE_MASTER)
    conf.set("zookeeper.znode.parent", Constants.ZOOKEEPER_ZNODE_PARENT)*/

    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "192.168.11.165")
    conf.set("hbase.master", "192.168.11.164")
    conf.set("zookeeper.znode.parent", "/hbase")
    ConnectionFactory.createConnection(conf)
  }

  /**
    * 返回或者新建HBASEAdmin
    * @param conf
    * @param tableName
    */
  def getHBaseAdmin(conf:Configuration,tableName:String): Admin ={
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = conn.getAdmin
    if(!admin.isTableAvailable(TableName.valueOf(tableName))){
      val tabledesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tabledesc)
    }
    admin
  }

  /**
    * 判断制定表是否存在
    * @param tableName
    * @param admin
    * @return
    */
  def isTableExists(tableName:String,admin:Admin): Boolean ={
    val isExists: Boolean = admin.tableExists(TableName.valueOf(tableName))
    isExists
  }

  /**
    * 返回HTable
    * @param configuration
    * @param tableName
    * @return
    */
  def getTable(configuration: Configuration,tableName: String): HTable ={

    new HTable(configuration,TableName.valueOf(tableName))
  }

  /**
    * 创建表
    * @param tableName
    * @param admin
    * @param cfs
    */
  def createTable(tableName:String,admin: Admin ,cfs:String*){
    //表如果存在就直接结束方法
    if(isTableExists(tableName,admin)) {return}
    val desc = new HTableDescriptor(TableName.valueOf(tableName))
    for (cf <- cfs) {
      desc.addFamily(new HColumnDescriptor(cf))
    }

    admin.createTable(desc)
    println("表:" + tableName + "创建成功!")
  }

  def createTable(hiveTable: String,columnFamily:String): Unit = {
    val conn = getHBaseConnection()
    val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]
    val tableName = TableName.valueOf(hiveTable)
    if (!admin.tableExists(tableName)) {
      // 表不存在则创建
      val desc = new HTableDescriptor(tableName)
      val columnDesc = new HColumnDescriptor(columnFamily)
      desc.addFamily(columnDesc)
      admin.createTable(desc)
    }else{
      val tableDesc: HTableDescriptor = admin.getTableDescriptor(tableName)
      admin.disableTable(tableName)
      //若列族已存在，先删除
      tableDesc.removeFamily(Bytes.toBytes(columnFamily))
      //创建新的列族
      val columnDescriptor = new HColumnDescriptor(columnFamily)
      tableDesc.addFamily(columnDescriptor)
      admin.modifyTable(tableName,tableDesc)
      admin.enableTable(tableName)
    }
  }

  /**
    * 删除表
    * @param tableName
    * @param admin
    */
  def deleteTable(tableName: String,admin: Admin): Unit ={
    if (isTableExists(tableName,admin)){
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
    }else{
      println("你要删除的表：" + tableName + "不存在！！");
    }
  }

  /**
    * 向表中插入数据
    * @param tableName
    * @param rowKey
    * @param cf 列族
    * @param column
    * @param value
    * @param admin
    */
  def addData(tableName:String,rowKey:String,cf:String,column:String,
              value:String,admin: Admin): Unit ={
    if(!isTableExists(tableName,admin)){
      createTable(tableName,admin)
    }
    val conn: Connection = admin.getConnection
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(cf),
      Bytes.toBytes(column),
      Bytes.toBytes(value)
    )

    table.put(put)
    table.close()
  }

  /**
    * 删除多行数据
    * @param tableName
    * @param admin
    * @param rows
    */
  def deleteRows(tableName:String,admin: Admin,rows :String*): Unit ={
    if(!isTableExists(tableName,admin)){return }
    val table: Table = admin.getConnection.getTable(TableName.valueOf(tableName))
    //批量删除
    val deletes = new util.ArrayList[Delete]()
    for (row <- rows) {
    deletes.add(new Delete(Bytes.toBytes(row)))
    }
    table.delete(deletes)
  }

  /**
    * 获取某一行指定列的数据
    * @param tableName
    * @param row
    * @param cf
    * @param column
    * @param admin
    */
  def getDataByColumn(tableName:String,row :String,cf:String,column:String,
                      admin: Admin): Unit ={
    if(!isTableExists(tableName,admin)){return }
    val table: Table = admin.getConnection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(row))

    get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column))
    val result: Result = table.get(get)
    val cells: util.List[Cell] = result.listCells()

    while(cells.iterator.hasNext)  {
      val cell: Cell = cells.iterator.next()
      val family: String = Bytes.toString(CellUtil.cloneFamily(cell))
      val value: String = Bytes.toString(CellUtil.cloneValue(cell))
      val rowName: String = Bytes.toString(CellUtil.cloneRow(cell))
      val column: String = Bytes.toString(CellUtil.cloneQualifier(cell))
      println("rowName = " + rowName)
      println("family = " + family)
      println("column = " + column)
      println("value = " + value)
    }
  }

  /**
    * 获取某一行数据
    * @param tableName
    * @param row
    * @param admin
    */
  def getRowData(tableName:String,row:String,admin: Admin): Unit ={
    val table: Table = admin.getConnection.getTable(TableName.valueOf(tableName))

    val get = new Get(Bytes.toBytes(row))
    val result: Result = table.get(get)
    val cells: util.List[Cell] = result.listCells()


    while(cells.iterator.hasNext)  {
      val cell: Cell = cells.iterator.next()
      val family: String = Bytes.toString(CellUtil.cloneFamily(cell))
      val value: String = Bytes.toString(CellUtil.cloneValue(cell))
      val rowName: String = Bytes.toString(CellUtil.cloneRow(cell))
      val column: String = Bytes.toString(CellUtil.cloneQualifier(cell))
      println("rowName = " + rowName)
      println("family = " + family)
      println("column = " + column)
      println("value = " + value)
    }
  }

  /**
    * 获取指定rowKey、列族的数据
    * @param admin
    * @param tablename
    * @param rowkey
    * @param famliyname
    * @return
    */
  def getDataByRowkeyCf(admin: Admin,
                        tablename: String,
                        rowkey: String,
                        famliyname: String): mutable.HashMap[String,String] = {
    val table: Table = admin.getConnection.getTable(TableName.valueOf(tablename))
    // 将字符串转换成byte[]
    val rowkeybyte: Array[Byte] = Bytes.toBytes(rowkey)
    val get = new Get(rowkeybyte)
    get.addFamily(famliyname.getBytes())
    val result: Result = table.get(get)
    val cells: Array[Cell] = result.rawCells()
    var resultMap =new mutable.HashMap[String,String]


    for(i <- 0 until(cells.length)) {
      val value: String = Bytes.toString(CellUtil.cloneValue(cells(i)))
      val column: String = Bytes.toString(CellUtil.cloneQualifier(cells(i)))

      resultMap.put(column , value)
    }

    return resultMap
  }

  /**
    * 获取指定rowKey、列族和列的数据
    * @param admin
    * @param tablename
    * @param rowkey
    * @param famliyname
    * @param column
    * @return
    */
  def getDataByRowkeyCfColumn(admin: Admin,
                              tablename: String,
                              rowkey: String,
                              famliyname: String,
                              column: String): String = {
    val table: Table = admin.getConnection.getTable(TableName.valueOf(tablename))
    // 将字符串转换成byte[]
    val rowkeybyte: Array[Byte] = Bytes.toBytes(rowkey)
    val get = new Get(rowkeybyte)
    get.addFamily(famliyname.getBytes())
    val result: Result = table.get(get)
    val resultbytes = result.getValue(famliyname.getBytes, column.getBytes)
    if (resultbytes == null) {return null}
    new String(resultbytes)
  }

  /**
    * 获取表中的所有数据
    * @param tableName
    * @param admin
    */
  def getAllData(tableName:String,admin: Admin): Unit ={
    val table: Table = admin.getConnection.getTable(TableName.valueOf(tableName))
    val results: ResultScanner = table.getScanner(new Scan)

    results.iterator()
    /*while(result != null){
      /*val row: Array[Byte] = result.getRow
      print(row.toString)*/
      var cells: Array[Cell] = result.rawCells()
      for (cell <- cells){
        val cell: Cell = cells.iterator.next()
        val family: String = Bytes.toString(CellUtil.cloneFamily(cell))
        val value: String = Bytes.toString(CellUtil.cloneValue(cell))
        val rowName: String = Bytes.toString(CellUtil.cloneRow(cell))
        val column: String = Bytes.toString(CellUtil.cloneQualifier(cell))
        println("rowName = " + rowName)
        println("family = " + family)
        println("column = " + column)
        println("value = " + value)
        println("----------------------------")
      }
      result = results.next()
    }*/
  }

  def close(admin: Admin): Unit ={
    if(admin != null){
      admin.close()
    }
  }
}
