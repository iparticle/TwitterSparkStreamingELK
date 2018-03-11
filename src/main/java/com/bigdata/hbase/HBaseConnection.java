package com.bigdata.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.protobuf.ServiceException;

public class HBaseConnection {

	public static String path = HBaseConnection.class.getClassLoader().getResource("hbase-site.xml").getPath();
	
	public  static Connection getConnection() throws IOException, ServiceException {
        Configuration config = HBaseConfiguration.create();
//        String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
        config.addResource(new Path(path));

        try {
            HBaseAdmin.checkHBaseAvailable(config);
        } catch (MasterNotRunningException e) {
            System.out.println("HBase is not running." + e.getMessage());
            return null;
        }

        return ConnectionFactory.createConnection(config);
    }
	
	
	public static Configuration  getHBaseConf() throws ZooKeeperConnectionException, ServiceException, IOException{
		Configuration config = HBaseConfiguration.create();
//      String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
      config.addResource(new Path(path));

      try {
          HBaseAdmin.checkHBaseAvailable(config);
      } catch (MasterNotRunningException e) {
          System.out.println("HBase is not running." + e.getMessage());
          return null;
      }
      
      return config;
	}
}
