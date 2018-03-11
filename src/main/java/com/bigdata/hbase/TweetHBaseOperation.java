package com.bigdata.hbase;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

public class TweetHBaseOperation {

	
	private final static TableName tweetsTable = TableName.valueOf("Tweets");
    private final String family1 = "cf";

    private final byte[] tweet = Bytes.toBytes("tweet");
    private final byte[] sentiment = Bytes.toBytes("sentiment");
    
    
	public static void main(String[] args) {
		try {
			Tweet t1= new Tweet();
			t1.setTweet("Good Job");
			t1.setSentiment("positive");
			
			Connection connection = HBaseConnection.getConnection();
			
			TweetHBaseOperation tweetOperations = new TweetHBaseOperation();
			tweetOperations.putTweet(connection.getAdmin(), connection.getTable(tweetsTable),t1.getTweet(),t1.getSentiment());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}	
	
	public void addTweet(){
		
	}
	
	public static byte[] getNextRowNo(){
		Random random = new Random();
		System.out.println(random.nextInt());
		return Bytes.toBytes(String.valueOf(random.nextInt()));
	}
	
	public void putTweet(Admin admin, Table table, byte[] tweetValue, byte[] sentimentValue) throws IOException {
        System.out.println("\n*** PUT example ~inserting \"cell-data\" into Family1:Qualifier1 of Table1 ~ ***");

        // Row1 => Family1:Qualifier1, Family1:Qualifier2
        byte[] row = getNextRowNo();
        
        Put p = new Put(row);
        p.addImmutable(family1.getBytes(), tweet, tweetValue);
        p.addImmutable(family1.getBytes(), sentiment, sentimentValue);
        table.put(p);

       
        admin.disableTable(tweetsTable);
        try {
            HColumnDescriptor desc = new HColumnDescriptor(row);
            admin.addColumn(tweetsTable, desc);
            System.out.println("Success.");
        } catch (Exception e) {
            System.out.println("Failed.");
            System.out.println(e.getMessage());
        } finally {
            admin.enableTable(tweetsTable);
        }
        System.out.println("Done. ");
    }
	
	
	public String getRow(){
		
		return "";
	}
	
}
