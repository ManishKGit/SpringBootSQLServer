package aero.sita.jaf.decompress;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.zip.InflaterOutputStream;
import org.apache.commons.lang3.text.WordUtils;

@SuppressWarnings("deprecation")
public class DecompressData {
	
	private static String url = "jdbc:sqlserver://LAP-TZD-0X61723/SQLEXPRESS;databaseName=AIE_GUI";
	private static String user = "sa";
	private static String password = "niit@123";
	
	public String fetchDataFromDbAndUncompress(String message_id)
		    throws Exception
		  {
		/*
		 * prop.load(new FileInputStream(new File(PROPERTY_FILE))); String url =
		 * prop.getProperty("jdbcUrl"); String user = prop.getProperty("user"); String
		 * password = prop.getProperty("password");
		 */
		    Connection con = DriverManager.getConnection(url, user, password);
		    Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT message_compressed from message where id=" + message_id);
		    System.out.println("MessageId : "+rs.getString("message_id"));
		    if (rs.next())
		    {
		      byte[] rawMessage = rs.getBytes("message_compressed");
		      String decompressed = decompress(rawMessage);
		      return WordUtils.wrap(decompressed, 80);
		    }
		    return "No compressed data found on DB for the ID:" + message_id;
		  }
	
	private static String decompress(byte[] compressedMsg)
	  {
	    ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
	    InflaterOutputStream outputStream1 = new InflaterOutputStream(baos1);
	    try
	    {
	      outputStream1.write(compressedMsg);
	      outputStream1.finish();
	    }
	    catch (Exception e) {
	    	e.printStackTrace();
	    }
	    String decompressedMsg = byteArrayToString(baos1.toByteArray());
	    return decompressedMsg;
	  }
	  
	  public static String byteArrayToString(byte[] a)
	  {
	    return new String(a, StandardCharsets.UTF_8);
	  }

	  public static void main(String[] args) {
			// TODO Auto-generated method stub
			DecompressData data = new DecompressData();
			String msg;
			try {
				msg = data.fetchDataFromDbAndUncompress("84011455");
				System.out.println("Message is +"+msg);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		}
}
