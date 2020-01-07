package javaapplication46;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


class LoadCsvFileData
{
    Path P1,P2,temp;
    
    public void loadCsv() throws ClassNotFoundException, SQLException, IOException
    {
      
       Class.forName("com.mysql.cj.jdbc.Driver");
        Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/TestDB?allowLoadLocalInfile=true","amirtha","1234");
        String query="load data local infile 'C:\\\\Users\\\\hostname\\\\Desktop\\\\PROJDATA\\\\pending\\\\delhi_weather.csv' into table weather fields terminated by ','"
                + " lines terminated by '\n' IGNORE 1 LINES (@datetime_utc,_conds, _dewptm, _fog, _hail, _hum, _precipm, _pressurem, _rain, _snow, _tempm, _thunder, _tornado, _wdire,city) set datetime_utc=DATE_FORMAT((STR_TO_DATE(@datetime_utc,'%Y%m%d-%H:%i')),'%Y-%m-%d'),year=(year(datetime_utc)),month=(month(datetime_utc)), city='Delhi'";
        Statement ps=con.createStatement();

     ps.execute(query);
     
     System.out.println("Loaded the data in database successfully");
     
   
         moveFile();
    }
     public void moveFile() throws IOException, ClassNotFoundException, SQLException
     {
         
        P1=Paths.get("C:\\Users\\hostname\\Desktop\\PROJDATA\\pending\\delhi_weather.csv");
       P2=Paths.get("C:\\Users\\hostname\\Desktop\\PROJDATA\\done\\delhi_weather.csv");
      
        temp = Files.move(P1,P2,REPLACE_EXISTING); 
       
        if(temp != null) 
        { 
            System.out.println("File moved successfully"); 
        } 
        else
        { 
            System.out.println("Failed to move the file"); 
        }
        
    }
    
}

public class Task2_NIO2_JDBC {

    public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, SQLException,FileAlreadyExistsException {
    
        LoadCsvFileData Ld=new LoadCsvFileData();
        Ld.loadCsv();
        
        
        
         
    }
    
}

