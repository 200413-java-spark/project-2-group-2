package com.github.group2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
public class IO {
    public static void datasetToCsv(Dataset<Row> temp, String filename)
    {
        String header="";
		for(int i = 0; i < temp.columns().length ; i++) {
			header = header.concat(temp.columns()[i]);
			if(i+1<temp.columns().length)
				header=header+",";
		}
		header=header+"\n";
		try (PrintWriter writer = new PrintWriter(new File(filename))) {

			StringBuilder line = new StringBuilder();
			line.append(header);
			for(Row s:temp.collectAsList())
			{
				int i=s.length();
				for(int j=0;j<i;j++)
				{
					line.append(s.get(j));
					if(j+1<i)
					{
						line.append(",");
					}
				}
				line.append("\n");
			}
			writer.write(line.toString());
		  } catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
          }
          System.out.println("Successfully Create CSV");
    }
    
}