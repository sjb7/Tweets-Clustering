import java.io.FileWriter;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class TwitterData
{
	public static void main(String[] args)
	{ 
		DataInputStream tw;
		int count =1;
		try
		{
			// This creates a DataInputStream object to tweetfile.txt that contains all the tweets.
			tw = new DataInputStream(new BufferedInputStream( new FileInputStream("tweetfile.txt")));
			String content = new String();
			// The below loop will convert each tweet into a separate file.
			while((content = tw.readLine()) != null)
			{
				if(!content.trim().equals(""))
				{
					FileWriter file = new FileWriter(String.valueOf(count)); 
					file.write(content);
				 	file.close();
				 	count++;
				}
				
			}
			tw.close();
		}
		catch(Exception e) { }
	}
}
