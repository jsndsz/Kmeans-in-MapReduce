import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class seedPoints {

	public static void main(String[] args) {
		int k = Integer.parseInt(args[0]);
		Random random = new Random();
		File file = new File("Kseeds.txt");
		try {
			BufferedWriter bf = new BufferedWriter(new FileWriter(file));
			for(int i =0 ; i <k ; i++) {
				int x = random.nextInt(10000);
				int y = random.nextInt(10000);
				bf.write("Center,"+x+","+y+"\n");
			}
			bf.close();
				
		} catch (IOException e) {
			e.printStackTrace();
		}
		
				
		
	}
	
}