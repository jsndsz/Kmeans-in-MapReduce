import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class PointGenerator {
	
	Random random = new Random();
	public static void main(String[] args) {
		PointGenerator pointList = new PointGenerator();
		pointList.createPoints("PointsLarge.txt");
		
	}
	
	public void createPoints(String f) {
		File fs = new File(f);
		try {
			BufferedWriter bf = new BufferedWriter(new FileWriter(fs));
			for (int i = 0; i < 6000000;i++) {
				int x = random.nextInt(10000-0)+0;
				int y = random.nextInt(10000-0)+0;
				bf.write("Points,"+x+","+y+"\n");
			}
			bf.close() ;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
