import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class test {
	public static void main(String[] args) throws IOException {

//		File mySerial = new File("./resources/" + "page" + "" + 1+".txt");
//		if (mySerial.delete())
//			System.out.println("File deleted successfully");
//		try {
//			File oldFile;
//			File newFile;
//			for (int i = 1; i < 4; i++) {
//				int temp = i;
//				oldFile = new File("./resources/" + "page" + "" + ++temp+".txt");
//				newFile = new File("./resources/" + "page" + "" + i+".txt");
//				if (oldFile.renameTo(newFile)) {
//					System.out.println("File renamed successfully");
//				} else {
//					System.out.println("Failed to rename file");
//				}
//			}
//		} catch (Exception e) {// msh 3ayez e3ml 7aga
//		}

		Date myDate = new Date(Long.MAX_VALUE);
		System.out.println(myDate);
	}
}
