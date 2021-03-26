import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class App {
    private static int operatingMode;
    private static String splitFileName;

    private static int secondsTimeout = 5;
    
    public static void main(String[] args) throws Exception {

        operatingMode = Integer.valueOf(args[0]);
        splitFileName = args[1];

        if(operatingMode==0){
            splitToMap();
        }
    }

    private static void splitToMap() throws Exception{
        //long startTime = System.currentTimeMillis();
        System.out.println("STARTED");
        
        ProcessBuilder pb1 = new ProcessBuilder("mkdir","maps");
        Process p1 = pb1.start();

        boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

        if (timeoutStatus1){
            String outputFileName = "UM"+splitFileName.charAt(1)+".txt";
            File file = new File("maps/"+outputFileName);
            file.createNewFile();
            FileWriter writer = new FileWriter(file);

            FileReader reader = new FileReader("splits/"+splitFileName);
            Scanner scanner = new Scanner(reader);
            
            while(scanner.hasNext()){
                String element = scanner.next();
                writer.write(element + " 1\n");
            }

            writer.flush();
            writer.close();

            scanner.close();
            reader.close();

        } else {
            System.out.println("TIMEOUT 'mkdir'");
            p1.destroy();
        }

        System.out.println("MAP CREATED");


        //long endTime   = System.currentTimeMillis();
        //long totalTime = endTime - startTime;
        //System.out.println("Execution time: "+totalTime+" ms");

    }

}
