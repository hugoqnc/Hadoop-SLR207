import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class App {
    private static int operatingMode;
    private static String splitFileName;
    private static String mapFileName;
    private static String username = "hqueinnec";
    private static String distantComputersList = "machines.txt";
    private static String distantPath = "/tmp/hugo";
    private static int numberOfDistantComputers;


    private static List<Integer> hashCodeList = new ArrayList<Integer>();

    private static int secondsTimeout = 5;
    
    public static void main(String[] args) throws Exception {

        operatingMode = Integer.valueOf(args[0]);

        if(operatingMode==0){ //map phase
            splitFileName = args[1];
            split();
        }
        if(operatingMode==1){ //hash and shuffle phase
            mapFileName = args[1];
            hash();
            shuffle();
        }
    }

    private static void split() throws Exception{
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

    private static void hash() throws Exception{
        System.out.println("STARTED");
        
        ProcessBuilder pb1 = new ProcessBuilder("mkdir","shuffles");
        Process p1 = pb1.start();

        boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

        if (timeoutStatus1){
            FileReader reader = new FileReader("maps/"+mapFileName);
            Scanner scanner = new Scanner(reader);

            while(scanner.hasNextLine()){

                String line = scanner.nextLine();
                String element = line.substring(0, line.indexOf(' ')); 
                int hashCode = element.hashCode();

                if(!hashCodeList.contains(hashCode)){
                    hashCodeList.add(hashCode);
                }

                String outputFileName = hashCode+"-"+java.net.InetAddress.getLocalHost().getHostName()+".txt";
                File file = new File("shuffles/"+outputFileName);
                file.createNewFile();
                FileWriter writer = new FileWriter(file, true); // if the file already exists, appends text to the existing file

                writer.write(line+"\n");

                writer.flush();
                writer.close();
            }

            scanner.close();
            reader.close();

        } else {
            System.out.println("TIMEOUT 'mkdir'");
            p1.destroy();
        }

        System.out.println("HASH CREATED");

    }

    private static void shuffle() throws Exception{
        System.out.println("STARTED");

        numberOfDistantComputers = countLines(distantComputersList);
        CountDownLatch hashesDeploymentCountdown = new CountDownLatch(hashCodeList.size());

        for (int hashCode : hashCodeList){
            String hashFileName = hashCode+"-"+java.net.InetAddress.getLocalHost().getHostName()+".txt";
            int computerID = hashCode%numberOfDistantComputers;

            String hostname = Files.readAllLines(Paths.get(distantComputersList)).get(computerID);
        
            new Thread() {
                public void run() {
    
                    ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "mkdir "+distantPath+"/shufflesreceived");
                    Process p1;
    
                    try {
                        p1 = pb1.start();
    
                        boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);
    
                        if (timeoutStatus1){
                            ProcessBuilder pb2 = new ProcessBuilder("scp", "shuffles/"+hashFileName ,  username+"@"+hostname + ":"+distantPath+"/shufflesreceived");
                            Process p2 = pb2.start();
                    
                            boolean timeoutStatus2 = p2.waitFor(secondsTimeout, TimeUnit.SECONDS);
                
                            if (timeoutStatus2){
                                System.out.println("  DONE: Hash Deploy  ("+hostname+")");
                                hashesDeploymentCountdown.countDown();
                
                            } else {
                                System.out.println("  TMO : Hash Deploy  ("+hostname+")");
                                p2.destroy();
                            }
                
                        } else {
                            System.out.println("  TMO : Mkdir        ("+hostname+")");
                            p1.destroy();
                        }
                    
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }.start();

        }

        //waiting for all threads to send hashes to distant computers
        boolean globalHashesTimeoutStatus = hashesDeploymentCountdown.await(numberOfDistantComputers*secondsTimeout, TimeUnit.SECONDS);

        if (globalHashesTimeoutStatus){
            System.out.println("SHUFFLE & HASH DEPLOY DONE");

        } else {
            System.out.println("TMO : Hash Deploy    (global)");
            return;
        }
    }

    private static int countLines(String fileName) throws Exception {
        FileReader fileReader = new FileReader(distantComputersList) ;
        LineNumberReader reader = null;
        try {
            reader = new LineNumberReader(fileReader);
            while ((reader.readLine()) != null);
            return reader.getLineNumber();
        } catch (Exception ex) {
            return -1;
        } finally { 
            if(reader != null){
                reader.close();
            }
            fileReader.close();
        }
        
    }

}
