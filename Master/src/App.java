import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class App {

    private static String username = "hqueinnec";
    private static String distantPath = "/tmp/hugo";
    private static String distantComputersList = "../Resources/machines.txt";
    private static String localSplitsPath = "../Resources/splits";
    private static String workerMapTaskName = "Worker.jar";
    private static int numberOfDistantComputers;
    private static CountDownLatch splitDeploymentCountdown;
    private static CountDownLatch mapCountdown;
    private static CountDownLatch scpComputersCountdown;
    private static CountDownLatch shuffleCountdown;
    private static CountDownLatch reduceCountdown;



    private static int secondsTimeout = 10;

    public static void main(String[] args) throws Exception {

        numberOfDistantComputers = countLines(distantComputersList);
        splitDeploymentCountdown = new CountDownLatch(numberOfDistantComputers);
        mapCountdown = new CountDownLatch(numberOfDistantComputers);
        scpComputersCountdown = new CountDownLatch(numberOfDistantComputers);
        shuffleCountdown = new CountDownLatch(numberOfDistantComputers);
        reduceCountdown = new CountDownLatch(numberOfDistantComputers);

        DecimalFormat df = new DecimalFormat("#.#"); //for time measurement
        df.setRoundingMode(RoundingMode.CEILING);

        FileReader fr = new FileReader(distantComputersList) ;
        BufferedReader bu = new BufferedReader(fr) ;
        Scanner sc = new Scanner(bu) ;
        int computerIndex = 0;

        while(sc.hasNextLine()){
            String distantComputersListLine = sc.nextLine();
            ProcessBuilder pb = new ProcessBuilder("ssh", username+"@"+distantComputersListLine, "hostname");
            Process p = pb.start();

            boolean timeoutStatus = p.waitFor(secondsTimeout, TimeUnit.SECONDS);
    
            if (timeoutStatus){
                InputStream is = p.getInputStream();
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                String line;

                boolean e = false; //error?
    
                while ((line = br.readLine()) != null){
                    System.out.println("  DONE: Connection   ("+ line +")");
                }

                InputStream es = p.getErrorStream();
                BufferedReader ber = new BufferedReader(new InputStreamReader(es));
                String eLine;
                
                while ((eLine = ber.readLine()) != null){
                    System.out.println("--ERROR: Connection - "+ eLine);
                    e = true;
                }

                br.close();
                is.close();
                ber.close();
                es.close();

                if (!e) {deploySplit(distantComputersListLine, computerIndex);}                

            } else {
                System.out.println("  TMO : Connection   ("+distantComputersListLine+")");
                p.destroy();
            }
            computerIndex++;
        }
        sc.close();
        bu.close();
        fr.close();


        //waiting for all threads to send splits to distant computers
        boolean globalSplitTimeoutStatus = splitDeploymentCountdown.await(numberOfDistantComputers*secondsTimeout, TimeUnit.SECONDS);

        long startMapTime = System.nanoTime();   

        if (globalSplitTimeoutStatus){
            System.out.println("DONE: Split Deploy   (global)");

            FileReader fr2 = new FileReader(distantComputersList) ;
            BufferedReader bu2 = new BufferedReader(fr2) ;
            Scanner sc2 = new Scanner(bu2) ;
            int computerIndex2 = 0;

            while(sc2.hasNextLine()){
                String distantComputersListLine = sc2.nextLine();
                computeMap(distantComputersListLine, computerIndex2);         
                computerIndex2++;
            }
            sc2.close();
            bu2.close();
            fr2.close();

        } else {
            System.out.println("TMO : Split Deploy   (global)");
            return;
        }


        //waiting for all distant computers to finish the map phase
        boolean globalMapTimeoutStatus = mapCountdown.await(numberOfDistantComputers*secondsTimeout, TimeUnit.SECONDS);
        long elapsedMapTime = System.nanoTime() - startMapTime;

        if (globalMapTimeoutStatus){
            System.out.println("DONE: Map Compute    (global)      | "+ df.format(elapsedMapTime/1000000000.) + " s");

            FileReader fr3 = new FileReader(distantComputersList) ;
            BufferedReader bu3 = new BufferedReader(fr3) ;
            Scanner sc3 = new Scanner(bu3) ;

            while(sc3.hasNextLine()){
                String distantComputersListLine = sc3.nextLine();
                deployDistantComputersList(distantComputersListLine);         
            }
            sc3.close();
            bu3.close();
            fr3.close();

        } else {
            System.out.println("TMO : Map Compute    (global)");
            return;
        }

        //waiting for all distant computers to receive the list of distant computers
        boolean globalScpTimeoutStatus = scpComputersCountdown.await(numberOfDistantComputers*secondsTimeout, TimeUnit.SECONDS);

        long startShuffleTime = System.nanoTime();   

        if (globalScpTimeoutStatus){
            System.out.println("DONE: List Deploy    (global)");

            FileReader fr4 = new FileReader(distantComputersList) ;
            BufferedReader bu4 = new BufferedReader(fr4) ;
            Scanner sc4 = new Scanner(bu4) ;
            int computerIndex4 = 0;

            while(sc4.hasNextLine()){
                String distantComputersListLine = sc4.nextLine();
                computeShuffle(distantComputersListLine, computerIndex4);         
                computerIndex4++;
            }
            sc4.close();
            bu4.close();
            fr4.close();

        } else {
            System.out.println("TMO : List Deploy    (global)");
            return;
        }

        //waiting for all distant computers to finish the shuffle phase
        boolean globalShuffleTimeoutStatus = shuffleCountdown.await(numberOfDistantComputers*numberOfDistantComputers*secondsTimeout, TimeUnit.SECONDS);
        long elapsedShuffleTime = System.nanoTime() - startShuffleTime;

        long startReduceTime = System.nanoTime();   

        if (globalShuffleTimeoutStatus){
            System.out.println("DONE: Shuffle        (global)      | "+ df.format(elapsedShuffleTime/1000000000.) + " s");

            FileReader fr5 = new FileReader(distantComputersList) ;
            BufferedReader bu5 = new BufferedReader(fr5) ;
            Scanner sc5 = new Scanner(bu5) ;
            while(sc5.hasNextLine()){
                String distantComputersListLine = sc5.nextLine();
                computeReduce(distantComputersListLine);         
            }
            sc5.close();
            bu5.close();
            fr5.close();

        } else {
            System.out.println("TMO : Shuffle        (global)");
            return;
        }

        //waiting for all distant computers to finish the shuffle phase
        boolean globalReduceTimeoutStatus = reduceCountdown.await(numberOfDistantComputers*secondsTimeout, TimeUnit.SECONDS);
        long elapsedReduceTime = System.nanoTime() - startReduceTime;

        if (globalReduceTimeoutStatus){
            System.out.println("DONE: Reduce         (global)      | "+ df.format(elapsedReduceTime/1000000000.) + " s");

        } else {
            System.out.println("TMO : Reduce         (global)");
            return;
        }

    
    }
    

    private static void deploySplit(String hostname, int splitNumber) throws Exception {

        new Thread() {
            public void run() {

                ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "mkdir "+distantPath+"/splits");
                Process p1;

                try {
                    p1 = pb1.start();

                    boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

                    if (timeoutStatus1){
                        ProcessBuilder pb2 = new ProcessBuilder("scp", localSplitsPath+"/S"+splitNumber+".txt" ,  username+"@"+hostname + ":"+distantPath+"/splits");
                        Process p2 = pb2.start();
                
                        boolean timeoutStatus2 = p2.waitFor(secondsTimeout, TimeUnit.SECONDS);
            
                        if (timeoutStatus2){
                            System.out.println("  DONE: Split Deploy ("+hostname+")");
                            splitDeploymentCountdown.countDown();
            
                        } else {
                            System.out.println("  TMO : Split Deploy ("+hostname+")");
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


    private static void computeMap(String hostname, int splitNumber) throws Exception{

        new Thread() {
            public void run() {

                ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "cd "+distantPath+"; java -jar "+workerMapTaskName+" 0 S"+splitNumber+".txt");
                Process p1;

                try {
                    p1 = pb1.start();

                    boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

                    if (timeoutStatus1){
                        System.out.println("  DONE: Map Compute  ("+hostname+")");
                        mapCountdown.countDown();

                    } else {
                        System.out.println("  TMO : Map Compute  ("+hostname+")");
                        p1.destroy();
                    }
                
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

    }


    private static void deployDistantComputersList(String hostname) throws Exception{

        new Thread() {
            public void run() {

                ProcessBuilder pb1 = new ProcessBuilder("scp", distantComputersList ,  username+"@"+hostname + ":"+distantPath);
                Process p1;

                try {
                    p1 = pb1.start();

                    boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

                    if (timeoutStatus1){
                        System.out.println("  DONE: List Deploy  ("+hostname+")");
                        scpComputersCountdown.countDown();

                    } else {
                        System.out.println("  TMO : List Deploy  ("+hostname+")");
                        p1.destroy();
                    }
                
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

    }


    private static void computeShuffle(String hostname, int splitNumber) throws Exception{

        new Thread() {
            public void run() {

                ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "cd "+distantPath+"; java -jar "+workerMapTaskName+" 1 UM"+splitNumber+".txt");
                Process p1;

                try {
                    p1 = pb1.start();

                    boolean timeoutStatus1 = p1.waitFor(numberOfDistantComputers*secondsTimeout, TimeUnit.SECONDS);

                    if (timeoutStatus1){
                        System.out.println("  DONE: Shuffle      ("+hostname+")");
                        shuffleCountdown.countDown();

                    } else {
                        System.out.println("  TMO : Shuffle      ("+hostname+")");
                        p1.destroy();
                    }
                
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

    }

    private static void computeReduce(String hostname) throws Exception{

        new Thread() {
            public void run() {

                ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "cd "+distantPath+"; java -jar "+workerMapTaskName+" 2");
                Process p1;

                try {
                    p1 = pb1.start();

                    boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

                    if (timeoutStatus1){
                        System.out.println("  DONE: Reduce       ("+hostname+")");
                        reduceCountdown.countDown();

                    } else {
                        System.out.println("  TMO : Reduce       ("+hostname+")");
                        p1.destroy();
                    }
                
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

    }


    private static int countLines(String fileName) throws IOException {
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


