import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {

    private static String username = "hqueinnec";
    private static String distantComputersList = "../Resources/machines.txt";
    private static String workerFileName = "../Worker/Worker.jar";
    private static String distantPath = "/tmp/hugo";
    private static int secondsTimeout = 10;
    private static boolean verbose = false; //if true, shows DONE status for each individual distant computer

    private static HashMap<String,Process> mapOfProcesses;

    private static int numberOfDistantComputers = 0;
    private static int connectionCount = 0;
    private static int mkdirCount = 0;
    private static int deployCount = 0;

    public static void main(String[] args) throws Exception {
        mapOfProcesses = new HashMap<String,Process>();

        FileReader fr = new FileReader(distantComputersList) ;
        BufferedReader bu = new BufferedReader(fr) ;
        Scanner sc = new Scanner(bu) ;

        //connecting test
        System.out.println("START: Connection     (global)");

        while(sc.hasNextLine()){
            numberOfDistantComputers++;
            String distantComputersListLine = sc.nextLine();
            ProcessBuilder pb = new ProcessBuilder("ssh", username+"@"+distantComputersListLine, "hostname");
            Process p = pb.start();
            mapOfProcesses.put(distantComputersListLine, p);
        }

        sc.close();
        bu.close();
        fr.close();

        for(String hostname : mapOfProcesses.keySet()){
            Process p = mapOfProcesses.get(hostname);
            boolean timeoutStatus = p.waitFor(secondsTimeout, TimeUnit.SECONDS);
    
            if (timeoutStatus){
                InputStream is = p.getInputStream();
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                String line;

                boolean e = false; //error?
    
                while ((line = br.readLine()) != null){
                    if(verbose) System.out.println("  DONE : Connection   ("+ line +")");
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

                if (!e) {connectionCount++;}

            } else {
                System.out.println("  TMOUT: Connection   ("+hostname+")");
                p.destroy();
            }
        }

        if(connectionCount!=numberOfDistantComputers){
            System.out.println("TMOUT: Connection     (global)");
            return;
        }
        System.out.println("DONE : Connection     (global)");


        //mkdir
        System.out.println("START: Mkdir          (global)");

        for (String hostname : mapOfProcesses.keySet()){
            mkdir(hostname, distantPath);
        }

        for(String hostname : mapOfProcesses.keySet()){
            Process p = mapOfProcesses.get(hostname);
            boolean timeoutStatus1 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);

            if (timeoutStatus1){
                if(verbose) System.out.println("  DONE : Mkdir        ("+hostname+")");
                mkdirCount++;
            } else {
                System.out.println("  TMOUT: Mkdir        ("+hostname+")");
                p.destroy();
            }
        }

        if(mkdirCount!=numberOfDistantComputers){
            System.out.println("TMOUT: Mkdir          (global)");
            return;
        }
        System.out.println("DONE : Mkdir          (global)");


        //deploy
        System.out.println("START: Deployment     (global)");

        for (String hostname : mapOfProcesses.keySet()){
            deploy(hostname);
        }

        for(String hostname : mapOfProcesses.keySet()){
            Process p = mapOfProcesses.get(hostname);
            boolean timeoutStatus1 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);

            if (timeoutStatus1){  
                if (verbose) System.out.println("  DONE : Deployment   ("+hostname+")");
                deployCount++;
            } else {
                System.out.println("  TMOUT: Deployment   ("+hostname+")");
                p.destroy();
            }
        }

        if(deployCount!=numberOfDistantComputers){
            System.out.println("TMOUT: Deployment     (global)");
            return;
        }
        System.out.println("DONE : Deployment     (global)");

    }

    private static void mkdir(String hostname, String directoryName) throws Exception{
        ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "mkdir " + directoryName);
        Process p1 = pb1.start();
        mapOfProcesses.replace(hostname, p1);
    }

    private static void deploy(String hostname) throws Exception {
        ProcessBuilder pb2 = new ProcessBuilder("scp", workerFileName ,  username+"@"+hostname + ":"+distantPath);
        Process p2 = pb2.start();
        mapOfProcesses.replace(hostname, p2);
    }

}
