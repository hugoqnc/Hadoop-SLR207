import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;


public class App {

    private static String username = "hqueinnec";
    private static String distantPath = "/tmp/hugo";
    private static String distantComputersList = "../Resources/machines.txt";
    private static String localSplitsPath = "../Resources/splits/";
    private static String inputsPath = "../Resources/inputs/";
    private static String outputPath = "../Resources/outputs/";
    private static String workerMapTaskName = "Worker.jar";
    private static String outputAllReducedFileName = "all_reduced.txt";
    private static int secondsTimeout = 120;
    private static int maxSimultaneousScpConnexions = 13;
    private static boolean verbose = true; //if true, shows DONE status for each individual distant computer

    private static String inputFileName;
    private static HashMap<String,Process> mapOfProcesses;
    private static HashMap<String,Integer> mapOfSplitNumbers;
    private static int numberOfDistantComputers;
    
    private static int connectionCount = 0;
    private static int mkdirCount = 0;
    private static int splitDeploymentCount = 0;
    private static int mapCount = 0;
    private static int scpComputersCount = 0;
    private static int shuffleCount = 0;
    private static int reduceCount = 0;

    public static void main(String[] args) throws Exception { //enter input file name as first argument (it should be placed in ../Resources/inputs/)
        long globalStartTime = System.nanoTime();   

        inputFileName = args[0];
        mapOfProcesses = new HashMap<String,Process>();
        mapOfSplitNumbers = new HashMap<String,Integer>();

        numberOfDistantComputers = countLines(distantComputersList);

        DecimalFormat df = new DecimalFormat("#.#"); //for time measurement
        df.setRoundingMode(RoundingMode.CEILING);

        System.out.println("\nSTART: Splitting      (local)");

        ProcessBuilder pb0 = new ProcessBuilder("rm","-rf",localSplitsPath);
        Process p0 = pb0.start();
        boolean timeoutStatus0 = p0.waitFor(secondsTimeout, TimeUnit.SECONDS);

        if (!timeoutStatus0){
            System.out.println("TMOUT: 'rm -rf'");
            p0.destroy();
            return;
        }

        ProcessBuilder pb1 = new ProcessBuilder("mkdir",localSplitsPath);
        Process p1 = pb1.start();
        boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

        if (!timeoutStatus1){
            System.out.println("TMOUT: 'mkdir'");
            p1.destroy();
            return;
        }



        ProcessBuilder pb13 = new ProcessBuilder("gsplit","-n",""+numberOfDistantComputers,"-d","--additional-suffix",".txt",inputFileName,"../splits/S");

        pb13.directory(new File(inputsPath));
        Process p13 = pb13.start();

        boolean timeoutStatus13 = p13.waitFor(secondsTimeout, TimeUnit.SECONDS);

        if (!timeoutStatus13){
            System.out.println("TMOUT: 'gsplit'");
            p13.destroy();
            return;
        } 

        File dir = new File(localSplitsPath);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File child : directoryListing) {
                String fileName = child.getName();
                if (fileName.substring(0,2).equals("S0")){
                    //System.out.println(fileName+" -> "+"S"+fileName.substring(2));
                    Path path = Paths.get(localSplitsPath+fileName);
                    Files.move(path, path.resolveSibling("S"+fileName.substring(2)));
                }
            }
        } else {
            System.out.println("--ERROR: renaming files");
            return;
        }

        System.out.println("DONE : Splitting      (local)");
        System.out.println("-----------------------------------");


        //connecting test
        System.out.println("START: Connection     (global)");

        FileReader fr = new FileReader(distantComputersList) ;
        BufferedReader bu = new BufferedReader(fr) ;
        Scanner sc = new Scanner(bu) ;

        int i = 0;
        while(sc.hasNextLine()){
            String distantComputersListLine = sc.nextLine();
            ProcessBuilder pb = new ProcessBuilder("ssh", username+"@"+distantComputersListLine, "hostname");
            Process p = pb.start();
            mapOfProcesses.put(distantComputersListLine, p);
            mapOfSplitNumbers.put(distantComputersListLine, i);
            i++;
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
        System.out.println("-----------------------------------");


        //mkdir split
        System.out.println("START: Mkdir          (global)");

        for (String hostname : mapOfProcesses.keySet()){
            mkdir(hostname, distantPath+"/splits");
        }

        for(String hostname : mapOfProcesses.keySet()){
            Process p = mapOfProcesses.get(hostname);
            boolean timeoutStatus2 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);

            if (timeoutStatus2){
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
        System.out.println("-----------------------------------");


        

        //split deploy
        System.out.println("START: Split Deploy   (global)");

        for (String hostname : mapOfProcesses.keySet()){
            deploySplit(hostname, mapOfSplitNumbers.get(hostname));
/*         }

        for(String hostname : mapOfProcesses.keySet()){ */
            Process p = mapOfProcesses.get(hostname);
            boolean timeoutStatus2 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);
            if (timeoutStatus2){
                if (verbose) System.out.println("  DONE : Split Deploy ("+hostname+")");
                splitDeploymentCount++;

            } else {
                System.out.println("  TMOUT: Split Deploy ("+hostname+")");
                p.destroy();
            }
        }
        if(splitDeploymentCount!=numberOfDistantComputers){
            System.out.println("TMOUT: Split Deploy   (global)");
            return;
        }

        System.out.println("DONE : Split Deploy   (global)");
        System.out.println("-----------------------------------");


        //map computation
        long startMapTime = System.nanoTime();
        System.out.println("START: Map Compute    (global)");

        for (String hostname : mapOfProcesses.keySet()){
            computeMap(hostname, mapOfSplitNumbers.get(hostname));        }

        for(String hostname : mapOfProcesses.keySet()){
            Process p = mapOfProcesses.get(hostname);
            boolean timeoutStatus2 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);

            if (timeoutStatus2){
                if(verbose) System.out.println("  DONE : Map Compute  ("+hostname+")");
                mapCount++;
            } else {
                System.out.println("  TMOUT: Map Compute  ("+hostname+")");
                p.destroy();
            }
        }

        if(mapCount!=numberOfDistantComputers){
            System.out.println("TMOUT: Map Compute    (global)");
            return;
        }
        long elapsedMapTime = System.nanoTime() - startMapTime;
        System.out.println("DONE : Map Compute    (global)      | "+ df.format(elapsedMapTime/1000000000.) + " s");
        System.out.println("-----------------------------------");



        //deploy distant computer list
        System.out.println("START: List Deploy    (global)");

        for (String hostname : mapOfProcesses.keySet()){
            deployDistantComputersList(hostname);         
        }

        for(String hostname : mapOfProcesses.keySet()){
            Process p = mapOfProcesses.get(hostname);
            boolean timeoutStatus2 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);
            if (timeoutStatus2){
                if (verbose) System.out.println("  DONE : List Deploy  ("+hostname+")");
                scpComputersCount++;

            } else {
                System.out.println("  TMOUT: List Deploy  ("+hostname+")");
                p.destroy();
            }
        }
        if(scpComputersCount!=numberOfDistantComputers){
            System.out.println("TMOUT: List Deploy    (global)");
            return;
        }

        System.out.println("DONE : List Deploy    (global)");
        System.out.println("-----------------------------------");



        //shuffle
        long startShuffleTime = System.nanoTime();   
        System.out.println("START: Shuffle        (global)");

        int scpProcessCount = 0;
        String hostnameToWaitFor = "";
        for (String hostname : mapOfProcesses.keySet()){
            scpProcessCount++;
            if(scpProcessCount%maxSimultaneousScpConnexions==1){
                hostnameToWaitFor = hostname;
            }

            computeShuffle(hostname, mapOfSplitNumbers.get(hostname));

            if(scpProcessCount%maxSimultaneousScpConnexions==0){
                Process p = mapOfProcesses.get(hostnameToWaitFor);
                boolean timeoutStatus2 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);
                if (!timeoutStatus2){
                    System.out.println("  TMOUT: Shuffle      ("+hostname+")");
                    p.destroy();
                }
            }
        }

        for(String hostname : mapOfProcesses.keySet()){
            Process p = mapOfProcesses.get(hostname);
            boolean hasTimedOut = computeShuffleStatus(p);
            boolean timeoutStatus2 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);

            if (timeoutStatus2 && !hasTimedOut){
                if (verbose) System.out.println("  DONE : Shuffle      ("+hostname+")");
                shuffleCount++;

            } else {
                System.out.println("  TMOUT: Shuffle      ("+hostname+")");
                p.destroy();
            }
        }
        if(shuffleCount!=numberOfDistantComputers){
            System.out.println("TMOUT: Shuffle        (global)");
            return;
        }
        long elapsedShuffleTime = System.nanoTime() - startShuffleTime;
        System.out.println("DONE : Shuffle        (global)      | "+ df.format(elapsedShuffleTime/1000000000.) + " s");
        System.out.println("-----------------------------------");


        //reduce
        long startReduceTime = System.nanoTime();   
        System.out.println("START: Reduce         (global)");

        for (String hostname : mapOfProcesses.keySet()){
            computeReduce(hostname);
        }

        for(String hostname : mapOfProcesses.keySet()){
            Process p = mapOfProcesses.get(hostname);
            boolean timeoutStatus2 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);

            if (timeoutStatus2){
                if (verbose) System.out.println("  DONE : Reduce       ("+hostname+")");
                reduceCount++;

            } else {
                System.out.println("  TMOUT: Reduce       ("+hostname+")");
                p.destroy();
            }
        }
        if(reduceCount!=numberOfDistantComputers){
            System.out.println("TMOUT: Reduce         (global)");
            return;
        }
        long elapsedReduceTime = System.nanoTime() - startReduceTime;
        System.out.println("DONE : Reduce         (global)      | "+ df.format(elapsedReduceTime/1000000000.) + " s");
        System.out.println("-----------------------------------");



        //gather reduce
        System.out.println("START: Gather Reduce  (global)");
        boolean successfulGatherReduces = gatherReduces();

        if (successfulGatherReduces){
            System.out.println("DONE : Gather Reduce  (global)");
            System.out.println("-----------------------------------");

        } else {
            System.out.println("TMOUT: Gather Reduce  (global)");
            return;
        }

        long globalElapsedTime = System.nanoTime() - globalStartTime;
        System.out.println("\nTOTAL TIME : "+ df.format(globalElapsedTime/1000000000.) + " s\n");

    }

    private static void mkdir(String hostname, String directoryName) throws Exception{
        ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "mkdir " + directoryName);
        Process p1 = pb1.start();
        mapOfProcesses.replace(hostname, p1);
    }
    

    private static void deploySplit(String hostname, int splitNumber) throws Exception {
        ProcessBuilder pb1 = new ProcessBuilder("scp", localSplitsPath+"S"+splitNumber+".txt" ,  username+"@"+hostname + ":"+distantPath+"/splits");
        Process p1 = pb1.start();
        mapOfProcesses.replace(hostname, p1);       
    }


    private static void computeMap(String hostname, int splitNumber) throws Exception{
        ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "cd "+distantPath+"; java -jar "+workerMapTaskName+" 0 S"+splitNumber+".txt");
        Process p1 = pb1.start();
        mapOfProcesses.replace(hostname, p1);       
    }


    private static void deployDistantComputersList(String hostname) throws Exception{
        ProcessBuilder pb1 = new ProcessBuilder("scp", distantComputersList ,  username+"@"+hostname + ":"+distantPath);
        Process p1 = pb1.start();
        mapOfProcesses.replace(hostname, p1);       
    }


    private static void computeShuffle(String hostname, int splitNumber) throws Exception{ //return true if it timed out
        ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "cd "+distantPath+"; java -jar "+workerMapTaskName+" 1 UM"+splitNumber+".txt");
        Process p1 = pb1.start();
        mapOfProcesses.replace(hostname, p1);   
    }

    private static boolean computeShuffleStatus(Process p) throws Exception{
        boolean hasTimedOut = false;

        try{
            InputStream is = p.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;

            while ((line = br.readLine()) != null){ //to empty the worker buffer, we need to read it 
                //System.out.println(line); //DEBUG
            }
            
            InputStream es = p.getErrorStream();
            BufferedReader ber = new BufferedReader(new InputStreamReader(es));
            String eLine;
            
            while ((eLine = ber.readLine()) != null){
                System.out.println(eLine);
                if(eLine.equals("TIMEOUT SHUFFLE")){
                    hasTimedOut = true;
                }
            }
    
        } catch (Exception e) {
            e.printStackTrace();
        }

        return hasTimedOut;
    }

    private static void computeReduce(String hostname) throws Exception{
        ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "cd "+distantPath+"; java -jar "+workerMapTaskName+" 2");
        Process p1 = pb1.start();
        mapOfProcesses.replace(hostname, p1);
    }

    private static boolean gatherReduces() throws Exception {

        ProcessBuilder pb1 = new ProcessBuilder("mkdir",outputPath);
        Process p1 = pb1.start();

        boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

        if (!timeoutStatus1){
            System.out.println("TIMEOUT 'mkdir'");
            p1.destroy();
            return false;
        }

        String outputFileName = inputFileName.substring(0, inputFileName.indexOf('.')) + "-REDUCED.txt"; 

        File file = new File(outputPath+outputFileName);
        file.createNewFile();
        FileWriter writer = new FileWriter(file);


        FileReader fr = new FileReader(distantComputersList) ;
        BufferedReader bu = new BufferedReader(fr) ;
        Scanner sc = new Scanner(bu) ;

        while(sc.hasNextLine()){
            String distantComputersListLine = sc.nextLine();

            ProcessBuilder pb3= new ProcessBuilder("ssh", username+"@"+distantComputersListLine,"; cd "+distantPath,"; cat",outputAllReducedFileName); 
            Process p3 = pb3.start();

            InputStream is3 = p3.getInputStream();
            BufferedReader br3 = new BufferedReader(new InputStreamReader(is3));
            String lineCurrentReduce;

            while ((lineCurrentReduce = br3.readLine()) != null){
                writer.write(lineCurrentReduce+"\n");
            }

            writer.flush();
            br3.close();
            is3.close();

            boolean timeoutStatus3 = p3.waitFor(secondsTimeout, TimeUnit.SECONDS);

            if(timeoutStatus3){


            } else {
                System.out.println("  TMOUT: Gather Red   ("+distantComputersListLine+")");
                p3.destroy();
                sc.close();
                bu.close();
                fr.close();
                writer.close();
                return false;
                
            }
            
            if (verbose) System.out.println("  DONE : Gather Red   ("+distantComputersListLine+")");
        }

        sc.close();
        bu.close();
        fr.close();

        writer.close();
        return true;
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


