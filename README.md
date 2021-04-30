# Hadoop MapReduce *from scratch*

## Table of Contents
- [Hadoop MapReduce *from scratch*](#hadoop-mapreduce-from-scratch)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Quick Setup](#quick-setup)
  - [Problems encountered and Resolution](#problems-encountered-and-resolution)
    - [Computing Distribution](#computing-distribution)
    - [Various Optimizations](#various-optimizations)
      - [Splitting](#splitting)
      - [Listing](#listing)
    - [SCP Limits](#scp-limits)
  - [Results](#results)

----

## Introduction
This project aims to create a simple implementation of the MapReduce concept in Java.
This will allow to visualize and empirically demonstrate Ahmdal's law.
I will use a word count process for this implementation, and I will compare the results between this distributed implementation and a sequential one.

This project is based upon a [practical work](https://remisharrock.fr/courses/simple-hadoop-mapreduce-from-scratch/) in Télécom Paris (SLR207 course), taught by Rémi Sharrock.


---

## Quick Setup
> All following commands will assume that you are in the `Hadoop` folder, which is the root of this project.

This project is divided into five folders:  `Clean`, `Deploy`, `Master`, `Worker` and `Resources`.
To launch the project, you first need to add the names of the distant machines you want to accesse in a `machines.txt` file inside the `Resources` folder.

Then, you have to add your input files in the folder `Resources>inputs`. These are text files where you want to count their number of words.

You can then run the cleaning phase, which will create a folder in `\tmp` in each distant machine. If this folder already exists (because of a previous execution), it will delete it.
```zsh
cd Clean; java -jar Clean.jar; cd ..
```

Then, you need to send to worker program to all your distant machines. This is achieved during the deploy phase.
```zsh
cd Deploy; java -jar Deploy.jar; cd ..
```

Now, all your distant computers are correcty configured. From now on, you can launch the Master program on as much input files as you want. For an input file named `input.txt`, you can run the command :
```zsh
cd Master; java -jar Master.jar input.txt; cd ..
```
This will create the folder `Resources>outputs`in which you will find all outputs of Master execution, in a text file named `input-REDUCED.txt`. A Master execution will also create a folder `Resources>splits`which will contain splits of the text file `input.txt`, however this folder will be replaced by new splits at each execution of Master.

---

## Problems encountered and Resolution

### Computing Distribution
parler du séquentiel, puis des threads, puis de la liste

The goal of this program is to be quicker than a sequential version, at least for large files. This means that we have to compute a maximum of tasks in parallel on all distant computers. According to Amdhal's law, the speedup will increase with the proportion of parallelized tasks and with the number of processors.

On my first attempt to implement the MapReduce algorithm, I just launched a `ProcessBuilder` (to execute a command on a distant machine) and waited for its completion, before launching the next `ProcessBuilder`. 
```java
for(hostname : computerList){
      ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "mkdir " + distantPath);
  Process p1 = pb1.start();
  boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);
}
```
But by waiting for each task to finish, the execution of this program is necessarily slower (or equal) to the time of execution of a sequential version. So I searched for a parallelized version of this code. My first attempt was to use `Threads` and `CountDownLatch`, to allow the Master to simultaneously wait for several distant machines.
```java
mkdirCountdown = new CountDownLatch(numberOfDistantComputers);

for(hostname : computerList){
  new Thread() {
    public void run() {
      ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "mkdir " + distantPath);
      Process p1 = pb1.start();
      boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);
      mkdirCountdown.countDown()
    }
  }.start();
}

//waiting for all threads to finish waiting for the distant computer
boolean globalTimeoutStatus = mkdirCountdown.await(secondsTimeout, TimeUnit.SECONDS);
```
But this was a very complex solution to a very simple problem. A more efficient idea was to use two `for` loops, one to launch all `ProcessBuilder`, and a second one to wait for them.

```java
mapOfProcesses = new HashMap<String,Process>();

...

for (String hostname : mapOfProcesses.keySet()){
  ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "mkdir " + distantPath);
  Process p1 = pb1.start();
  mapOfProcesses.replace(hostname, p1);
}

for(String hostname : mapOfProcesses.keySet()){
  Process p = mapOfProcesses.get(hostname);
  boolean timeoutStatus1 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);
}
```
Ultimately, I used this last method to parallelize as most code as possible. I parallelized cleaning, deployment of files, directory creation, shuffling, reducing. The only long process that I have not parallelized is the gathering of all distant reduce files. This phase aims at reading the reduce file of each distant machine and writing its content on the local `input-REDUCED.txt` text file. To avoid conflicts with multiple wirters, I kept this phase sequential.

### Various Optimizations
As this implementation is meant to be used on huge text files (in the hope of seeing better performance than the sequential version), I added several optimizations to make the program execution - a lot - faster than my original implementation.

The two biggest optimizations I had to made concern splitting and listing, and transformed my program to "unusable" to "really fast".

#### Splitting
Splitting the `input.txt`is essential for the MapReduce algorithm.

My original idea to split this file was to create as much empty split files as the number of distant computers. Then, I would read each word `input.txt`, and copy it to a split file chosen in a round-robin way (with the modulo operator: `wordNumber%numberOfDistantComputers`).x

#### Listing


### SCP Limits


## Results 
graphique barre somme des temps distribués vs temps séquentiel

