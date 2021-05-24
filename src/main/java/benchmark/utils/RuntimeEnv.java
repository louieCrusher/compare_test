package benchmark.utils;

import oshi.SystemInfo;
import oshi.hardware.*;
import oshi.hardware.CentralProcessor.TickType;
import oshi.software.os.*;
import oshi.software.os.OperatingSystem.ProcessSort;
import oshi.util.FormatUtil;
import oshi.util.Util;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public enum RuntimeEnv {
    sjh("develop environment",
            "Intel(R) Core(TM) i5-4570 CPU @ 3.20GHz",
            "3.6 GHz",
            4,
            "23.4 GiB",
            "GNU/Linux Ubuntu 18.04.3 LTS (Bionic Beaver) build 5.0.0-29-generic (64 bits)",
            "Java(TM) SE Runtime Environment (1.8.0_131-b11) Java HotSpot(TM) 64-Bit Server VM (25.131-b11)"),
    zh("zhangh workstation",
            "Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz",
            "2.3 GHz",
            20,
            "63.8 GiB",
            "Microsoft Windows 10 build 17763 (64 bits)",
            "Java(TM) SE Runtime Environment (1.8.0_202-b08) Java HotSpot(TM) 64-Bit Server VM (25.202-b08)"),
    unknown();


    final String cpu;
    final String cpuFreq;
    final int numOfPhysicalCores;
    final String physicalMem;
    final String description;
    final String os;
    final String jvm;

    RuntimeEnv(String description, String cpu, String cpuFreq, int numOfPhysicalCores, String physicalMem, String os, String jvm) {
        this.cpu = cpu;
        this.cpuFreq = cpuFreq;
        this.numOfPhysicalCores = numOfPhysicalCores;
        this.physicalMem = physicalMem;
        this.description = description;
        this.os = os;
        this.jvm = jvm;
    }

    RuntimeEnv() {
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        OperatingSystem os = si.getOperatingSystem();
        this.cpu = hal.getProcessor().toString();
        this.cpuFreq = FormatUtil.formatHertz(hal.getProcessor().getMaxFreq());
        this.numOfPhysicalCores = hal.getProcessor().getPhysicalProcessorCount();
        this.physicalMem = FormatUtil.formatBytes(hal.getMemory().getTotal());
        this.os = os.getManufacturer() + ' ' + os.getFamily() + ' ' + os.getVersion().toString() + " (" + os.getBitness() + " bits)";
        this.jvm = System.getProperty("java.runtime.name") + " (" + System.getProperty("java.runtime.version") + ") " +
                System.getProperty("java.vm.name") + " (" + System.getProperty("java.vm.version") + ")";
        this.description = "unknown current runtime environment";
    }

    private static RuntimeEnv currentEnv;

    public static RuntimeEnv getCurrentEnv() {
        if (currentEnv == null) {
            for (RuntimeEnv env : RuntimeEnv.values()) {
                if (env != unknown &&
                        Objects.equals(env.cpu, unknown.cpu) &&
                        Objects.equals(env.physicalMem, unknown.physicalMem) &&
                        env.numOfPhysicalCores == unknown.numOfPhysicalCores &&
                        Objects.equals(env.jvm, unknown.jvm)
                ) {
                    currentEnv = env;
                    return env;
                }
            }
            currentEnv = unknown;
            System.out.println(unknown);
            return unknown;
        } else {
            return currentEnv;
        }
    }

    public static void main(String[] args) {
        System.out.println(); //sun.arch.data.model  java.runtime.name
        System.out.println(RuntimeEnv.unknown);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name())
                .append("[").append(cpu).append("]")
                .append("[").append(numOfPhysicalCores).append(" Physical Cores]")
                .append("[MaxCPUFreq: ").append(cpuFreq).append("]")
                .append("[Mem: ").append(physicalMem).append("]")
                .append("[OS: ").append(os).append("]")
                .append("[JVM: ").append(jvm).append("]")
                .append("[").append(description).append("]");
        return sb.toString();
    }

    public static void computerInfo() {
        System.out.println("Initializing System...");
        SystemInfo si = new SystemInfo();

        HardwareAbstractionLayer hal = si.getHardware();
        OperatingSystem os = si.getOperatingSystem();

        printOperatingSystem(os);

        System.out.println("Checking computer system...");
        printComputerSystem(hal.getComputerSystem());

        System.out.println("Checking Processor...");
        printProcessor(hal.getProcessor());

        System.out.println("Checking Memory...");
        printMemory(hal.getMemory());

        System.out.println("Checking CPU...");
        printCpu(hal.getProcessor());

        System.out.println("Checking Processes...");
        printProcesses(os, hal.getMemory());

        System.out.println("Checking Sensors...");
        printSensors(hal.getSensors());

        System.out.println("Checking Power sources...");
        printPowerSources(hal.getPowerSources());

        System.out.println("Checking Disks...");
        printDisks(hal.getDiskStores());

        System.out.println("Checking File System...");
        printFileSystem(os.getFileSystem());

        System.out.println("Checking Network interfaces...");
        printNetworkInterfaces(hal.getNetworkIFs());

        System.out.println("Checking Network parameters...");
        printNetworkParameters(os.getNetworkParams());

        // hardware: USB devices
        System.out.println("Checking USB Devices...");
        printUsbDevices(hal.getUsbDevices(true));

//        // hardware: displays
//        System.out.println("Checking Displays...");
//        printDisplays(hal.getDisplays());

//        System.out.println("Checking Sound Cards...");
//        printSoundCards(hal.getSoundCards());
    }

    private static void printOperatingSystem(final OperatingSystem os) {
        System.out.println(String.valueOf(os));
        System.out.println("Booted: " + Instant.ofEpochSecond(os.getSystemBootTime()));
        System.out.println("Uptime: " + FormatUtil.formatElapsedSecs(os.getSystemUptime()));
        System.out.println("Running with" + (os.isElevated() ? "" : "out") + " elevated permissions.");
    }

    private static void printComputerSystem(final ComputerSystem computerSystem) {
        System.out.println("system: " + computerSystem.toString());
        System.out.println("|--firmware: " + computerSystem.getFirmware().toString());
        System.out.println("|--baseboard: " + computerSystem.getBaseboard().toString());
    }

    private static void printProcessor(CentralProcessor processor) {
        System.out.println(processor.toString());
    }

    private static void printMemory(GlobalMemory memory) {
        System.out.println("Memory(total/available): " + memory.getTotal() + "/" + memory.getAvailable());
        VirtualMemory vm = memory.getVirtualMemory();
        System.out.println("Swap(total/used):" + vm.getSwapTotal() + "/" + vm.getSwapUsed());
    }

    private static void printCpu(CentralProcessor processor) {
        System.out.println("Context Switches/Interrupts: " + processor.getContextSwitches() + " / " + processor.getInterrupts());

        long[] prevTicks = processor.getSystemCpuLoadTicks();
        long[][] prevProcTicks = processor.getProcessorCpuLoadTicks();
        System.out.println("CPU, IOWait, and IRQ ticks @ 0 sec:" + Arrays.toString(prevTicks));
        // Wait a second...
        Util.sleep(1000);
        long[] ticks = processor.getSystemCpuLoadTicks();
        System.out.println("CPU, IOWait, and IRQ ticks @ 1 sec:" + Arrays.toString(ticks));
        long user = ticks[TickType.USER.getIndex()] - prevTicks[TickType.USER.getIndex()];
        long nice = ticks[TickType.NICE.getIndex()] - prevTicks[TickType.NICE.getIndex()];
        long sys = ticks[TickType.SYSTEM.getIndex()] - prevTicks[TickType.SYSTEM.getIndex()];
        long idle = ticks[TickType.IDLE.getIndex()] - prevTicks[TickType.IDLE.getIndex()];
        long iowait = ticks[TickType.IOWAIT.getIndex()] - prevTicks[TickType.IOWAIT.getIndex()];
        long irq = ticks[TickType.IRQ.getIndex()] - prevTicks[TickType.IRQ.getIndex()];
        long softirq = ticks[TickType.SOFTIRQ.getIndex()] - prevTicks[TickType.SOFTIRQ.getIndex()];
        long steal = ticks[TickType.STEAL.getIndex()] - prevTicks[TickType.STEAL.getIndex()];
        long totalCpu = user + nice + sys + idle + iowait + irq + softirq + steal;

        System.out.println(String.format(
                "User: %.1f%% Nice: %.1f%% System: %.1f%% Idle: %.1f%% IOwait: %.1f%% IRQ: %.1f%% SoftIRQ: %.1f%% Steal: %.1f%%",
                100d * user / totalCpu, 100d * nice / totalCpu, 100d * sys / totalCpu, 100d * idle / totalCpu,
                100d * iowait / totalCpu, 100d * irq / totalCpu, 100d * softirq / totalCpu, 100d * steal / totalCpu));
        System.out.println(String.format("CPU load: %.1f%%", processor.getSystemCpuLoadBetweenTicks(prevTicks) * 100));
        double[] loadAverage = processor.getSystemLoadAverage(3);
        System.out.println("CPU load averages:" + (loadAverage[0] < 0 ? " N/A" : String.format(" %.2f", loadAverage[0]))
                + (loadAverage[1] < 0 ? " N/A" : String.format(" %.2f", loadAverage[1]))
                + (loadAverage[2] < 0 ? " N/A" : String.format(" %.2f", loadAverage[2])));
        // per core CPU
        StringBuilder procCpu = new StringBuilder("CPU load per processor:");
        double[] load = processor.getProcessorCpuLoadBetweenTicks(prevProcTicks);
        for (double avg : load) {
            procCpu.append(String.format(" %.1f%%", avg * 100));
        }
        System.out.println(procCpu.toString());
        long freq = processor.getVendorFreq();
        if (freq > 0) {
            System.out.println("Vendor Frequency: " + FormatUtil.formatHertz(freq));
        }
        freq = processor.getMaxFreq();
        if (freq > 0) {
            System.out.println("Max Frequency: " + FormatUtil.formatHertz(freq));
        }
        long[] freqs = processor.getCurrentFreq();
        if (freqs[0] > 0) {
            StringBuilder sb = new StringBuilder("Current Frequencies: ");
            for (int i = 0; i < freqs.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(FormatUtil.formatHertz(freqs[i]));
            }
            System.out.println(sb.toString());
        }
    }

    private static void printProcesses(OperatingSystem os, GlobalMemory memory) {
        System.out.println("Processes: " + os.getProcessCount() + ", Threads: " + os.getThreadCount());
        // Sort by highest CPU
        List<OSProcess> procs = Arrays.asList(os.getProcesses(5, ProcessSort.CPU));

        System.out.println("   PID  %CPU %MEM       VSZ       RSS Name");
        for (int i = 0; i < procs.size() && i < 5; i++) {
            OSProcess p = procs.get(i);
            System.out.println(String.format(" %5d %5.1f %4.1f %9s %9s %s", p.getProcessID(),
                    100d * (p.getKernelTime() + p.getUserTime()) / p.getUpTime(),
                    100d * p.getResidentSetSize() / memory.getTotal(), FormatUtil.formatBytes(p.getVirtualSize()),
                    FormatUtil.formatBytes(p.getResidentSetSize()), p.getName()));
        }
    }

    private static void printSensors(Sensors sensors) {
        System.out.println("Sensors:");
        System.out.println(" cpu temperature:" + sensors.getCpuTemperature());
        System.out.println(" cpu voltage:" + sensors.getCpuVoltage());
        System.out.println(" cpu fan speed:" + Arrays.toString(sensors.getFanSpeeds()));
    }

    private static void printPowerSources(PowerSource[] powerSources) {
        System.out.println("Power Sources: ");
        if (powerSources.length == 0) {
            System.out.println("Unknown");
        }
        for (PowerSource powerSource : powerSources) {
            System.out.println(powerSource.toString());
        }
    }

    private static void printDisks(HWDiskStore[] diskStores) {
        System.out.println("Disks:");
        for (HWDiskStore disk : diskStores) {
            System.out.println(disk.getName());
            System.out.println(disk.getModel());
            System.out.println(disk.getSerial());
            System.out.println(" Transfer time: " + disk.getTransferTime() + " " +
                    "Queue length: " + disk.getCurrentQueueLength() + " " +
                    "Read Bytes: " + disk.getReadBytes() + " " +
                    "Reads: " + disk.getReads() + " " +
                    "Write Bytes: " + disk.getWriteBytes() + " " +
                    "Writes: " + disk.getWrites() + " " +
                    "");

            HWPartition[] partitions = disk.getPartitions();
            for (HWPartition part : partitions) {
                System.out.println(" |-- " + part.toString());
            }
        }

    }

    private static void printFileSystem(FileSystem fileSystem) {
        System.out.println("File System:");

        System.out.println(String.format(" File Descriptors: %d/%d", fileSystem.getOpenFileDescriptors(),
                fileSystem.getMaxFileDescriptors()));

        OSFileStore[] fsArray = fileSystem.getFileStores();
        for (OSFileStore fs : fsArray) {
            long usable = fs.getUsableSpace();
            long total = fs.getTotalSpace();
            System.out.println(String.format(
                    " %s (%s) [%s] %s of %s free (%.1f%%), %s of %s files free (%.1f%%) is %s "
                            + (fs.getLogicalVolume() != null && fs.getLogicalVolume().length() > 0 ? "[%s]" : "%s")
                            + " and is mounted at %s",
                    fs.getName(), fs.getDescription().isEmpty() ? "file system" : fs.getDescription(), fs.getType(),
                    FormatUtil.formatBytes(usable), FormatUtil.formatBytes(fs.getTotalSpace()), 100d * usable / total,
                    FormatUtil.formatValue(fs.getFreeInodes(), ""), FormatUtil.formatValue(fs.getTotalInodes(), ""),
                    100d * fs.getFreeInodes() / fs.getTotalInodes(), fs.getVolume(), fs.getLogicalVolume(),
                    fs.getMount()));
        }
    }

    private static void printNetworkInterfaces(NetworkIF[] networkIFs) {
        StringBuilder sb = new StringBuilder("Network Interfaces:");
        if (networkIFs.length == 0) {
            sb.append(" Unknown");
        }
        for (NetworkIF net : networkIFs) {
            sb.append("\n ").append(net.getName() + " Speed:" + net.getSpeed());
        }
        System.out.println(sb.toString());
    }

    private static void printNetworkParameters(NetworkParams networkParams) {
        System.out.println("Network parameters:\n " + networkParams.toString());
    }

    private static void printDisplays(Display[] displays) {
        System.out.println("Displays:");
        int i = 0;
        for (Display display : displays) {
            System.out.println(" Display " + i + ":");
            System.out.println(String.valueOf(display));
            i++;
        }
    }

    private static void printUsbDevices(UsbDevice[] usbDevices) {
        System.out.println("USB Devices:");
        for (UsbDevice usbDevice : usbDevices) {
            System.out.println(String.valueOf(usbDevice));
        }
    }

    private static void printSoundCards(SoundCard[] cards) {
        System.out.println("Sound Cards:");
        for (SoundCard card : cards) {
            System.out.println(" " + String.valueOf(card.getName()));
        }
    }


}
