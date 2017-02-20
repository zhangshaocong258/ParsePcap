package Distributed;



import PcapStatisticsOpt.ParseByDay;

import java.awt.*;
import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;


/**
 * Created by zsc on 2016/5/20.
 */
public class Server {
    private static ServerStart serverStart;
    private int taskCount = 0;//发送次数
    private int totalCount = 0;
    private String port = "7777";
    private boolean isPcapRunning = false;//判断是否正在运行，解除挂起状态
    private ReadWriteLock isRunningLock = new ReentrantReadWriteLock();


    //PcapServer
    private PcapPanel pcapPanel;
    private TaskPanel taskPanel;//分布式任务进度条
    private ArrayList<String> allTasks = new ArrayList<String>();
    private ArrayList<String> allTasks2 = new ArrayList<String>();//第二步任务
    private ConcurrentHashMap<String, String> allTasksTags = new ConcurrentHashMap<String, String>();//带标签，所有不同类型任务
    private ConcurrentHashMap<String, String> allTasksTags2 = new ConcurrentHashMap<String, String>();//带标签，所有不同类型任务2
    private ConcurrentHashMap<String, String> nameMap = new ConcurrentHashMap<String, String>();//文件part
    private ConcurrentHashMap<String, String> nameMap2 = new ConcurrentHashMap<String, String>();//文件part
    private ConcurrentHashMap<String, String> combineFile = new ConcurrentHashMap<String, String>();//合并文件,key=最后合并生成的文件，valude=待合并的小文件的文件夹路径
    private ConcurrentHashMap<String, String> combineFile2 = new ConcurrentHashMap<String, String>();//合并文件,key=最后合并生成的文件，valude=待合并的小文件的文件夹路径
    private HashSet<String> delFile = new HashSet<String>();//删除文件
    private HashSet<String> delFile2 = new HashSet<String>();//删除文件
    private HashMap<String, StringBuilder> tasksMap = new HashMap<String, StringBuilder>();//0, 0-0\r\n0-1\r\n0-2
    private HashMap<String, String> swapMap = new HashMap<String, String>();//文件名\r\n,路由号
    private HashMap<String, String> swapMap2 = new HashMap<String, String>();//routesrc/1.1_1.2, 1.1_1.2

    private AtomicInteger threadNum = new AtomicInteger(0);//所有线程个数
    private String DELIMITER = "\r\n";
    private String inPath;
    private String outPath = "D:\\57data";
//    private String fileName;
    private int index;
    private int BUF_LEN = 5 * 1024 * 1024;
    private int pcapCount1 = 0;//发送次数
    private int pcapCount2 = 0;//发送次数
    private int recCount = 0;//接收到的个数
    private int recCount2 = 0;
    private int tasksCount = 0;
    private String date;
    private ParseByDay parseByDay;

    private HashMap<Long, ArrayList<File>> tasks = new HashMap<Long, ArrayList<File>>();//日期，filelist 得到对应时间的文件列表

    private Lock recLock = new ReentrantLock(true);//接收结果，改为公平锁
    private Lock sendLock = new ReentrantLock();
    private Lock recLock2 = new ReentrantLock(true);//接收结果和第一步要分开，否则出bug
    private Lock sendLock2 = new ReentrantLock();

    private Lock pcapLock = new ReentrantLock();
    private Condition pcapCon = pcapLock.newCondition();

    private ReadWriteLock isPcapRunningLock = new ReentrantReadWriteLock();

    private boolean combineAndDelete = false;//判断是否完成合并删除
    private boolean combineAndDelete2 = false;//判断是否完成合并删除
    private Lock comAndDel = new ReentrantLock();//合并删除操作加锁

    private Lock fileListLock = new ReentrantLock();
    private Condition fileListCon = fileListLock.newCondition();

    private static class Holder {
        private static Server server = new Server();
    }

    private Server() {}

    public static void closeServer() {
        if (serverStart != null) {
            serverStart.stop();
        }
    }

    public static Server getInstance() {
        return Holder.server;
    }

    public static ServerStart getStartInstance() {
        if (serverStart != null) {
            return serverStart;
        }
        serverStart = Server.getInstance().new ServerStart();
        return serverStart;
    }

    public void setPort(String port) {
        this.port = port;
    }


    /**
     * 分布式Pcap解析调用的方法
     *
     */

    public void setIsPcapRunning(boolean isRunning) {
        isPcapRunningLock.writeLock().lock();
        try {
            this.isPcapRunning = isRunning;
        } finally {
            isPcapRunningLock.writeLock().unlock();
        }
    }

    public void awakePcap() {
        pcapLock.lock();
        try {
            pcapCon.signalAll();
        } finally {
            pcapLock.unlock();
        }
    }

    public void initPcap(PcapPanel pcapPanel, String inPath, String outPath) {
        this.pcapPanel = pcapPanel;
        this.inPath = inPath;
        this.outPath = outPath;
        this.index = outPath.length() + 1;
    }

    public void initTask(TaskPanel taskPanel) {
        this.taskPanel = taskPanel;
    }

    public void initBar() {
        taskPanel.getBar().setValue(0);
        taskPanel.getBar().setString("读取数据中...");
        taskPanel.getBar().setMaximum(totalCount);
    }

    public int getThreadNum() {
        return threadNum.get();
    }

    //等待所有线程完成
    public void awaitList() {
        fileListLock.lock();
        try {
            fileListCon.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            fileListLock.unlock();
        }
    }

    //释放PcapPanel中的等待，开始下一个fileList的解析
    public void awakeList() {
        fileListLock.lock();
        try {
            fileListCon.signal();
        } finally {
            fileListLock.unlock();
        }
    }

    //获取tasks，进行循环
    public HashMap<Long, ArrayList<File>> getTasks() {
        return tasks;
    }

    //得到时间个数，即任务filelist个数
    public void getPcapTaskNum(String fpath, boolean parseAll) {
        System.out.println("parseAll:  " + parseAll);
        TreeMap<Long, ArrayList<File>> allTimes = new TreeMap<Long, ArrayList<File>>();
        genPcapTimeList(fpath, parseAll, allTimes);//得到时间序列
        getPcapTaskMap(allTimes);//得到map
    }

    //得到需要解析的文件的时间、外加对应文件map
    private void genPcapTimeList(String fpath, boolean parseAll, TreeMap<Long, ArrayList<File>> allTimes) {
        File ff = new File(fpath);
        if (parseAll) {
            if (ff.isFile()) {
                if (allTimes.containsKey(ff.lastModified())) {
                    allTimes.get(ff.lastModified()).add(ff);//若时间相同，则会覆盖！！！因此改为ArrayList<File>
                } else {
                    allTimes.put(ff.lastModified(), new ArrayList<File>());
                    allTimes.get(ff.lastModified()).add(ff);
                }                ff.setWritable(false);//设为已读
            } else if (ff.isDirectory()) {
                File[] files = ff.listFiles();
                for (File f : files) {
                    genPcapTimeList(f.getAbsolutePath(), parseAll, allTimes);
                }
            }
        } else {
            if (ff.isFile()) {
                if (ff.canWrite()) {//如果不可写，即只读
                    if (allTimes.containsKey(ff.lastModified())) {
                        allTimes.get(ff.lastModified()).add(ff);//若时间相同，则会覆盖！！！因此改为ArrayList<File>
                    } else {
                        allTimes.put(ff.lastModified(), new ArrayList<File>());
                        allTimes.get(ff.lastModified()).add(ff);
                    }                    ff.setWritable(false);//设为已读
                }
            } else if (ff.isDirectory()) {
                File[] files = ff.listFiles();
                for (File f : files) {
                    genPcapTimeList(f.getAbsolutePath(), parseAll, allTimes);
                }
            }
        }
    }

    //遍历时间，若超过一天，则put，在一天之内，则append，得到task MAP，个数代表所需解析文件list的个数，原来只有一个，现在可能有多个
    public void getPcapTaskMap(TreeMap<Long, ArrayList<File>> allTimes) {
        Long key = 0l;//实际时间
        Long switchKey = 0l;//key转换为当天0点，parsebyday也做了处理...
        for (Map.Entry<Long, ArrayList<File>> entry : allTimes.entrySet()) {
            //只在最初运行一次
            if (key == 0l) {
                key = entry.getKey();
                switchKey = key - (key + 8 * 3600000) % 86400000;//转换为当天0点，北京时间多8小时
                tasks.put(switchKey, new ArrayList<File>());
            }
            //进行判断，若else，则新添加
            if ((entry.getKey() - key) <= 86400000) {
                tasks.get(switchKey).addAll(entry.getValue());
            } else {
                key = entry.getKey();
                switchKey = key - (key + 8 * 3600000) % 86400000;
                tasks.put(switchKey, new ArrayList<File>());
                tasks.get(switchKey).addAll(entry.getValue());
                continue;
            }
        }

    }

    private String getDate(Long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date(time));
    }

    public void genTasks(Long time, ArrayList<File> files) {
        allTasks = new ArrayList<String>();
        allTasks2 = new ArrayList<String>();//第二步任务
        allTasksTags = new ConcurrentHashMap<String, String>();//带标签，所有不同类型任务
        allTasksTags2 = new ConcurrentHashMap<String, String>();//带标签，所有不同类型任务2
        nameMap = new ConcurrentHashMap<String, String>();//文件part
        nameMap2 = new ConcurrentHashMap<String, String>();//文件part
        combineFile = new ConcurrentHashMap<String, String>();//合并文件,key=最后合并生成的文件，value=待合并的小文件的文件夹路径
        combineFile2 = new ConcurrentHashMap<String, String>();//合并文件,key=最后合并生成的文件，value=待合并的小文件的文件夹路径
        delFile = new HashSet<String>();//删除文件
        delFile2 = new HashSet<String>();//删除文件
        tasksMap = new HashMap<String, StringBuilder>();
        swapMap = new HashMap<String, String>();//文件名\r\n,路由号
        swapMap2 = new HashMap<String, String>();//routesrc/1.1_1.2, 1.1_1.2
        pcapCount1 = 0;//发送次数
        pcapCount2 = 0;//发送次数
        recCount = 0;
        recCount2 = 0;
        combineAndDelete = false;
        combineAndDelete2 = false;
        date = getDate(time);//得到最小时间
        System.out.println("DATE: " + date);

        //按节点得到所有任务，只执行一次时使用
//        ArrayList<File> files = new ArrayList<File>();
//        getFileList(files, filePath, type);

        for (File file : files) {
            String key = new StringBuilder().append(file.getParent().
                    substring(file.getParent().lastIndexOf("\\") + 1)).toString();//得到文件parent E:\ppp\9中的9
            String name = new StringBuilder().append(file.getAbsolutePath().
                    substring(file.getParent().lastIndexOf("\\") + 1)).toString();//得到9\9-1.pcap .etc
//            System.out.println("key: " + key);
//            System.out.println("name:  " + name);
            if (tasksMap.containsKey(key)) {
                tasksMap.get(key).append(name).append(DELIMITER);
            } else {
                tasksMap.put(key, new StringBuilder(name).append(DELIMITER));
            }
        }
        for (Map.Entry<String, StringBuilder> entry : tasksMap.entrySet()) {
            allTasks.add(entry.getValue().toString());
            allTasksTags.put(entry.getValue().toString(), "n");
            swapMap.put(entry.getValue().toString(), entry.getKey());
        }
        tasksCount = allTasks.size();

        //删除outpath下的routsrc，作用是解析下一天的文件，需要清空routesrc
        File folder = new File(outPath + File.separator + "routesrc");
        if (folder.exists() && folder.isDirectory()) {
            deleteFile(folder.getAbsolutePath());
        }

    }

    public void deleteFile(String fileName) {
        File file = new File(fileName);
        if (file.exists()) {
            if (file.isFile()){
                file.delete();
            } else if (file.isDirectory()) {
                File[] files = file.listFiles();
                for (int i = 0; i < files.length; i++) {
                    //删除子文件
                    deleteFile(files[i].getAbsolutePath());
                }
                file.delete();
            }
        } else {
            System.out.println("文件不存在");
        }
    }

    private int getFileList(ArrayList<File> file, String filePath, String type) {
        int num = 0;
        File ff = new File(filePath);
        if (ff.isFile() && filePath.endsWith(type)) {
            file.add(ff);
            num += 1;
        } else if (ff.isDirectory()) {
            File[] subFiles = ff.listFiles();
            for (File f : subFiles) {
                getFileList(file, f.getAbsolutePath(), type);
            }
        }
        return num;
    }

    //启动服务端
    class ServerStart implements Runnable {
        private ServerSocket serverSocket = null;
        private UserClient dataClient;
        private UserClientObject resultClient;
        private UserClient pcapClient;
        private boolean start = false;

        public void run() {
            try {
                serverSocket = new ServerSocket(Integer.valueOf(port));
                start = true;
            } catch (BindException e) {
                System.out.println("端口使用中...");
                System.exit(0);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                while (start) {
                    Socket dataSocket = serverSocket.accept();//接收dataoutputstream
                    Socket resultSocket = serverSocket.accept();//接收objectoutputstream
                    Socket pcapSocket = serverSocket.accept();//pcap_dataoutputstream
                    dataClient = new UserClient(dataSocket);
                    resultClient = new UserClientObject(resultSocket);
                    pcapClient = new UserClient(pcapSocket);
//                    ReceiveResult receiveResult = new ReceiveResult(resultClient);//连接
                    ParsePcap parsePcap = new ParsePcap(pcapClient);
                    threadNum.getAndIncrement();//统计线程个数
                    System.out.println("一个客户端已连接！");
                    isRunningLock.readLock().lock();
                    //pcap是否运行
                    isPcapRunningLock.readLock().lock();
                    try {
                        if (isPcapRunning) {
                            parsePcap.setPcapSuspend(false);
                        }
                    } finally {
                        isPcapRunningLock.readLock().unlock();
                    }
                    new Thread(parsePcap).start();//启动线程
                }
            } catch (IOException e) {
                System.out.println("服务端错误位置");
//                e.printStackTrace();
            } finally {
                try {
                    serverSocket.close();
                    start = false;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void stop() {
            try {
                if (serverSocket != null) {
                    serverSocket.close();
                }
                if (dataClient != null) {
                    dataClient.close();
                }
                if (resultClient != null) {
                    resultClient.close();
                }
                if (pcapClient != null) {
                    pcapClient.close();
                }
                start = false;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Pcap解析线程
     *
     * 1.要考虑到first与last这两步之间的过渡，即first未执行完，其他线程已经开始last，first执行完毕后如何跳转
     * 2.考虑first和last2个发送线程的原子性（直接更改为原子操作）
     * 3.考虑接收线程的原子性（直接更改为原子操作会出现同时传送同一个文件的bug，解决方法为在文件名后面添加线程名）
     * 4.考虑最后合并删除操作的唯一性
     * 5.考虑多个fileList循环，接收线程的完成解锁情况
     *
     */
    class ParsePcap implements Runnable {
        private boolean firstConnected = false;
        private boolean lastConnected = false;
        private UserClient userClient;
        private String dataFromClient = "";
        private long totalLen;
        private String finalFolderPath;
        private String task;
        private String task2;
        private String status;
        private boolean isEmpty = false;
        private boolean isEmpty2 = false;
        private boolean isPcapSuspend = true;
        private boolean isConnected = true;


        ParsePcap(UserClient userClient) {
            this.userClient = userClient;
            firstConnected = true;
        }

        public void setPcapSuspend(boolean suspend) {
            isPcapSuspend = suspend;
        }

        @Override
        public void run() {
            try {
                while (isConnected) {
                    if (isPcapSuspend) {
                        pcapLock.lock();
                        try {
                            System.out.println("pcap阻塞");
                            pcapCon.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } finally {
                            pcapLock.unlock();
                        }
                    }
                    pcapPanel.getBar().setValue(recCount);
                    pcapPanel.getBar().setMaximum(allTasks.size());//进度条最大
                    pcapPanel.getjLabel().setText("阶段 1/3");
                    long a = System.currentTimeMillis();
                    while (firstConnected) {
                        dataFromClient = userClient.receiveMsg();
                        System.out.println("First接收到ready");
                        userClient.sendMsg("First");
                        if (dataFromClient.equals("Ready")) {
//                            pcapCount1 < 或 =两种情况，只发送一次
                            sendLock.lock();//将全部过程锁起
                            try {
//                            int tempNum;//作为发送任务的临时变量
                                if (pcapCount1 < allTasks.size()) {
//                                pcapCount1 += 1;
//                                sendLock.unlock();
                                    userClient.sendTask(allTasks.get(pcapCount1));
                                    System.out.println("第" + pcapCount1 + "次已发送" + allTasks.size());
                                    pcapCount1 += 1;
                                    System.out.println("下一次发送：" + (pcapCount1 + 1));
                                } else {
//                                sendLock.unlock();

//                                updateMapLock1.lock();
//                                try {
                                    int temp = 0;//中途最后一个结果发回来，发送Empty，避免客户端发送ready后接收不到任务造成死锁

                                    //找到没有完成的任务，此时会put或remove，会和updateMap冲突
                                    for (Map.Entry<String, String> entry : allTasksTags.entrySet()) {
                                        temp += 1;
                                        System.out.println("遍历TaskCombination= " + entry.getKey() +
                                                " and String = " + entry.getValue());
                                        if (entry.getValue().equals("n")) {
                                            userClient.sendTask(entry.getKey());
                                            System.out.println("第二次发送的task：" + entry.getKey());
                                            break;
                                        }
                                        if (temp == allTasksTags.size()) {
                                            userClient.sendMsg("Empty");//全部结果已返回，客户端重新待命
                                            System.out.println("发送Empty");
                                            isEmpty = true;
                                        }
                                    }
//                                } finally {
//                                    updateMapLock1.unlock();
//                                }
                                }
                            } finally {
                                sendLock.unlock();
                            }
                        }

                        //接收结果
                        recLock.lock();
                        try {
                            if (!isEmpty) {
                                //判断是否返回已存在结果，若不存在，则接收
                                task = userClient.receiveMsg();
                                if (allTasksTags.get(task).equals("y")) {
                                    userClient.sendMsg(status = "Existent");
                                } else {
                                    userClient.sendMsg(status = "Absent");
                                }

                                if (status.equals("Absent")) {
                                    status = null;
                                    if (recCount < tasksCount) {
                                        finalFolderPath = outPath;
                                        //接收文件
                                        receiveResult(finalFolderPath);
                                        updateMap(task);
                                        recCount += 1;
                                        pcapPanel.getBar().setValue(recCount);
                                        pcapPanel.getjLabel().setText("阶段 1/3");
                                        if (recCount == tasksCount) {
//                                    userClient.close();
                                            System.out.println("运行结束");
                                            comAndDel.lock();
                                            try {
                                                if (!combineAndDelete) {
                                                    combineFiles(combineFile);
                                                    System.out.println("文件已合并");
                                                    deleteFile(delFile);
                                                    System.out.println("文件已删除");
                                                    combineAndDelete = true;
                                                    firstConnected = false;
                                                    lastConnected = true;
                                                } else {
                                                    firstConnected = false;
                                                    lastConnected = true;
                                                    System.out.println("运行结束2");
                                                }
                                            } finally {
                                                comAndDel.unlock();
                                            }
//                                        recCon.await();//释放cLock，但recLock无法释放!!!
                                        }
                                    }
                                } else if (status.equals("Existent")) {
                                    status = null;
                                    if (recCount < tasksCount) {
                                        continue;
                                    } else if (recCount == tasksCount) {
                                        comAndDel.lock();
                                        try {
                                            if (!combineAndDelete) {
                                                combineFiles(combineFile);
                                                System.out.println("文件已合并");
                                                deleteFile(delFile);
                                                System.out.println("文件已删除");
                                                combineAndDelete = true;
                                                firstConnected = false;
                                                lastConnected = true;
                                                System.out.println("运行结束2");
                                            } else {
                                                firstConnected = false;
                                                lastConnected = true;
                                                System.out.println("运行结束2");
                                            }
                                        } finally {
                                            comAndDel.unlock();
                                        }

//                                    recCon.await();
                                    }
                                }
                            } else {
                                comAndDel.lock();
                                try {
                                    if (!combineAndDelete) {
                                        combineFiles(combineFile);
                                        System.out.println("文件已合并");
                                        deleteFile(delFile);
                                        System.out.println("文件已删除");
                                        combineAndDelete = true;
                                        firstConnected = false;
                                        lastConnected = true;
                                        System.out.println("运行结束3");
                                    } else {
                                        firstConnected = false;
                                        lastConnected = true;
                                        System.out.println("运行结束3");
                                    }
                                } finally {
                                    comAndDel.unlock();
                                }
//                            recCon.await();
                            }
                        } finally {
                            recLock.unlock();
                        }
                    }

                    tasksCount = allTasks2.size();
                    pcapPanel.getBar().setValue(recCount2);
                    pcapPanel.getBar().setMaximum(tasksCount);//进度条最大
                    pcapPanel.getjLabel().setText("阶段 2/3");
                    //执行后2步
                    while (lastConnected) {
                        dataFromClient = userClient.receiveMsg();
                        System.out.println("last接收到ready");
                        userClient.sendMsg("Last");
                        if (dataFromClient.equals("Ready")) {
//                            pcapCount1 < 或 =两种情况，只发送一次
                            sendLock2.lock();
                            try {
//                            int tempNum;//作为发送任务的临时变量
                                if (pcapCount2 < allTasks2.size()) {
                                    userClient.sendTask(allTasks2.get(pcapCount2));
                                    System.out.println("第" + pcapCount2 + "次已发送" + allTasks2.size());
                                    sendFileTask(allTasks2.get(pcapCount2).split(DELIMITER)[0]);//发送单个文件,routesrc/10.0.0.1_10.0.0.2.bin
                                    pcapCount2 += 1;
                                    System.out.println("下一次发送：" + (pcapCount2 + 1));
                                } else {

//                                updateMapLock2.lock();
//                                try {
                                    int temp = 0;//中途最后一个结果发回来，发送Empty，避免客户端发送ready后接收不到任务造成死锁

                                    //找到没有完成的任务
                                    for (Map.Entry<String, String> entry : allTasksTags2.entrySet()) {
                                        temp += 1;
                                        System.out.println("遍历TaskCombination= " + entry.getKey() +
                                                " and String = " + entry.getValue());
                                        if (entry.getValue().equals("n")) {
                                            userClient.sendTask(entry.getKey());
                                            sendFileTask(entry.getKey().split(DELIMITER)[0]);
                                            System.out.println("第二次发送的task：" + entry.getKey());
                                            break;
                                        }
                                        if (temp == allTasksTags2.size()) {
                                            userClient.sendMsg("Empty");//全部结果已返回，客户端重新待命
                                            System.out.println("发送Empty");
                                            isEmpty2 = true;
                                        }
                                    }
//                                } finally {
//                                    updateMapLock2.unlock();
//                                }
                                }
                            } finally {
                                sendLock2.unlock();
                            }
                        }

                        //接收结果
                        recLock2.lock();
                        try {
                            if (!isEmpty2) {
                                //判断是否返回已存在结果
                                task2 = userClient.receiveMsg();
                                System.out.println("alltaskstags2: " + allTasksTags2.size());
                                if (allTasksTags2.get(task2).equals("y")) {
                                    userClient.sendMsg(status = "Existent");
                                    System.out.println("发送existent");
                                } else {
                                    userClient.sendMsg(status = "Absent");
                                    System.out.println("发送absent");
                                }

                                if (status.equals("Absent")) {
                                    status = null;
                                    if (recCount2 < tasksCount) {
                                        finalFolderPath = outPath;
                                        //接收文件
                                        receiveResult2(finalFolderPath);
                                        updateMap2(task2);
                                        recCount2 += 1;
                                        pcapPanel.getBar().setValue(recCount2);
                                        pcapPanel.getjLabel().setText("阶段 2/3");
                                        if (recCount2 == tasksCount) {
                                            comAndDel.lock();
                                            try {
                                                if (!combineAndDelete2) {
                                                    combineFiles2(combineFile2);
                                                    System.out.println("文件已合并2.1");
                                                    deleteFile(delFile2);
                                                    System.out.println("文件已删除2.1");
                                                    combineAndDelete2 = true;
                                                    lastConnected = false;
                                                    firstConnected = true;
                                                    isEmpty = isEmpty2 = false;
                                                    isPcapSuspend = true;
                                                    setIsPcapRunning(false);
                                                    parseByDay();
                                                } else {
                                                    lastConnected = false;
                                                    firstConnected = true;
                                                    isEmpty = isEmpty2 = false;
                                                    isPcapSuspend = true;
                                                    setIsPcapRunning(false);
                                                    System.out.println("运行结束2.1else");
                                                }
                                                System.out.println("运行结束2.1");

                                            } finally {
                                                comAndDel.unlock();
                                            }
                                            long b = System.currentTimeMillis();
                                            System.out.println("time.... " + (b - a));
                                        }
                                    }
                                } else if (status.equals("Existent")) {
                                    status = null;
                                    if (recCount2 < tasksCount) {
                                        continue;
                                    } else if (recCount2 == tasksCount) {
                                        comAndDel.lock();
                                        try {
                                            if (!combineAndDelete2) {
                                                combineFiles2(combineFile2);
                                                System.out.println("文件已合并2.2");
                                                deleteFile(delFile2);
                                                System.out.println("文件已删除2.2");
                                                combineAndDelete2 = true;
                                                lastConnected = false;
                                                firstConnected = true;
                                                isEmpty = isEmpty2 = false;
                                                isPcapSuspend = true;
                                                setIsPcapRunning(false);
                                                parseByDay();
                                            } else {
                                                lastConnected = false;
                                                firstConnected = true;
                                                isEmpty = isEmpty2 = false;
                                                isPcapSuspend = true;
                                                setIsPcapRunning(false);
                                                System.out.println("运行结束2.2else");
                                            }
                                            System.out.println("运行结束2.2");
                                            long b = System.currentTimeMillis();
                                            System.out.println("time.... " + (b - a));
                                        } finally {
                                            comAndDel.unlock();
                                        }

                                    }
                                }
                            } else {
                                comAndDel.lock();
                                try {
                                    if (!combineAndDelete2) {
                                        combineFiles2(combineFile2);
                                        System.out.println("文件已合并2.3");
                                        deleteFile(delFile2);
                                        System.out.println("文件已删除2.3");
                                        combineAndDelete2 = true;
                                        lastConnected = false;
                                        firstConnected = true;
                                        isEmpty = isEmpty2 = false;
                                        isPcapSuspend = true;
                                        setIsPcapRunning(false);
                                        parseByDay();
                                    } else {
                                        lastConnected = false;
                                        firstConnected = true;
                                        isEmpty = isEmpty2 = false;
                                        isPcapSuspend = true;
                                        setIsPcapRunning(false);
                                        System.out.println("运行结束2.3else");
                                    }
                                    System.out.println("运行结束2.3");
                                    long b = System.currentTimeMillis();
                                    System.out.println("time.... " + (b - a));
                                } finally {
                                    comAndDel.unlock();
                                }
                            }
                        } finally {
                            recLock2.unlock();
                        }
                    }
                    System.out.println("本次执行完毕，进行到第" + recCount2 + "  Thread: " + Thread.currentThread().getName());
                    if (recCount2 >= taskCount) {
                        if (threadNum.decrementAndGet() == 0) {
                            System.out.println("threadNum: " + threadNum.get());
                            awakeList();//唤起等待，开始下一次任务
                        }
                    }

                }
            } catch (IOException e) {
                System.out.println("发送文件报错");
                e.printStackTrace();
            } finally {
                System.out.println("进入pf");
//                firstConnected = false;
//                lastConnected = false;
//                isPcapSuspend = true;
//                setIsPcapRunning(false);//只有在完成任务后才设为false,与task保持同步
                threadNum.getAndDecrement();//报错则减掉当前线程
                isConnected = false;
                try {
                    userClient.close();
                    System.out.println("关闭流");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        private void parseByDay() {
            System.out.println("执行parsebyDay");
//            getModifiedTime(inPath, "pcap");
            pcapPanel.getBar().setValue(0);
            pcapPanel.getBar().setMaximum(3);
            pcapPanel.getjLabel().setText("阶段 3/3");
            parseByDay = new ParseByDay(outPath, outPath, date);
            parseByDay.initDataByDay();
            parseByDay.parseNode();
            pcapPanel.getBar().setValue(1);
            parseByDay.parseRoute();
            pcapPanel.getBar().setValue(2);
            parseByDay.parseTraffic();
            pcapPanel.getBar().setValue(3);
            pcapPanel.getjLabel().setText("全部完成");
            pcapPanel.getBeginDig().setEnabled(true);
        }


        private void sendFileTask(String task) throws IOException{
            String finalPath = outPath + File.separator + task;
            String subFolder = task.substring(0, task.indexOf("\\"));
            File file = new File(finalPath);
            sendFolder(subFolder);//发送routesrc文件夹
            sendFile(file);
            userClient.sendMsg("endTransmit");//结束任务
        }

        private void sendFile(File file) {
            byte[] sendBuffer = new byte[BUF_LEN];
            int length;
            try {
                userClient.sendMsg("sendFile");
                userClient.sendMsg(file.getName());
                System.out.println("fileName: " + file.getName());

                DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
                length = dis.read(sendBuffer, 0, sendBuffer.length);
                while (length > 0) {
                    userClient.sendInt(length);
                    userClient.sendByte(sendBuffer, 0, length);
                    length = dis.read(sendBuffer, 0, sendBuffer.length);
                }
                userClient.sendInt(length);
                dis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void sendFolder(String subFolder) {
            try {
                userClient.sendMsg("sendFolder");
                userClient.sendMsg(subFolder);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //traffic排序
        private void sortTraffic(String path, String outPath) {
            try {
                InputStreamReader in = new InputStreamReader(new FileInputStream(path), "UTF-8");
                BufferedReader bin = new BufferedReader(in);
                String curLine;
                ArrayList<TrafficKey> keys = new ArrayList<TrafficKey>();

                while ((curLine = bin.readLine()) != null) {
                    String str[] = curLine.split(",");
                    TrafficKey key = new TrafficKey();
                    key.setTime(Long.valueOf(str[0]));
                    key.setSrcIp(str[1]);
                    key.setDstIp(str[2]);
                    key.setProtocol(str[3]);
                    keys.add(key);
                }
                bin.close();
                Collections.sort(keys);

                OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(outPath), "UTF-8");
                BufferedWriter bout = new BufferedWriter(out);

                for (TrafficKey key : keys) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(key.getTime()).append(",").append(key.getSrcIp()).append(",").
                            append(key.getDstIp()).append(",").append(key.getProtocol());
                    bout.write(sb.toString());
                    bout.newLine();
                }
                bout.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void combineFiles2(ConcurrentHashMap<String, String> combineFile) throws IOException {
            for (Map.Entry<String, String> entry : combineFile.entrySet()) {
                ArrayList<File> fileList = new ArrayList<File>();
                getFilePath(fileList, entry.getValue(), "txt");//得到待删除文件

                File key = new File(entry.getKey());//D:/57data/traffic/10.0.0.1.txt
                String name = key.getName();//10.0.0.1.txt
                File comFile = new File(entry.getValue() + File.separator + name);//合并后暂存为D:/57data/traffic/10.0.0.1/10.0.0.1.txt,未排序，从0-1942 0-1942 0-1942
                if (!comFile.exists()) {
                    comFile.createNewFile();
                }
                FileChannel outChannel = new FileOutputStream(comFile).getChannel();
                FileChannel inChannel;
                for (File file : fileList) {
                    inChannel = new FileInputStream(file).getChannel();
                    inChannel.transferTo(0, inChannel.size(), outChannel);
                    inChannel.close();
                }
                outChannel.close();
                sortTraffic(comFile.getAbsolutePath(), entry.getKey());//最后生成排序后的txt
            }
        }

        private void combineFiles(ConcurrentHashMap<String, String> combineFile) throws IOException {
//            System.out.println("combineFiles000: " + combineFile.size());
            for (Map.Entry<String, String> entry : combineFile.entrySet()) {
                ArrayList<File> fileList = new ArrayList<File>();
                getFilePath(fileList, entry.getValue(), "bin");//得到待删除文件
//                System.out.println("combineFiles:  " + fileList.size());

                File outputFile = new File(entry.getKey());
                if (!outputFile.exists()) {
                    outputFile.createNewFile();
                }
                FileChannel outChannel = new FileOutputStream(outputFile).getChannel();
                FileChannel inChannel;
                for (File file : fileList) {
                    inChannel = new FileInputStream(file).getChannel();
                    inChannel.transferTo(0, inChannel.size(), outChannel);
                    inChannel.close();
                }
                outChannel.close();
            }
        }

        private int getFilePath(ArrayList<File> fileList, String filePath, String type) {
            int num = 0;
            File ff = new File(filePath);
            if (ff.isFile() && filePath.endsWith(type)) {
                fileList.add(ff);
                num += 1;
            } else if (ff.isDirectory()) {
                File[] files = ff.listFiles();
                for (File f : files) {
                    getFilePath(fileList, f.getAbsolutePath(), type);
                }
            }
            return num;
        }

        private void deleteFile(HashSet<String> fileNameList) {
            for (String fileName : fileNameList) {
                File file = new File(fileName);
                if (file.isDirectory()) {
                    deleteFile(fileName);
                } else {
                    System.out.println("不是文件夹");
                }
            }
        }

        public void deleteFile(String fileName) {
            File file = new File(fileName);
            if (file.exists()) {
                if (file.isFile()){
//                    System.out.println("delfilename:   " + file.getName());
                    file.delete();
                } else if (file.isDirectory()) {
                    File[] files = file.listFiles();
                    for (int i = 0; i < files.length; i++) {
                        //删除子文件
                        deleteFile(files[i].getAbsolutePath());
                    }
//                    System.out.println("delfileDirename: " + file.getName());
                    file.delete();
                }
            } else {
                System.out.println("文件不存在");
            }
        }

        public String getExtension(String fileName) {
            return fileName.substring(fileName.lastIndexOf("."));
        }

        public String getName(String fileName) {
            return fileName.substring(0, fileName.lastIndexOf("."));
        }

        public void genPart(String fileName, String type) {
            if (!type.equals(".bin")) {
                return;
            }
            if (!nameMap.containsKey(fileName)) {
                nameMap.put(fileName, swapMap.get(task));
            } else {
                nameMap.remove(fileName);
                nameMap.put(fileName, swapMap.get(task));
            }
        }

        public void genPart2(String fileName, String type) {
            if (!type.equals(".txt")) {
                return;
            }
            if (!nameMap2.containsKey(fileName)) {
                nameMap2.put(fileName, swapMap2.get(task2));
            } else {
                nameMap2.remove(fileName);
                nameMap2.put(fileName, swapMap2.get(task2));
            }
        }

        public void receiveResult(String finalFolderPath) throws IOException {
            totalLen = userClient.receiveLong();
            System.out.println("totalLen: " + totalLen);
            long beginTime = System.currentTimeMillis();
            String subFolder;
            while (true) {
                String receiveType = userClient.receiveMsg();
                if (receiveType.equals("sendFile")) {
                    receiveFile(finalFolderPath);//仅文件
                } else if (receiveType.equals("sendFolder")) {
                    subFolder = userClient.receiveMsg();//发送方的selectFolderPath子目录：routesrc或node
                    finalFolderPath = outPath + File.separator + subFolder;
                    //生成子目录
                    File folder = new File(finalFolderPath);
                    boolean suc = (folder.exists() && folder.isDirectory()) ? true : folder.mkdirs();
                } else if (receiveType.equals("endTransmit")) {
                    break;
                }
            }
        }

        private void receiveFile(String outPath) {
            byte[] receiveBuffer = new byte[BUF_LEN];
            int length;
            long passedlen = 0;
            String fileName;
            String name;
            String tempName;
            String extension;
            String finalFilePath;
            String filePath;
            String folderPath;
            String subFolder;
            String task2;

            try {
                fileName = userClient.receiveMsg();//得到 文件名.扩展名 10.0.1.1_10.0.1.2.bin
                name = getName(fileName);//得到文件名10.0.1.1_10.0.1.2
                tempName = name + "_temp";
                extension = getExtension(fileName);//得到扩展名bin
                genPart(fileName, extension);//得到路由器号作为part
                //创建文件夹/routesrc/10.0.1.1_10.0.1.2/...
                //生成合并文件map、删除文件list
                if (extension.equals(".bin")) {
                    File folder = new File(outPath + File.separator + tempName);
                    boolean suc = (folder.exists() && folder.isDirectory()) ? true : folder.mkdirs();
                    finalFilePath = outPath + File.separator + tempName + File.separator + name + "_part_" + nameMap.get(fileName) + extension;
//                    System.out.println("part: " + nameMap.get(fileName));
                    filePath = outPath + File.separator + fileName;//D:/57data/routesrc/10.0.0.1_10.0.0.2.bin,用于合并文件
                    folderPath = outPath + File.separator + tempName;//D:/57data/routesrc/10.0.0.1_10.0.0.2_temp/文件夹后面加temp
                    //生成第二步的task
                    subFolder = outPath.substring(index) + File.separator + fileName;//routesrc/10.0.0.1_10.0.0.2.bin
                    task2 = subFolder + DELIMITER + name;

                    if (!combineFile.containsKey(filePath)) {
                        combineFile.put(filePath, folderPath);
                        delFile.add(folderPath);//待删除的10.0.0.1_10.0.0.2文件夹里面的所有文件
//                        System.out.println("待删除的routesrc： " + folderPath);
                        //生成第二步任务list
                        allTasks2.add(task2);
                        allTasksTags2.put(task2, "n");
                        swapMap2.put(task2, name);//routesrc/10.0.0.1_10.0.0.2.bin, 10.0.0.1_10.0.0.2;用于生成part
                    }
                } else {
                    finalFilePath = outPath + File.separator + fileName;//接收node文件
                }

                //接收文件
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(finalFilePath)));
                length = userClient.receiveInt();
                while (length > 0) {
                    userClient.receiveFullByte(receiveBuffer, 0, length);//read到length才返回，若用read，可能不到length就返回
                    dos.write(receiveBuffer, 0, length);
                    dos.flush();
                    length = userClient.receiveInt();
                }
                System.out.println("接收方结束循环");
                dos.close();
            } catch (IOException e) {
                System.out.println("接收文件报错");
                e.printStackTrace();
            }

        }

        public void receiveResult2(String finalFolderPath) throws IOException {
            totalLen = userClient.receiveLong();
            System.out.println("totalLen: " + totalLen);
            long beginTime = System.currentTimeMillis();
            String subFolder;
            while (true) {
                String receiveType = userClient.receiveMsg();
                if (receiveType.equals("sendFile")) {
                    receiveFile2(finalFolderPath);//仅文件
                } else if (receiveType.equals("sendFolder")) {
                    subFolder = userClient.receiveMsg();//发送方的selectFolderPath子目录：route或traffic
                    finalFolderPath = outPath + File.separator + subFolder;
                    //生成子目录
                    File folder = new File(finalFolderPath);
                    boolean suc = (folder.exists() && folder.isDirectory()) ? true : folder.mkdirs();
                } else if (receiveType.equals("endTransmit")) {
                    break;
                }
            }
        }

        private void receiveFile2(String outPath) {
            byte[] receiveBuffer = new byte[BUF_LEN];
            int length;
            long passedlen = 0;
            String fileName;
            String name;
            String tempName;
            String extension;
            String finalFilePath;
            String filePath;
            String folderPath;

            try {
                fileName = userClient.receiveMsg();
//                finalFilePath = outPath + File.separator + fileName;
                name = getName(fileName);//得到文件名
                tempName = name + "_temp";
                extension = getExtension(fileName);//得到扩展名
                genPart2(fileName, extension);//得到part


                if (extension.equals(".txt")) {
                    File folder = new File(outPath + File.separator + tempName);
                    boolean suc = (folder.exists() && folder.isDirectory()) ? true : folder.mkdirs();
                    finalFilePath = outPath + File.separator + tempName + File.separator + name + "_part_" + nameMap2.get(fileName) + extension;
//                    System.out.println("part: " + nameMap.get(fileName));
                    filePath = outPath + File.separator + fileName;//D:/57data/traffic/10.0.0.1.txt
                    folderPath = outPath + File.separator + tempName;//D:/57data/traffic/10.0.0.1/
                    if (!combineFile2.containsKey(filePath)) {
                        combineFile2.put(filePath, folderPath);
                        delFile2.add(folderPath);//待删除的10.0.0.1-10.0.0.2文件夹们
                    }
                } else {
                    finalFilePath = outPath + File.separator + fileName;
                }

                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(finalFilePath)));
                length = userClient.receiveInt();
                while (length > 0) {
                    userClient.receiveFullByte(receiveBuffer, 0, length);//read到length才返回，若用read，可能不到length就返回
                    dos.write(receiveBuffer, 0, length);
                    dos.flush();
                    length = userClient.receiveInt();
                }
                System.out.println("接收方结束循环");
                dos.close();
            } catch (IOException e) {
                System.out.println("接收文件报错");
                e.printStackTrace();
            }
        }

        private void updateMap(String task) {
//            updateMapLock1.lock();
//            try {
                if (allTasksTags.get(task).equals("n")) {
//                    allTasksTags.remove(task);//可注释
                    allTasksTags.put(task, "y");//更新标记，表示完成
                }
//            } finally {
//                updateMapLock1.unlock();
//            }
        }

        private void updateMap2(String task2) {
//            updateMapLock2.lock();
//            try {
                if (allTasksTags2.get(task2).equals("n")) {
//                    allTasksTags2.remove(task2);//可注释
                    allTasksTags2.put(task2, "y");//更新标记，表示完成
                }
//            } finally {
//                updateMapLock2.unlock();
//            }
        }
    }

}

class UserClient {
    private Socket socket = null;
    private DataInputStream disWithClient;
    private DataOutputStream dosWithClient;

    public UserClient(Socket socket) {
        this.socket = socket;
        try {
            disWithClient = new DataInputStream(socket.getInputStream());
            dosWithClient = new DataOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        try {
            if (disWithClient != null) disWithClient.close();
            if (socket != null) socket.close();
            if (dosWithClient != null) dosWithClient.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }


    public String receiveReady() throws IOException {
        return disWithClient.readUTF();
    }

    //PcapServer
    public int receiveInt() throws IOException {
        return disWithClient.readInt();
    }

    public long receiveLong() throws IOException {
        return disWithClient.readLong();
    }

    public String receiveMsg() throws IOException {
        return disWithClient.readUTF();
    }

    public void receiveFullByte(byte[] bytes, int off, int len) throws IOException {
        disWithClient.readFully(bytes, off, len);
    }

    //发送文件
    public void sendInt(int len) throws IOException {
        dosWithClient.writeInt(len);
    }

    //发送文件长度
    public void sendLong(long len) throws IOException {
        dosWithClient.writeLong(len);
        dosWithClient.flush();
    }

    public void sendByte(byte[] bytes, int off, int len) throws IOException {
        dosWithClient.write(bytes, off, len);
        dosWithClient.flush();
    }

    public void sendTask(String task) throws IOException {
        dosWithClient.writeUTF(task);
        dosWithClient.flush();
    }

    public void sendMsg(String str) throws IOException {
        dosWithClient.writeUTF(str);
        dosWithClient.flush();
    }
}

class UserClientObject {
    private Socket socket = null;
    private ObjectInputStream disWithClient;
    private ObjectOutputStream dosWithClient;

    public UserClientObject(Socket socket) {
        this.socket = socket;
        try {
            dosWithClient = new ObjectOutputStream(socket.getOutputStream());
            disWithClient = new ObjectInputStream(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        try {
            if (disWithClient != null) disWithClient.close();
            if (socket != null) socket.close();
            if (dosWithClient != null) dosWithClient.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

}

