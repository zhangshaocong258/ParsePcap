package Distributed;

import javax.swing.*;
import java.awt.*;

/**
 * Created by zsc on 2016/8/24.
 * @Deprecated
 */
public class ServerFrame extends JFrame{
    private final PcapPanel pcapPanel = new PcapPanel();
    public static void main(String[] args) {
        Server.getInstance().setPort("7777");
        new Thread(Server.getStartInstance()).start();
        new ServerFrame().init();
    }

    public void init(){
        JFrame.setDefaultLookAndFeelDecorated(true);

        this.setTitle("网络规律挖掘解析pcap");
        this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        this.setSize(new Dimension(600, 500));
        this.add(pcapPanel);
        this.setResizable(true);
        this.setVisible(true);
    }
}
