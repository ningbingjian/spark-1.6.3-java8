package org.apache.spark;

import org.apache.spark.network.server.TransportServer;
import org.junit.Test;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/13
 * Time: 20:25
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */

public class Junit01 {
    @Test
    public void testT1(){
        new TransportServer().testLog();
    }
}
