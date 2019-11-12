package core.learn;

import core.learn.field.*;
import core.learn.module.*;
import org.apache.spark.api.java.JavaPairRDD;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldLearn {
    public void execute(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        HotkeyFinder h;
        List<Module> indicesModule = new ArrayList<>();
        // Indices :
        AmsModule amsModule = new AmsModule();
        ArpModule arpModule = new ArpModule();
        BacnetModule bacnetModule = new BacnetModule();
        CipModule cipModule = new CipModule();
        CoapModule coapModule = new CoapModule();
        Dnp3Module dnp3Module = new Dnp3Module();
        DnsModule dnsModule = new DnsModule();
        DsiModule dsiModule = new DsiModule();
        EgdModule egdModule = new EgdModule();
        Eplv1Module eplv1Module = new Eplv1Module();
        EsioModule esioModule = new EsioModule();
        EsModule esModule = new EsModule();
        EthercatModule ethercatModule = new EthercatModule();
        EthernetipModule ethernetipModule = new EthernetipModule();
        FfhseModule ffhseModule = new FfhseModule();
        FoxModule foxModule = new FoxModule();
        FtpModule ftpModule = new FtpModule();
        GooseModule gooseModule = new GooseModule();
        GryphonModule gryphonModule = new GryphonModule();
        HartipModule hartipModule = new HartipModule();
        HttpsModule httpsModule = new HttpsModule();
        Iec104Module iec104Module = new Iec104Module();
        ImapModule imapModule = new ImapModule();
        InfluxModule influxModule = new InfluxModule();
        IrcModule ircModule = new IrcModule();
        LldpModule lldpModule = new LldpModule();
        LlmnrModule llmnrModule = new LlmnrModule();
        LontalkModule lontalkModule = new LontalkModule();
        MdnsModule mdnsModule = new MdnsModule();
        MmsModule mmsModule = new MmsModule();
        ModbusModule modbusModule = new ModbusModule();
        MysqlModule mysqlModule = new MysqlModule();
        OicqModule oicqModule = new OicqModule();
        OmronfinsModule omronfinsModule = new OmronfinsModule();
        OpcModule opcModule = new OpcModule();
        OpcuaModule opcuaModule = new OpcuaModule();
        OpensafetyModule opensafetyModule = new OpensafetyModule();
        PgsqlModule pgsqlModule = new PgsqlModule();
        PowerlinkModule powerlinkModule = new PowerlinkModule();
        S7plusModule s7plusModule = new S7plusModule();
        SercosiiiModule sercosiiiModule = new SercosiiiModule();
        Sf_haspModule sf_haspModule = new Sf_haspModule();
        SsdpModule ssdpModule = new SsdpModule();

        indicesModule.add(amsModule);
        indicesModule.add(arpModule);
        indicesModule.add(bacnetModule);
        indicesModule.add(cipModule);
        indicesModule.add(coapModule);
        indicesModule.add(dnp3Module);
        indicesModule.add(dnsModule);
        indicesModule.add(dsiModule);
        indicesModule.add(egdModule);
        indicesModule.add(eplv1Module);
        indicesModule.add(esioModule);
        indicesModule.add(esModule);
        indicesModule.add(ethercatModule);
        indicesModule.add(ethernetipModule);
        indicesModule.add(ffhseModule);
        indicesModule.add(foxModule);
        indicesModule.add(ftpModule);
        indicesModule.add(gooseModule);
        indicesModule.add(gryphonModule);
        indicesModule.add(hartipModule);
        indicesModule.add(httpsModule);
        indicesModule.add(iec104Module);
        indicesModule.add(imapModule);
        indicesModule.add(influxModule);
        indicesModule.add(ircModule);
        indicesModule.add(lldpModule);
        indicesModule.add(llmnrModule);
        indicesModule.add(lontalkModule);
        indicesModule.add(mdnsModule);
        indicesModule.add(mmsModule);
        indicesModule.add(modbusModule);
        indicesModule.add(mysqlModule);
        indicesModule.add(oicqModule);
        indicesModule.add(omronfinsModule);
        indicesModule.add(opcModule);
        indicesModule.add(opcuaModule);
        indicesModule.add(opensafetyModule);
        indicesModule.add(pgsqlModule);
        indicesModule.add(powerlinkModule);
        indicesModule.add(s7plusModule);
        indicesModule.add(sercosiiiModule);
        indicesModule.add(sf_haspModule);
        indicesModule.add(ssdpModule);


        for (Module module: indicesModule) {
            module.fieldFillin(rddMap);
        }

        // Field
        h = new HotkeyFinder();
//        List<Class<Field>> list = new ArrayList<>();
//        list.add(CmdField.class);
//        for (Field f: list) {
//            c.gettS
//        }

        h.appendOperationLists(CmdField.getStrList());
        System.out.println(h.getFrequentOperationList());
        h = new HotkeyFinder();
        CmdField.reset();
        AddrField.reset();
    }
}
