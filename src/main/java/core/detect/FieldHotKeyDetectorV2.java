package core.detect;

import core.learn.HotkeyFinder;
import core.learn.field.*;
import core.learn.module.Module;
import core.learn.module.cross.index.*;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldHotKeyDetectorV2 {
    /**
     * FieldHotKeyDetectorV2 类
     * 是Field字段发现的核心类、控制器类，提供了统计检测和报警的入口方法。
     */

    /**
     * execute手动指定了field和index的关系。** 这里有待改进
     * @param rddMap rdd集合
     * @return 学习到的高频操作队列
     */
    public Map<String, List<Map<List<String>, Long>>> execute(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
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

        /**
         *  Field learning , slove the result into result map.
         **/
        Map<String, List<Map<List<String>, Long>>> result = new HashMap<>();
        HotkeyFinder h = new HotkeyFinder();

        //Addr
        h.appendOperationLists(AddrField.getStrList());
        result.put("addr", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        AddrField.reset();
        //Appid
        h.appendOperationLists(AppidField.getStrList());
        result.put("appid", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        AppidField.reset();
        //Cmd
        h.appendOperationLists(CmdField.getStrList());
        result.put("cmd", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        CmdField.reset();
        //Cmd_str
        h.appendOperationLists(Cmd_strField.getStrList());
        result.put("cmd_str", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Cmd_strField.reset();
        //Code
        h.appendOperationLists(CodeField.getStrList());
        result.put("code", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        CodeField.reset();
        //Commaddr
        h.appendOperationLists(CommaddrField.getStrList());
        result.put("commaddr", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        CommaddrField.reset();
        //Conf_rev
        h.appendOperationLists(Conf_revField.getStrList());
        result.put("conf_rev", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Conf_revField.reset();
        //Confirm
        h.appendOperationLists(ConfirmField.getStrList());
        result.put("confirm", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        ConfirmField.reset();
        //Content
        h.appendOperationLists(ContentField.getStrList());
        result.put("content", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        ContentField.reset();
        //Context
        h.appendOperationLists(ContextField.getStrList());
        result.put("context", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        ContextField.reset();
        //Daddr
        h.appendOperationLists(DaddrField.getStrList());
        result.put("daddr", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        DaddrField.reset();
        //Data_hdr
        h.appendOperationLists(Data_hdrField.getStrList());
        result.put("data_hdr", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Data_hdrField.reset();
        //Data_pt_id
        h.appendOperationLists(Data_pt_idField.getStrList());
        result.put("data_pt_id", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Data_pt_idField.reset();
        //Data_type
        h.appendOperationLists(Data_typeField.getStrList());
        result.put("data_type", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Data_typeField.reset();
        //Data
        h.appendOperationLists(DataField.getStrList());
        result.put("data", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        DataField.reset();
        //Datalen
        h.appendOperationLists(DatalenField.getStrList());
        result.put("datalen", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        DatalenField.reset();
        //Datset
        h.appendOperationLists(DatsetField.getStrList());
        result.put("datset", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        DatsetField.reset();
        //Dest_str
        h.appendOperationLists(Dest_strField.getStrList());
        result.put("dest_str", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Dest_strField.reset();
        //Dest
        h.appendOperationLists(DestField.getStrList());
        result.put("dest", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        DestField.reset();
        //Dir
        h.appendOperationLists(DirField.getStrList());
        result.put("dir", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        DirField.reset();
        //Domain
        h.appendOperationLists(DomainField.getStrList());
        result.put("domain", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        DomainField.reset();
        //Dst_ch
        h.appendOperationLists(Dst_chField.getStrList());
        result.put("dst_ch", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Dst_chField.reset();
        //Dst
        h.appendOperationLists(DstField.getStrList());
        result.put("dst", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        DstField.reset();
        //Entry_num
        h.appendOperationLists(Entry_numField.getStrList());
        result.put("entry_num", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Entry_numField.reset();
        //Exchange_id
        h.appendOperationLists(Exchange_idField.getStrList());
        result.put("exchange_id", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Exchange_idField.reset();
        //Ext_ser_id
        h.appendOperationLists(Ext_ser_idField.getStrList());
        result.put("ext_ser_id", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Ext_ser_idField.reset();
        //Fda_addr
        h.appendOperationLists(Fda_addrField.getStrList());
        result.put("fda_addr", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Fda_addrField.reset();
        //Flag
        h.appendOperationLists(FlagField.getStrList());
        result.put("flag", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        FlagField.reset();
        //Frame_type
        h.appendOperationLists(Frame_typeField.getStrList());
        result.put("frame_type", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Frame_typeField.reset();
        //Func
        h.appendOperationLists(FuncField.getStrList());
        result.put("func", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        FuncField.reset();
        //Function
        h.appendOperationLists(FunctionField.getStrList());
        result.put("function", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        FunctionField.reset();
        //Gocbref
        h.appendOperationLists(GocbrefField.getStrList());
        result.put("gocbref", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        GocbrefField.reset();
        //Goid
        h.appendOperationLists(GoidField.getStrList());
        result.put("goid", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        GoidField.reset();
        //Groupnum
        h.appendOperationLists(GroupnumField.getStrList());
        result.put("groupnum", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        GroupnumField.reset();
        //Hdr_flag
        h.appendOperationLists(Hdr_flagField.getStrList());
        result.put("hdr_flag", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Hdr_flagField.reset();
        //Hostname
        h.appendOperationLists(HostnameField.getStrList());
        result.put("hostname", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        HostnameField.reset();
        //Infoaddr
        h.appendOperationLists(InfoaddrField.getStrList());
        result.put("infoaddr", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        InfoaddrField.reset();
        //Interface
        h.appendOperationLists(InterfaceField.getStrList());
        result.put("interface", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        InterfaceField.reset();
        //Ip
        h.appendOperationLists(IpField.getStrList());
        result.put("ip", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        IpField.reset();
        //Json
        h.appendOperationLists(JsonField.getStrList());
        result.put("json", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        JsonField.reset();
        //Layer
        h.appendOperationLists(LayerField.getStrList());
        result.put("layer", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        LayerField.reset();
        //Loc
        h.appendOperationLists(LocField.getStrList());
        result.put("loc", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        LocField.reset();
        //Master
        h.appendOperationLists(MasterField.getStrList());
        result.put("master", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        MasterField.reset();
        //Method
        h.appendOperationLists(MethodField.getStrList());
        result.put("method", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        MethodField.reset();
        //Msgid
        h.appendOperationLists(MsgidField.getStrList());
        result.put("msgid", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        MsgidField.reset();
        //Msgtype_str
        h.appendOperationLists(Msgtype_strField.getStrList());
        result.put("msgtype_str", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Msgtype_strField.reset();
        //Msgtype
        h.appendOperationLists(MsgtypeField.getStrList());
        result.put("msgtype", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        MsgtypeField.reset();
        //N_data
        h.appendOperationLists(N_dataField.getStrList());
        result.put("n_data", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        N_dataField.reset();
        //Ndscom
        h.appendOperationLists(NdscomField.getStrList());
        result.put("ndscom", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        NdscomField.reset();
        //Node_d
        h.appendOperationLists(Node_dField.getStrList());
        result.put("node_d", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Node_dField.reset();
        //Node_s
        h.appendOperationLists(Node_sField.getStrList());
        result.put("node_s", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Node_sField.reset();
        //Offset_addr
        h.appendOperationLists(Offset_addrField.getStrList());
        result.put("offset_addr", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Offset_addrField.reset();
        //Offset_errcode
        h.appendOperationLists(Offset_errcodeField.getStrList());
        result.put("offset_errcode", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Offset_errcodeField.reset();
        //Pkt_type
        h.appendOperationLists(Pkt_typeField.getStrList());
        result.put("pkt_type", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Pkt_typeField.reset();
        //Producer_id
        h.appendOperationLists(Producer_idField.getStrList());
        result.put("producer_id", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Producer_idField.reset();
        //Proto
        h.appendOperationLists(ProtoField.getStrList());
        result.put("proto", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        ProtoField.reset();
        //QQ
        h.appendOperationLists(QqidField.getStrList());
        result.put("qq", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        QqidField.reset();
        //Rdn
        h.appendOperationLists(RdnField.getStrList());
        result.put("rdn", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        RdnField.reset();
        //Req_id
        h.appendOperationLists(Req_idField.getStrList());
        result.put("req_id", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Req_idField.reset();
        //Reqid
        h.appendOperationLists(ReqidField.getStrList());
        result.put("reqid", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        ReqidField.reset();
        //Reqtype
        h.appendOperationLists(ReqtypeField.getStrList());
        result.put("reqtype", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        ReqtypeField.reset();
        //Saddr
        h.appendOperationLists(SaddrField.getStrList());
        result.put("saddr", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        SaddrField.reset();
        //Safezone
        h.appendOperationLists(SafezoneField.getStrList());
        result.put("safezone", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        SafezoneField.reset();
        //Sender_id
        h.appendOperationLists(Sender_idField.getStrList());
        result.put("sender_id", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Sender_idField.reset();
        //Server
        h.appendOperationLists(ServerField.getStrList());
        result.put("server", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        ServerField.reset();
        //Service
        h.appendOperationLists(ServiceField.getStrList());
        result.put("service", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        ServiceField.reset();
        //Sid
        h.appendOperationLists(SidField.getStrList());
        result.put("sid", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        SidField.reset();
        //Slave_addr
        h.appendOperationLists(Slave_addrField.getStrList());
        result.put("slave_addr", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Slave_addrField.reset();
        //Slave
        h.appendOperationLists(SlaveField.getStrList());
        result.put("slave", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        SlaveField.reset();
        //Sn_from
        h.appendOperationLists(Sn_fromField.getStrList());
        result.put("sn_from", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Sn_fromField.reset();
        //Sn_to
        h.appendOperationLists(Sn_toField.getStrList());
        result.put("sn_to", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Sn_toField.reset();
        //Source_str
        h.appendOperationLists(Source_strField.getStrList());
        result.put("source_str", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        SourceField.reset();
        //Source
        h.appendOperationLists(SourceField.getStrList());
        result.put("source", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        SourceField.reset();
        //Sqnum
        h.appendOperationLists(SqnumField.getStrList());
        result.put("sqnum", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        SqnumField.reset();
        //Src_cid
        h.appendOperationLists(Src_cidField.getStrList());
        result.put("src_cid", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Src_cidField.reset();
        //Src_statid
        h.appendOperationLists(Src_statidField.getStrList());
        result.put("src_statid", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Src_statidField.reset();
        //Src
        h.appendOperationLists(SrcField.getStrList());
        result.put("src", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        SrcField.reset();
        //Status
        h.appendOperationLists(StatusField.getStrList());
        result.put("status", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        StatusField.reset();
        //Stnum
        h.appendOperationLists(StnumField.getStrList());
        result.put("stnum", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        StnumField.reset();
        //Subnet_d
        h.appendOperationLists(Subnet_dField.getStrList());
        result.put("subnet_d", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Subnet_dField.reset();
        //Subnet_s
        h.appendOperationLists(Subnet_sField.getStrList());
        result.put("subnet_s", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Subnet_sField.reset();
        //Tel_id
        h.appendOperationLists(Tel_idField.getStrList());
        result.put("tel_id", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Tel_idField.reset();
        //Test
        h.appendOperationLists(TestField.getStrList());
        result.put("test", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        TestField.reset();
        //Token
        h.appendOperationLists(TokenField.getStrList());
        result.put("token", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        TokenField.reset();
        //Trans_id
        h.appendOperationLists(Trans_idField.getStrList());
        result.put("trans_id", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Trans_idField.reset();
        //Type
        h.appendOperationLists(TypeField.getStrList());
        result.put("type", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        TypeField.reset();
        //Url
        h.appendOperationLists(UrlField.getStrList());
        result.put("url", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        UrlField.reset();
        //Varnum
        h.appendOperationLists(VarnumField.getStrList());
        result.put("varnum", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        VarnumField.reset();
        //Vendor_code
        h.appendOperationLists(Vendor_codeField.getStrList());
        result.put("vendor_code", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        Vendor_codeField.reset();
        //VerField
        h.appendOperationLists(VerField.getStrList());
        result.put("ver", h.getFrequentOperationListWithCounting());
        h = new HotkeyFinder();
        VerField.reset();
        return result;
    }
}
