package core.learn;

import core.learn.field.*;
import core.learn.module.Module;
import core.learn.module.cross.index.*;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldHotkeyFindLoader {
    public Map<String, List<List<String>>> execute(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
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
        Map<String, List<List<String>>> result = new HashMap<>();
        HotkeyFinder h = new HotkeyFinder();

        //Addr
        h.appendOperationLists(AddrField.getStrList());
        result.put("addr", h.getFrequentOperationList());
        h = new HotkeyFinder();
        AddrField.reset();
        //Appid
        h.appendOperationLists(AppidField.getStrList());
        result.put("appid", h.getFrequentOperationList());
        h = new HotkeyFinder();
        AppidField.reset();
        //Cmd
        h.appendOperationLists(CmdField.getStrList());
        result.put("cmd", h.getFrequentOperationList());
        h = new HotkeyFinder();
        CmdField.reset();
        //Cmd_str
        h.appendOperationLists(Cmd_strField.getStrList());
        result.put("cmd_str", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Cmd_strField.reset();
        //Code
        h.appendOperationLists(CodeField.getStrList());
        result.put("code", h.getFrequentOperationList());
        h = new HotkeyFinder();
        CodeField.reset();
        //Commaddr
        h.appendOperationLists(CommaddrField.getStrList());
        result.put("commaddr", h.getFrequentOperationList());
        h = new HotkeyFinder();
        CommaddrField.reset();
        //Conf_rev
        h.appendOperationLists(Conf_revField.getStrList());
        result.put("conf_rev", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Conf_revField.reset();
        //Confirm
        h.appendOperationLists(ConfirmField.getStrList());
        result.put("confirm", h.getFrequentOperationList());
        h = new HotkeyFinder();
        ConfirmField.reset();
        //Content
        h.appendOperationLists(ContentField.getStrList());
        result.put("content", h.getFrequentOperationList());
        h = new HotkeyFinder();
        ContentField.reset();
        //Context
        h.appendOperationLists(ContextField.getStrList());
        result.put("context", h.getFrequentOperationList());
        h = new HotkeyFinder();
        ContextField.reset();
        //Daddr
        h.appendOperationLists(DaddrField.getStrList());
        result.put("daddr", h.getFrequentOperationList());
        h = new HotkeyFinder();
        DaddrField.reset();
        //Data_hdr
        h.appendOperationLists(Data_hdrField.getStrList());
        result.put("data_hdr", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Data_hdrField.reset();
        //Data_pt_id
        h.appendOperationLists(Data_pt_idField.getStrList());
        result.put("data_pt_id", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Data_pt_idField.reset();
        //Data_type
        h.appendOperationLists(Data_typeField.getStrList());
        result.put("data_type", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Data_typeField.reset();
        //Data
        h.appendOperationLists(DataField.getStrList());
        result.put("data", h.getFrequentOperationList());
        h = new HotkeyFinder();
        DataField.reset();
        //Datalen
        h.appendOperationLists(DatalenField.getStrList());
        result.put("datalen", h.getFrequentOperationList());
        h = new HotkeyFinder();
        DatalenField.reset();
        //Datset
        h.appendOperationLists(DatsetField.getStrList());
        result.put("datset", h.getFrequentOperationList());
        h = new HotkeyFinder();
        DatsetField.reset();
        //Dest_str
        h.appendOperationLists(Dest_strField.getStrList());
        result.put("dest_str", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Dest_strField.reset();
        //Dest
        h.appendOperationLists(DestField.getStrList());
        result.put("dest", h.getFrequentOperationList());
        h = new HotkeyFinder();
        DestField.reset();
        //Dir
        h.appendOperationLists(DirField.getStrList());
        result.put("dir", h.getFrequentOperationList());
        h = new HotkeyFinder();
        DirField.reset();
        //Domain
        h.appendOperationLists(DomainField.getStrList());
        result.put("domain", h.getFrequentOperationList());
        h = new HotkeyFinder();
        DomainField.reset();
        //Dst_ch
        h.appendOperationLists(Dst_chField.getStrList());
        result.put("dst_ch", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Dst_chField.reset();
        //Dst
        h.appendOperationLists(DstField.getStrList());
        result.put("dst", h.getFrequentOperationList());
        h = new HotkeyFinder();
        DstField.reset();
        //Entry_num
        h.appendOperationLists(Entry_numField.getStrList());
        result.put("entry_num", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Entry_numField.reset();
        //Exchange_id
        h.appendOperationLists(Exchange_idField.getStrList());
        result.put("exchange_id", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Exchange_idField.reset();
        //Ext_ser_id
        h.appendOperationLists(Ext_ser_idField.getStrList());
        result.put("ext_ser_id", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Ext_ser_idField.reset();
        //Fda_addr
        h.appendOperationLists(Fda_addrField.getStrList());
        result.put("fda_addr", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Fda_addrField.reset();
        //Flag
        h.appendOperationLists(FlagField.getStrList());
        result.put("flag", h.getFrequentOperationList());
        h = new HotkeyFinder();
        FlagField.reset();
        //Frame_type
        h.appendOperationLists(Frame_typeField.getStrList());
        result.put("frame_type", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Frame_typeField.reset();
        //Func
        h.appendOperationLists(FuncField.getStrList());
        result.put("func", h.getFrequentOperationList());
        h = new HotkeyFinder();
        FuncField.reset();
        //Function
        h.appendOperationLists(FunctionField.getStrList());
        result.put("function", h.getFrequentOperationList());
        h = new HotkeyFinder();
        FunctionField.reset();
        //Gocbref
        h.appendOperationLists(GocbrefField.getStrList());
        result.put("gocbref", h.getFrequentOperationList());
        h = new HotkeyFinder();
        GocbrefField.reset();
        //Goid
        h.appendOperationLists(GoidField.getStrList());
        result.put("goid", h.getFrequentOperationList());
        h = new HotkeyFinder();
        GoidField.reset();
        //Groupnum
        h.appendOperationLists(GroupnumField.getStrList());
        result.put("groupnum", h.getFrequentOperationList());
        h = new HotkeyFinder();
        GroupnumField.reset();
        //Hdr_flag
        h.appendOperationLists(Hdr_flagField.getStrList());
        result.put("hdr_flag", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Hdr_flagField.reset();
        //Hostname
        h.appendOperationLists(HostnameField.getStrList());
        result.put("hostname", h.getFrequentOperationList());
        h = new HotkeyFinder();
        HostnameField.reset();
        //Infoaddr
        h.appendOperationLists(InfoaddrField.getStrList());
        result.put("infoaddr", h.getFrequentOperationList());
        h = new HotkeyFinder();
        InfoaddrField.reset();
        //Interface
        h.appendOperationLists(InterfaceField.getStrList());
        result.put("interface", h.getFrequentOperationList());
        h = new HotkeyFinder();
        InterfaceField.reset();
        //Ip
        h.appendOperationLists(IpField.getStrList());
        result.put("ip", h.getFrequentOperationList());
        h = new HotkeyFinder();
        IpField.reset();
        //Json
        h.appendOperationLists(JsonField.getStrList());
        result.put("json", h.getFrequentOperationList());
        h = new HotkeyFinder();
        JsonField.reset();
        //Layer
        h.appendOperationLists(LayerField.getStrList());
        result.put("layer", h.getFrequentOperationList());
        h = new HotkeyFinder();
        LayerField.reset();
        //Loc
        h.appendOperationLists(LocField.getStrList());
        result.put("loc", h.getFrequentOperationList());
        h = new HotkeyFinder();
        LocField.reset();
        //Master
        h.appendOperationLists(MasterField.getStrList());
        result.put("master", h.getFrequentOperationList());
        h = new HotkeyFinder();
        MasterField.reset();
        //Method
        h.appendOperationLists(MethodField.getStrList());
        result.put("method", h.getFrequentOperationList());
        h = new HotkeyFinder();
        MethodField.reset();
        //Msgid
        h.appendOperationLists(MsgidField.getStrList());
        result.put("msgid", h.getFrequentOperationList());
        h = new HotkeyFinder();
        MsgidField.reset();
        //Msgtype_str
        h.appendOperationLists(Msgtype_strField.getStrList());
        result.put("msgtype_str", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Msgtype_strField.reset();
        //Msgtype
        h.appendOperationLists(MsgtypeField.getStrList());
        result.put("msgtype", h.getFrequentOperationList());
        h = new HotkeyFinder();
        MsgtypeField.reset();
        //N_data
        h.appendOperationLists(N_dataField.getStrList());
        result.put("n_data", h.getFrequentOperationList());
        h = new HotkeyFinder();
        N_dataField.reset();
        //Ndscom
        h.appendOperationLists(NdscomField.getStrList());
        result.put("ndscom", h.getFrequentOperationList());
        h = new HotkeyFinder();
        NdscomField.reset();
        //Node_d
        h.appendOperationLists(Node_dField.getStrList());
        result.put("node_d", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Node_dField.reset();
        //Node_s
        h.appendOperationLists(Node_sField.getStrList());
        result.put("node_s", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Node_sField.reset();
        //Offset_addr
        h.appendOperationLists(Offset_addrField.getStrList());
        result.put("offset_addr", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Offset_addrField.reset();
        //Offset_errcode
        h.appendOperationLists(Offset_errcodeField.getStrList());
        result.put("offset_errcode", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Offset_errcodeField.reset();
        //Pkt_type
        h.appendOperationLists(Pkt_typeField.getStrList());
        result.put("pkt_type", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Pkt_typeField.reset();
        //Producer_id
        h.appendOperationLists(Producer_idField.getStrList());
        result.put("producer_id", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Producer_idField.reset();
        //Proto
        h.appendOperationLists(ProtoField.getStrList());
        result.put("proto", h.getFrequentOperationList());
        h = new HotkeyFinder();
        ProtoField.reset();
        //QQ
        h.appendOperationLists(QqidField.getStrList());
        result.put("qq", h.getFrequentOperationList());
        h = new HotkeyFinder();
        QqidField.reset();
        //Rdn
        h.appendOperationLists(RdnField.getStrList());
        result.put("rdn", h.getFrequentOperationList());
        h = new HotkeyFinder();
        RdnField.reset();
        //Req_id
        h.appendOperationLists(Req_idField.getStrList());
        result.put("req_id", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Req_idField.reset();
        //Reqid
        h.appendOperationLists(ReqidField.getStrList());
        result.put("reqid", h.getFrequentOperationList());
        h = new HotkeyFinder();
        ReqidField.reset();
        //Reqtype
        h.appendOperationLists(ReqtypeField.getStrList());
        result.put("reqtype", h.getFrequentOperationList());
        h = new HotkeyFinder();
        ReqtypeField.reset();
        //Saddr
        h.appendOperationLists(SaddrField.getStrList());
        result.put("saddr", h.getFrequentOperationList());
        h = new HotkeyFinder();
        SaddrField.reset();
        //Safezone
        h.appendOperationLists(SafezoneField.getStrList());
        result.put("safezone", h.getFrequentOperationList());
        h = new HotkeyFinder();
        SafezoneField.reset();
        //Sender_id
        h.appendOperationLists(Sender_idField.getStrList());
        result.put("sender_id", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Sender_idField.reset();
        //Server
        h.appendOperationLists(ServerField.getStrList());
        result.put("server", h.getFrequentOperationList());
        h = new HotkeyFinder();
        ServerField.reset();
        //Service
        h.appendOperationLists(ServiceField.getStrList());
        result.put("service", h.getFrequentOperationList());
        h = new HotkeyFinder();
        ServiceField.reset();
        //Sid
        h.appendOperationLists(SidField.getStrList());
        result.put("sid", h.getFrequentOperationList());
        h = new HotkeyFinder();
        SidField.reset();
        //Slave_addr
        h.appendOperationLists(Slave_addrField.getStrList());
        result.put("slave_addr", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Slave_addrField.reset();
        //Slave
        h.appendOperationLists(SlaveField.getStrList());
        result.put("slave", h.getFrequentOperationList());
        h = new HotkeyFinder();
        SlaveField.reset();
        //Sn_from
        h.appendOperationLists(Sn_fromField.getStrList());
        result.put("sn_from", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Sn_fromField.reset();
        //Sn_to
        h.appendOperationLists(Sn_toField.getStrList());
        result.put("sn_to", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Sn_toField.reset();
        //Source_str
        h.appendOperationLists(Source_strField.getStrList());
        result.put("source_str", h.getFrequentOperationList());
        h = new HotkeyFinder();
        SourceField.reset();
        //Source
        h.appendOperationLists(SourceField.getStrList());
        result.put("source", h.getFrequentOperationList());
        h = new HotkeyFinder();
        SourceField.reset();
        //Sqnum
        h.appendOperationLists(SqnumField.getStrList());
        result.put("sqnum", h.getFrequentOperationList());
        h = new HotkeyFinder();
        SqnumField.reset();
        //Src_cid
        h.appendOperationLists(Src_cidField.getStrList());
        result.put("src_cid", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Src_cidField.reset();
        //Src_statid
        h.appendOperationLists(Src_statidField.getStrList());
        result.put("src_statid", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Src_statidField.reset();
        //Src
        h.appendOperationLists(SrcField.getStrList());
        result.put("src", h.getFrequentOperationList());
        h = new HotkeyFinder();
        SrcField.reset();
        //Status
        h.appendOperationLists(StatusField.getStrList());
        result.put("status", h.getFrequentOperationList());
        h = new HotkeyFinder();
        StatusField.reset();
        //Stnum
        h.appendOperationLists(StnumField.getStrList());
        result.put("stnum", h.getFrequentOperationList());
        h = new HotkeyFinder();
        StnumField.reset();
        //Subnet_d
        h.appendOperationLists(Subnet_dField.getStrList());
        result.put("subnet_d", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Subnet_dField.reset();
        //Subnet_s
        h.appendOperationLists(Subnet_sField.getStrList());
        result.put("subnet_s", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Subnet_sField.reset();
        //Tel_id
        h.appendOperationLists(Tel_idField.getStrList());
        result.put("tel_id", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Tel_idField.reset();
        //Test
        h.appendOperationLists(TestField.getStrList());
        result.put("test", h.getFrequentOperationList());
        h = new HotkeyFinder();
        TestField.reset();
        //Token
        h.appendOperationLists(TokenField.getStrList());
        result.put("token", h.getFrequentOperationList());
        h = new HotkeyFinder();
        TokenField.reset();
        //Trans_id
        h.appendOperationLists(Trans_idField.getStrList());
        result.put("trans_id", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Trans_idField.reset();
        //Type
        h.appendOperationLists(TypeField.getStrList());
        result.put("type", h.getFrequentOperationList());
        h = new HotkeyFinder();
        TypeField.reset();
        //Url
        h.appendOperationLists(UrlField.getStrList());
        result.put("url", h.getFrequentOperationList());
        h = new HotkeyFinder();
        UrlField.reset();
        //Varnum
        h.appendOperationLists(VarnumField.getStrList());
        result.put("varnum", h.getFrequentOperationList());
        h = new HotkeyFinder();
        VarnumField.reset();
        //Vendor_code
        h.appendOperationLists(Vendor_codeField.getStrList());
        result.put("vendor_code", h.getFrequentOperationList());
        h = new HotkeyFinder();
        Vendor_codeField.reset();
        //VerField
        h.appendOperationLists(VerField.getStrList());
        result.put("ver", h.getFrequentOperationList());
        h = new HotkeyFinder();
        VerField.reset();
        return result;
    }
}
