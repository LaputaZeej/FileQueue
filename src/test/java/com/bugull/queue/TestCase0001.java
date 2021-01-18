package com.bugull.queue;

import com.bugu.queue.FileQueue;
import com.bugu.queue.ImmutableFileQueue;
import com.bugu.queue.MutableFileQueue;
import com.bugu.queue.transform.GsonTransform;
import com.bugu.queue.util.Logger;
import com.bugu.queue.util.Size;
import org.junit.Test;

import javax.print.attribute.standard.Media;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TestCase0001 {
    @Test
    public void t01() {

        MutableFileQueue<String> fileQueue = new MutableFileQueue<>("path", new GsonTransform<>(String.class));
        try {
            fileQueue.put("1");
            String take = fileQueue.take();
            System.out.println(take);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void t02() {
        String path = createPath("t02_0000001.txt");
        MutableFileQueue<String> fileQueue = new MutableFileQueue<>(path, new GsonTransform<>(String.class));
    }

    // put
    @Test
    public void t03() {
        Logger.info("-------------------------------------------");
        String path = createPath("t03_0000009.txt");
        MutableFileQueue<MqttMessage> fileQueue = new MutableFileQueue<>(path, new GsonTransform<>(MqttMessage.class));

        int index = 0;
        int count = 100;
        while (true) {
            if (index > count) {
                break;
            }
            try {
                long time = System.currentTimeMillis();
                MqttMessage message = new MqttMessage(time, index % 2, "content = " + index, index);
                fileQueue.put(message);
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
            index++;
        }
        fileQueue.close();
        Logger.info("-------------------------------------------");


    }

    // take
    @Test
    public void t04() {
        Logger.info("-------------------------------------------");
        String path = createPath("t03_0000009.txt");
        ImmutableFileQueue<MqttMessage> fileQueue = new ImmutableFileQueue<>(path, new GsonTransform<>(MqttMessage.class));
        while (true) {
            try {
                Thread.sleep(500);
                MqttMessage take = fileQueue.take();
                System.out.println(take);
            } catch (Exception e) {
                break;
            }
        }

    }

    @Test
    public void takeAndPut() {
        Logger.info("-------------------------------------------");
        String path = createPath("takeAndPut_0000001.txt");
        MutableFileQueue<MqttMessage> fileQueue = new MutableFileQueue<>(path, new GsonTransform<>(MqttMessage.class));
        startThread(() -> {
            while (true) {
                try {
                    Thread.sleep(500);
                    MqttMessage take = fileQueue.take();
                    System.out.println(take);
                } catch (Exception e) {
                    break;
                }
            }
        });

        startThread(() -> {
            while (true) {
                try {
                    int index = 0;
                    int count = 10000;
                    while (true) {
                        if (index > count) {
                            break;
                        }
                        try {
                            long time = System.currentTimeMillis();
                            MqttMessage message = new MqttMessage(time, index % 2, "content = " + index, index);
                            fileQueue.put(message);
                        } catch (Exception e) {
                            e.printStackTrace();
                            break;
                        }
                        index++;
                    }
                } catch (Exception e) {
                    break;
                }
            }
        });
        fileQueue.close();
        holdOn();
    }

    @Test
    public void testPut() {
        String path = createPath("testPut_0000003.txt");
        MutableFileQueue<MqttMessage> fileQueue = new MutableFileQueue<>(path, new GsonTransform<>(MqttMessage.class));
        fileQueue.setOnFileQueueChanged((t1, t2, t3) -> {
            System.out.println("====================================");
            System.out.println("============   error!!!    =========");
            System.out.println("====================================");
            full.set(true);
        });
        startThread(() -> {
            try {
                MqttMessage message = new MqttMessage(111, 111, "11111", 1);
                fileQueue.put(message);
            } catch (Exception e) {
                e.printStackTrace();
            }

        });

        startThread(() -> {
            try {
                MqttMessage message = new MqttMessage(2222, 2222, "22222", 2);
                fileQueue.put(message);
            } catch (Exception e) {
                e.printStackTrace();
            }

        });
        fileQueue.close();
        holdOn();
    }

    AtomicBoolean full = new AtomicBoolean(false);


    @Test
    public void takeAndPutMutable() {
        Logger.info("-------------------------------------------");
        String path = createPath("takeAndPutMutable_0000009_10G.txt");
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        long maxSize = Size._G * 10;
        MutableFileQueue<MqttMessage> fileQueue = new MutableFileQueue<>(path, maxSize, new GsonTransform<>(MqttMessage.class));

        fileQueue.setOnFileQueueChanged((t1, t2, t3) -> {
            if (t2 == 0 && t3.getLength() >= maxSize) {
                System.out.println("====================================");
                System.out.println("============   error!!!    =========");
                System.out.println("====================================");
                full.set(true);
            }
        });
        executorService.execute(new PutRunnable(fileQueue, "<1>"));
        executorService.execute(new PutRunnable(fileQueue, "<2>"));
        executorService.execute(new TakeRunnable(fileQueue, "<1>"));
        executorService.execute(new TakeRunnable(fileQueue, "<2>"));
        executorService.execute(new TakeRunnable(fileQueue, "<3>"));
        fileQueue.close();
        holdOn();
    }

    private class PutRunnable implements Runnable {
        FileQueue<MqttMessage> fileQueue;
        String tag;

        public PutRunnable(FileQueue<MqttMessage> fileQueue, String tag) {
            this.fileQueue = fileQueue;
            this.tag = tag;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    int index = 0;
                    while (true) {

                        Thread.sleep(100);
                        try {
                            long time = System.currentTimeMillis();
                            MqttMessage message = new MqttMessage(time, index % 2, tag + index + TEXT, index);
                            fileQueue.put(message);
                        } catch (Exception e) {
                            e.printStackTrace();
                            break;
                        }
                        index++;
                    }
                } catch (Exception e) {
                    break;
                }
            }
        }
    }

    private static class TakeRunnable implements Runnable {
        FileQueue<MqttMessage> fileQueue;
        String tag;

        public TakeRunnable(FileQueue<MqttMessage> fileQueue, String tag) {
            this.fileQueue = fileQueue;
            this.tag = tag;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(2000);
                    MqttMessage take = fileQueue.take();
                    System.out.println("take " + tag + " =>" + take);
                } catch (Exception e) {
                    break;
                }
            }
        }
    }


    private void holdOn() {
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String createPath(String fileName) {
        return "C:\\Users\\xpl\\Documents\\projs\\mqtt\\case\\" + fileName;
    }

    private void startThread(Runnable runnable) {
        new Thread(runnable).start();
    }

    private final String TEXT = "   <-- 如何布局？ XML layout 文件跑哪去了？\n" +
            "在Android中，您通过XML编写布局，但在Flutter中，您可以使用widget树来编写布局。\n" +
            "\n" +
            "这里是一个例子，展示了如何在屏幕上显示一个简单的Widget并添加一些padding。\n" +
            "\n" +
            "@override\n" +
            "Widget build(BuildContext context) {\n" +
            "  return new Scaffold(\n" +
            "    appBar: new AppBar(\n" +
            "      title: new Text(\"Sample App\"),\n" +
            "    ),\n" +
            "    body: new Center(\n" +
            "      child: new MaterialButton(\n" +
            "        onPressed: () {},\n" +
            "        child: new Text('Hello'),\n" +
            "        padding: new EdgeInsets.only(left: 10.0, right: 10.0),\n" +
            "      ),\n" +
            "    ),\n" +
            "  );\n" +
            "}\n" +
            "您可以查看Flutter所提供的所有布局: Flutter widget layout\n" +
            "\n" +
            "如何在布局中添加或删除组件\n" +
            "在Android中，您可以从父级控件调用addChild或removeChild以动态添加或删除View。 在Flutter中，因为widget是不可变的，所以没有addChild。相反，您可以传入一个函数，该函数返回一个widget给父项，并通过布尔值控制该widget的创建。\n" +
            "\n" +
            "例如，当你点击一个FloatingActionButton时，如何在两个widget之间切换：" +
            "" +
            "刘邦登基后，采用叔孙通的建议，恢复礼法，设三公和九卿，任萧何为丞相，采取与民休息、清静无为、休养生息的黄老政策 [15]  。鼓励生产，轻徭薄赋。在政治上，则先分封功臣韩信、彭越、英布等为王，等到政权稳固，为了防止反叛和巩固皇权稳定则又以种种罪名取消他们的王爵，或贬或杀，改封刘氏宗亲为王，订立了“非刘氏而王者，天下共击之” [16]  的誓言。 [17] \n" +
            "此时，由于历经多年动乱，国力较弱，而北方草原在冒顿单于的带领下首次统一了蒙古草原国力强盛。汉高祖六年（前201年）秋天匈奴冒顿单于重重包围了韩王信，韩王信多次派使者到匈奴处求和。汉朝派人带兵前往援救，但怀疑韩王信多次私派使者，有背叛汉朝之心，派人责备韩王信。韩王信害怕被杀，于是就和匈奴约定好共同攻打汉朝，起兵造反，把国都马邑拿出投降匈奴，并率军攻打太原。汉高祖七年（前200年）冬天，刘邦亲自率军前往攻打，在铜鞮（dī，\u007F堤）击败韩王信的军队，并将其部将王喜斩杀。\n" +
            "韩王信逃跑投奔匈奴，他的部将白土人曼丘臣、王黄等人拥立赵王的后代赵利为王，又收集起韩王信被击败逃散的军队，并和韩王信及匈奴冒顿单于商议一齐攻打汉朝。匈奴派遣左右贤王带领一万多骑兵和王黄等人驻扎在广武以南地区，到达晋阳时，和汉军交战，汉军将他们打得大败，乘胜追到离石，又把他们打败。匈奴再次在楼烦西将地区聚集军队，汉高祖命令战车部队和骑兵把他们打败。匈奴常败退逃跑，汉军乘胜追击败兵，听说冒顿单于驻扎在代谷，汉高祖当时在晋阳，派人去侦察冒顿，侦察人员回来报告说“可以出击”。刘邦也就到达平城。刘邦出城登上白登山，被匈奴骑兵团团围住，刘邦采纳陈平之计，派人送给匈奴王后阏氏许多礼物。阏氏便劝冒顿单于说：“现在已经攻取了汉朝的土地，但还是不能居住下来；更何况汉朝援兵即将到来。”匈奴遂逐渐撒去。\n" +
            "当时天降大雾，汉朝派人在白登山和平城之间往来，匈奴一点也没有察觉。陈平对刘邦说：“匈奴人都用长枪弓箭，请命令士兵每张强弩朝外搭两支利箭，慢慢地撤出包围。”撤进平城之后，汉朝的救兵也赶到了，匈奴的骑兵这才解围而去。汉朝也收兵而归。韩王信为匈奴人带兵往来在边境一带攻击汉军。史称白登之围事件 [18]  ，从此以后，汉朝采用和亲政策，以婚姻和财宝换取帝国和平，于是，汉朝初期并没有什么战事，百姓得以休养生息。 [19] \n" +
            "刘邦死后，刘盈继位，即汉惠帝，但是在此期间，实际是吕后称制。吕后尊刘邦遗嘱用曹参为丞相，萧规曹随，沿用汉高祖刘邦的黄老政治的政策，达到了“政不出房户，天下晏然”的效果，为史家所称道，但吕后同时又任用外戚，压制功臣，酿成“诸吕之乱”。 [20] \n" +
            "文景之治\n" +
            "汉文帝刘恒\n" +
            "汉文帝刘恒 [21]\n" +
            "参见：文景之治、七国之乱、南越国\n" +
            "吕后死后，诸吕之乱被以周勃为领袖的大臣铲除，众臣迎立汉文帝。 [22]  在汉文帝的皇后窦漪房的影响下文帝和儿子汉景帝即位期间，继续采取黄老无为而治的手段，实行轻徭薄赋、与民休息的政策，恩威并施，恢复了多年战争带来的巨大破坏，使人民负担得到减轻； [23]  虽然汉景帝时期发生了此时期唯一的动乱—“七国之乱”，但是仅经历三个月便为周亚夫、栾布所平定，并未对汉朝带来实质影响。 [24] \n" +
            "这段时期，匈奴虽然几次入寇中原，但大多数时间里处于相对和平的状态。汉朝方面则不断积蓄国力，透过一系列措施来积极备战。这一时期史称“文景之治”，是中国大一统王朝的第一个治世，被后世历史学家称羡。 [25] \n" +
            "汉武时期\n" +
            "汉武帝画像\n" +
            "汉武帝画像 [26]\n" +
            "参见：罢黜百家，独尊儒术、酎金夺爵、盐铁官营、汉武盛世、汉匈百年战争、丝绸之路、巫蛊之祸\n" +
            "汉景帝死后，其子刘彻即位，即为汉武帝。刘彻在位期间采取了一系列改革措施，锐意进取，开疆拓土。 [27] \n" +
            "在政治上，汉武帝加强皇权，首创年号，采纳主父偃的建议，施行推恩令，削弱了诸侯王的势力，从此，诸侯王的势力不再能够对中央构成威胁；后又以诸侯献上的黄金成色不纯为由，取消了百余位列侯的爵位，即史书上所称的“酎金失侯”事件。经此二次事件后，中央集权得到了大大的加强。 [28] \n" +
            "文化上，废除了汉朝以“黄老学说、无为而治”治国的思想，积极治国；并采纳董仲舒的建议，开始重用儒术。尽管刘彻时期兼用儒、法、道、阴阳、纵横等各家人才，汉朝也一直采取集合霸道、王道的治国方针，但汉武帝对儒家的推崇，使儒家思想得到重视，并在以后逐渐成为中国历经二千年的主流思想。 [28] \n" +
            "张骞拜别汉武帝出使西域\n" +
            "张骞拜别汉武帝出使西域\n" +
            "军事上，积极对付汉朝的最大外患—匈奴。汉武帝时期大幅提高军人的待遇，在巡视北方时，一次犒赏边防军就达100万匹丝绸和200万钱。这期间汉朝先后出现了卫青、霍去病、李广等优秀将领，终于击溃匈奴，修建外长城之光禄塞、居延塞，收复河套并将河西纳入版图，促使“漠南无王庭”的局面，又先后吞并南越、闽越、夜郎、滇国、卫满朝鲜等国，远征大宛降服西域诸国，使中国成为当时世界上首屈一指的强国。汉武帝时期奠定了汉地范围，也是汉朝走向强盛的重要时期。 [29] \n" +
            "外交上，两次派张骞出使西域，开辟了丝绸之路。并先后以两位公主刘细君，刘解忧和亲西域乌孙，而达到了离间西域和匈奴，进而控制西域的目的，并开通了长安到中亚的丝绸之路，丝绸之路成为东西方经济文化交流的桥梁。 [30] \n" +
            "昭宣中兴\n" +
            "汉昭帝刘弗陵\n" +
            "汉昭帝刘弗陵\n" +
            "参见：霍光辅政、盐铁论、昭宣中兴、西域都护\n" +
            "汉武帝晚年，发生了著名的“巫蛊之祸”，太子刘据因此冤死。汉朝经历多年对外战争，对经济产生巨大冲击，导致汉朝国力衰弱，前朝积蓄被挥霍殆尽。为此，汉武帝晚年曾发表著名的轮台之诏，不再穷兵黩武。为挽救经济，汉武帝在位期间曾采取一系列政策，将铸币、盐铁收归中央管理，加强农业生产，实行和籴法，开凿白渠，并创立均输、平准政策稳定物价，加强对经济的控制。 [31] \n" +
            "刘彻死后，年仅7岁的刘弗陵即位，是为汉昭帝。汉昭帝登基之初，由上官桀、金日磾、田千秋、桑弘羊和霍光5人共同辅政。但是在元凤元年（前80年），爆发元凤政变，汉昭帝清醒地诛杀了上官桀等一批阴谋权臣，避免了霍光被冤杀。霍光得以继续辅佐汉昭帝治国，史称霍光辅政。 [32] \n" +
            "汉宣帝刘询\n" +
            "汉宣帝刘询\n" +
            "霍光遵循汉武帝晚年的国策，对内继续休养生息，得以让百姓安居乐业，四海清平。汉昭帝死后，汉武帝孙昌邑王刘贺即位，史称汉废帝，因行为放纵，被霍光所废 [33]  ，后霍光又迎立汉宣帝刘询即位 [34]  。地节二年（前68年），霍光去世后，汉宣帝方亲理政事。 [35]  后因霍氏一门飞扬跋扈，汉宣帝将意图谋反的霍氏戚族一网打尽。 [32]  [36] \n" +
            "汉宣帝治国摒弃不实际的儒学，采取道法结合的治国方针 [37]  ，在位期间关心民间疾苦并借公田来安置流民 [38]  ，时常派遣官吏巡查民生以此减免赋税赈济受灾百姓 [39]  ，又设置常平仓供应边塞军需及平衡粮价 [40]  ，并多次下诏扶助鳏、寡、孤、独、高龄老人、贫困百姓等人群 [41-43]  。经汉宣帝治理，国家经济得到恢复，国势达到西汉极盛，四夷宾服、万邦来朝，使汉朝再度迎来了盛世，史称“孝宣之治” [44]  。\n" +
            "神爵二年（前60年），汉宣帝于西域乌垒城置西域都护府，汉廷政令得以颁行于西域。 [45]  汉宣帝时期，匈奴进一步衰落和分裂，南匈奴臣服于汉朝。 [46]  汉元帝建昭三年（前36年），北匈奴郅支单于被陈汤斩杀 [47]  ，并发出”明犯强汉者，虽远必诛！“的时代强音 [48]  ，自此汉匈战争告一段落。 [49] \n" +
            "新莽篡汉\n" +
            "新朝建立者王莽\n" +
            "新朝建立者王莽 [50]\n" +
            "参见：王莽篡汉、新朝、王莽改制\n" +
            "汉宣帝死后，汉元帝刘奭即位，西汉开始走向衰败。汉元帝，柔仁好儒，导致皇权旁落，外戚与宦官势力兴起。汉元帝死后，汉成帝刘骜即位。汉成帝好女色，先后宠爱许皇后、班婕妤和赵氏姐妹（赵飞燕、赵合德），由于赵氏姐妹不能生育，汉成帝与其他妃嫔的子女均为赵飞燕姐妹残害杀死，史称“燕啄皇孙”。由于“酒色侵骨”，汉成帝最后竟死在温柔乡中。汉成帝不理朝政，为外戚王氏集团的兴起提供了条件，皇太后王政君权力急剧膨胀。汉成帝死后，刘欣即位，是为汉哀帝。汉哀帝有断袖之癖，与男宠董贤情深一世却也疏于朝政。外戚王氏的权力进一步膨胀。 [51] \n" +
            "国家已经呈现一片末世之象，民间“再受命”说法四起。公元前1年8月15日，汉哀帝去世。8月17日，太皇太后王政君派王莽接替董贤成为大司马，并迎接刘衎。10月17日，刘衎即位，是为汉平帝。但是，汉平帝已经沦为王莽的傀儡。公元6年2月3日，年仅14岁的汉平帝病死，王莽立仅两岁的刘婴为太子，自任“摄皇帝”。公元8年十二月，王莽废除孺子婴的皇太子之位，建立新朝，西汉灭亡。 [52] \n" +
            "光武中兴\n" +
            "东汉的建立者汉光武帝刘秀\n" +
            "东汉的建立者汉光武帝刘秀\n" +
            "参见：绿林军、赤眉军、东汉统一战争、光武中兴\n" +
            "公元23年，新莽在赤眉、绿林民变下覆灭。绿林军拥立汉宗室刘玄作皇帝，恢复汉朝国号，史称玄汉，改元更始，刘玄即更始帝。公元25年，赤眉军立刘盆子为帝，沿袭汉朝国号，史称赤眉汉，建元建世，刘盆子即建世帝，随后击败绿林军。 [53] \n" +
            "公元25年，原本服从更始帝的汉室宗亲刘秀在鄗县 [54]  之南称帝，诛杀刘玄，登基称帝，为汉光武帝，沿用汉朝国号，称建武元年 [55]  ，定都洛阳（今河南洛阳），史称东汉。光武帝定关中，降铜马，灭赤眉，后又消灭隗嚣、公孙述等割据势力，实现了全国统一。 [56] \n" +
            "光武帝废王莽弊政，大兴儒学，使得东汉成为风化最美，儒学最盛的朝代。时年社会安定，加强中央集权，对外戚严加限制，史称“光武中兴” [57]  。 [58] \n" +
            "明章之治\n" +
            "汉明帝刘庄\n" +
            "汉明帝刘庄\n" +
            "参见：明章之治、汉传佛教、白虎观会议、班超经营西域\n" +
            "汉明帝和汉章帝在位期间，东汉进入国力恢复期，史称“明章之治”。永平十六年（73年），汉明帝遣窦固率军攻伐北匈奴，窦固大破呼衍王于天山并占据伊吾 [59]  ，因此西域各国皆遣子入侍，第二年复置西域都护府，遂“西域自绝六十五载，乃复通焉”。 [60]  然而不久焉耆、龟兹复叛攻陷西域都护府，汉章帝即位后，不欲因西域疲敝中国而罢遣都护 [61]  。 [62] \n" +
            "汉章帝刘炟\n" +
            "汉章帝刘炟\n" +
            "章和二年（88年）十月，车骑将军窦宪领军出塞，大破北匈奴，登燕然山刻石勒功而还，史称燕然勒功。 [63]  汉和帝永元三年（91年）窦宪再次率军出击，出塞五千里进攻金微山，大破北单于主力，北匈奴仓皇逃窜不知所踪。 [64]  章帝后期，外戚窦氏日益跋扈，揭开东汉后期戚宦之争的序幕。 [65] \n" +
            "位于今云南、缅甸北部一带的哀牢国 [66]  于永平十二年（69年）内附中国，汉明帝于其地设永昌郡 [67]  ，初步奠定了中国对整个云南的统治，使古哀牢境内的各民族从此融入中国。\n" +
            "明、章两帝共统治三十一年（57年—88年），秉承光武帝遗规，对外戚勋臣严加防范；屡下诏招抚流民，赈济鳏寡孤独和贫民前后凡九次； [68-72]  修治汴渠完成，消除西汉平帝以来河汴决坏；经营西域，再断匈奴右臂，复置西域都护府和戊己校尉。史载“天下安平，百姓殷富” [73]  ，号称“明章之治”。 [74-75]";
}
