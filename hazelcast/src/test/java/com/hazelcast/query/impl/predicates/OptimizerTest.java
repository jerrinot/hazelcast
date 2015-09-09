package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.Visitable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class OptimizerTest {
    public static final String query1 = "disabled_for_upload = false " +
            " AND ( media_id != -1 AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 " +
            " AND ( ( media_content_approval_instruction != 3 AND content_approval_media_state != 19 AND content_approval_media_state != 17) " +
            " OR ( media_clock_approval_instruction != 31 AND clock_approval_media_state != 41) OR ( media_instruction_approval_instruction != 51 " +
            " AND ( num_instruction_approval_not_performed > 0 OR num_instruction_rejected > 0)) OR ( media_class_approval_instruction != 41 " +
            " AND class_approval_media_state != 71) OR ( media_house_nr = '' AND media_house_nr_instruction = 10)) AND ( content_approval_media_state != 18 " +
            " AND clock_approval_media_state != 42 AND class_approval_media_state != 72 AND num_instruction_rejected = 0 AND qc_approval_media_state != 22 " +
            " AND approval_advert_state != 11) AND ( aired = false) AND ( approval_advert_state = 10 OR approval_advert_state = 9) AND ( NOT ( time_delivered <= 0 " +
            " AND stopped = false AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 AND ( content_approval_media_state = 19 " +
            " OR content_approval_media_state = 17 OR media_content_approval_instruction != 1) AND ( class_approval_media_state = 71 OR media_class_approval_instruction != 40) " +
            "AND ( clock_approval_media_state = 41 OR media_clock_approval_instruction != 30) " +
            "AND ( media_instruction_approval_instruction != 50 OR ( num_instruction_approval_not_performed = 0 AND num_instruction_rejected = 0)) " +
            "AND ( approval_advert_state = 10 OR approval_advert_state = 9) AND ( media_house_nr != '' OR media_house_nr_instruction = 11))) " +
            "AND ( media_id = 73474404 OR media_id = 59673386 OR media_id = 59673555 " +
            "OR media_id = 60386838 OR media_id = 60386991 OR media_id = 60387038 OR media_id = 63063042 OR media_id = 71930797 OR media_id = 71930974 OR media_id = 71931106 " +
            "OR media_id = 78158138 OR media_id = 121 OR media_id = 122 OR media_id = 123 OR media_id = 74724915 OR media_id = 59791484 OR media_id = 60474387 " +
            "OR media_id = 60995088 OR media_id = 62718106 OR media_id = 63355282 OR media_id = 63355295 OR media_id = 63803353 OR media_id = 64695309 OR media_id = 67657712 " +
            "OR media_id = 72587686 OR media_id = 62017377 ))" +
            "OR media_id = 64854099 OR media_id = 67102704 OR media_id = 67102758 OR media_id = 67102823 OR media_id = 67102923 OR media_id = 67102991 OR media_id = 67409968 " +
            "OR media_id = 67425958 OR media_id = 69581739 OR media_id = 69902352 OR media_id = 77041794 OR media_id = 77042317 OR media_id = 77042419 OR media_id = 77042607 " +
            "OR media_id = 77133328 OR media_id = 77133445 OR media_id = 77133519 OR media_id = 140 OR media_id = 3264387 OR media_id = 3264455 OR media_id = 3264523 " +
            "OR media_id = 65982154 OR media_id = 65982356 OR media_id = 141 OR media_id = 58721638 OR media_id = 59698851 OR media_id = 59698923 OR media_id = 59805097 " +
            "OR media_id = 59805166 OR media_id = 59805235 OR media_id = 59805304 OR media_id = 66225057 OR media_id = 71930557 OR media_id = 71930578 OR media_id = 55097116 " +
            "OR media_id = 55101161 OR media_id = 55103652 OR media_id = 55106387 OR media_id = 62046263 OR media_id = 154 OR media_id = 59738411 OR media_id = 59738458 " +
            "OR media_id = 59738505 OR media_id = 64397729 OR media_id = 64897053 OR media_id = 66485631 OR media_id = 68213411 OR media_id = 23 OR media_id = 24 " +
            "OR media_id = 25 OR media_id = 26 OR media_id = 27 OR media_id = 60505044 OR media_id = 63293532 OR media_id = 68582738 OR media_id = 71916462 " +
            "OR media_id = 63294136 OR media_id = 72 OR media_id = 73 OR media_id = 74 OR media_id = 75 OR media_id = 76 OR media_id = 77 OR media_id = 78 OR media_id = 79 " +
            "OR media_id = 80 OR media_id = 55874320 OR media_id = 59809236 OR media_id = 61011675 OR media_id = 61886419 OR media_id = 64693948 OR media_id = 67322242 " +
            "OR media_id = 68273046 OR media_id = 70072636 OR media_id = 74534152 OR media_id = 76342761 OR media_id = 4294273 OR media_id = 67387111 OR media_id = 60362913 " +
            "OR media_id = 68253459 OR media_id = 60362914 OR media_id = 67820 OR media_id = 64658131 OR media_id = 68384425 OR media_id = 71398673 OR media_id = 62542181 " +
            "OR media_id = 3263119 OR media_id = 59786652 OR media_id = 59786673 OR media_id = 16128 OR media_id = 55831870 OR media_id = 58381550 OR media_id = 59949741 " +
            "OR media_id = 59820961 OR media_id = 60422086 OR media_id = 60422095 OR media_id = 60422104 OR media_id = 65113499 OR media_id = 68280362 OR media_id = 61162521 " +
            "OR media_id = 62827656 OR media_id = 63150848 OR media_id = 63152006 OR media_id = 61190448 OR media_id = 62017376 OR media_id = 64854196 OR media_id = 65709715 " +
            "OR media_id = 68039591 OR media_id = 62149524 OR media_id = 62149535 OR media_id = 62259450 OR media_id = 62259459 OR media_id = 62307645 OR media_id = 62325686 " +
            "OR media_id = 62470947 OR media_id = 62506916 OR media_id = 63528051 OR media_id = 66406223 OR media_id = 66406245 OR media_id = 66406261 OR media_id = 66406271 " +
            "OR media_id = 67529080 OR media_id = 63592565 OR media_id = 67160305 OR media_id = 67160316 OR media_id = 67160341 OR media_id = 67160345 OR media_id = 67160347 " +
            "OR media_id = 67160362 OR media_id = 67274251 OR media_id = 70194680 OR media_id = 63593656 OR media_id = 64854124 OR media_id = 67551125 OR media_id = 67596488 " +
            "OR media_id = 67643586 OR media_id = 67643600 OR media_id = 67643614 OR media_id = 67643628 " +
            "OR media_id = 75213963 OR media_id = 75213996 OR media_id = 61905080 OR media_id = 63817262 OR media_id = 64205490 OR media_id = 64944725 OR media_id = 64944738 " +
            "OR media_id = 64944751 OR media_id = 66378271 " +
            "OR media_id = 66759156 OR media_id = 22 OR media_id = 64572354 OR media_id = 64631762 OR media_id = 75336027 OR media_id = 75336297 OR media_id = 75336471 " +
            "OR media_id = 74807721 OR media_id = 64792267 OR media_id = 64794726 OR media_id = 65450790 OR media_id = 65450809 OR media_id = 65450896 OR media_id = 66333933 " +
            "OR media_id = 64858827 OR media_id = 67565798 OR media_id = 69945741 OR media_id = 73766959 OR media_id = 73767049 OR media_id = 73767115 OR media_id = 73767510 " +
            "OR media_id = 64975618 OR media_id = 75669260 OR media_id = 65040105 OR media_id = 67428493 OR media_id = 65879385 OR media_id = 65196601 OR media_id = 65196604 " +
            "OR media_id = 65196605 OR media_id = 65196606 OR media_id = 68039954 OR media_id = 68040073 OR media_id = 68040115 OR media_id = 65302433 OR media_id = 65850658 " +
            "OR media_id = 65850673 OR media_id = 66274728 OR media_id = 66276361 OR media_id = 66276370 OR media_id = 74834083 OR media_id = 74834171 OR media_id = 74834201 " +
            "OR media_id = 74834241 OR media_id = 74834291 OR media_id = 74834354 OR media_id = 74834395 OR media_id = 74834538 OR media_id = 74834598 OR media_id = 74835532 " +
            "OR media_id = 65337986 OR media_id = 65337988 OR media_id = 65337989 OR media_id = 65337990 OR media_id = 65337991 OR media_id = 65337992 OR media_id = 65337993 " +
            "OR media_id = 65850942 OR media_id = 65881262 OR media_id = 65881267 OR media_id = 65881272 OR media_id = 65881277 OR media_id = 65881282 OR media_id = 65881287 " +
            "OR media_id = 65881292 OR media_id = 65881297 OR media_id = 65881302 OR media_id = 65881307 OR media_id = 65881312 OR media_id = 68310062 OR media_id = 68332854 " +
            "OR media_id = 74911907 OR media_id = 74912040 OR media_id = 65337995 OR media_id = 65337997 OR media_id = 65337998 OR media_id = 65338000 OR media_id = 65338001 " +
            "OR media_id = 65338002 OR media_id = 65338003 OR media_id = 65850988 OR media_id = 65882368 OR media_id = 65882373 OR media_id = 65882378 OR media_id = 65882383 " +
            "OR media_id = 65882388 OR media_id = 65882393 OR media_id = 65882398 OR media_id = 75829398 OR media_id = 75845412 OR media_id = 75845575 OR media_id = 76234605 " +
            "OR media_id = 65338004 OR media_id = 65851648 OR media_id = 65851684 OR media_id = 65851685 OR media_id = 65851686 OR media_id = 65851813 OR media_id = 65851843 " +
            "OR media_id = 65851872 OR media_id = 65851895 OR media_id = 65851917 OR media_id = 65338006 OR media_id = 65338008 OR media_id = 65338010 OR media_id = 65338011 " +
            "OR media_id = 65338012 OR media_id = 65874880 OR media_id = 65874901 OR media_id = 65874977 OR media_id = 65874998 OR media_id = 65875019 OR media_id = 65875040 " +
            "OR media_id = 65875061 OR media_id = 65875082 OR media_id = 65338013 OR media_id = 65338015 OR media_id = 65963730 OR media_id = 65963733 OR media_id = 70583780 " +
            "OR media_id = 65338016 OR media_id = 70685072 OR media_id = 70685076 OR media_id = 70685091 OR media_id = 70685103 OR media_id = 72415535 OR media_id = 72433212 " +
            "OR media_id = 65338020 OR media_id = 65534628 OR media_id = 65554016 " +
            "OR media_id = 65604907 OR media_id = 65338018 OR media_id = 65659673 OR media_id = 72587634 OR media_id = 72605951 OR media_id = 72650996 OR media_id = 74709627 " +
            "OR media_id = 74709807 OR media_id = 65674276 OR media_id = 65736689 OR media_id = 65737363 OR media_id = 65737366 OR media_id = 65737369 OR media_id = 65737372 " +
            "OR media_id = 65737375 OR media_id = 65737378 OR media_id = 65737381 OR media_id = 65737384 OR media_id = 65737387 OR media_id = 65737390 OR media_id = 65773569 " +
            "OR media_id = 65773585 OR media_id = 65773678 OR media_id = 65773714 OR media_id = 65773843 OR media_id = 66083607 OR media_id = 66083608 OR media_id = 66083609 " +
            "OR media_id = 66083622 OR media_id = 66710880 OR media_id = 65707021 OR media_id = 65773893 OR media_id = 65773895 OR media_id = 65773901 OR media_id = 67051047 " +
            "OR media_id = 67051052 OR media_id = 65794928 OR media_id = 65796339 OR media_id = 73908735 OR media_id = 74030147 OR media_id = 65796340 OR media_id = 65796335 " +
            "OR media_id = 65985101 OR media_id = 65985102 OR media_id = 65985103 OR media_id = 76797952 OR media_id = 66024159 OR media_id = 66044592 OR media_id = 66055819 " +
            "OR media_id = 66097555 OR media_id = 66102654 OR media_id = 66137767 OR media_id = 77463679 OR media_id = 77463786 OR media_id = 62677964 OR media_id = 65338022 " +
            "OR media_id = 66251776 OR media_id = 66395527 OR media_id = 66409945 OR media_id = 66409952 OR media_id = 66409953 OR media_id = 71933405 OR media_id = 71933426 " +
            "OR media_id = 71933441 OR media_id = 71933455 OR media_id = 66453829 OR media_id = 66453820 OR media_id = 66453763 OR media_id = 66453803 OR media_id = 66478667 " +
            "OR media_id = 66482243 OR media_id = 66969501 OR media_id = 66969504 OR media_id = 66969507 OR media_id = 66969510 OR media_id = 66969513 OR media_id = 66969516 " +
            "OR media_id = 66969519 OR media_id = 66969522 OR media_id = 66506289 OR media_id = 77375180 OR media_id = 66547927 OR media_id = 67159789 OR media_id = 67159825 " +
            "OR media_id = 67159835 OR media_id = 67159838 OR media_id = 67159871 OR media_id = 67159874 OR media_id = 67159876 OR media_id = 67159898 OR media_id = 67159929 " +
            "OR media_id = 67159938 OR media_id = 67159943 OR media_id = 72637275 OR media_id = 72639549 OR media_id = 72901029 OR media_id = 66580210 OR media_id = 66583633 " +
            "OR media_id = 66584426 OR media_id = 66605440 OR media_id = 67160020 OR media_id = 67160042 OR media_id = 67160071 OR media_id = 67160076 OR media_id = 67160101 " +
            "OR media_id = 67160108 OR media_id = 67160147 OR media_id = 67160158 OR media_id = 67160159 OR media_id = 67160162 OR media_id = 67160188 OR media_id = 67160205 " +
            "OR media_id = 67160246 OR media_id = 70183281 OR media_id = 66633452 OR media_id = 73752125 OR media_id = 73752137 OR media_id = 66768586 OR media_id = 72602322 " +
            "OR media_id = 67736420 OR media_id = 67158341 OR media_id = 67158459 OR media_id = 67158499 OR media_id = 67158504 OR media_id = 67158517 OR media_id = 67158530 " +
            "OR media_id = 67158548 OR media_id = 67158569 OR media_id = 67158574 OR media_id = 67158580 OR media_id = 67158612 OR media_id = 67158613 OR media_id = 67158614 " +
            "OR media_id = 67158618 OR media_id = 67158624 OR media_id = 67158625 OR media_id = 67158626 OR media_id = 67158627 OR media_id = 67158633 OR media_id = 67158634 " +
            "OR media_id = 67158643 OR media_id = 67158658 OR media_id = 70178075 OR media_id = 70178156 OR media_id = 70178579 OR media_id = 70178702 OR media_id = 70178818 " +
            "OR media_id = 70179118 OR media_id = 71308307 OR media_id = 71308315 OR media_id = 71308509 OR media_id = 71308552 OR media_id = 71308608 OR media_id = 71308642 " +
            "OR media_id = 71308657 OR media_id = 71308664 OR media_id = 71308787 OR media_id = 71308856 OR media_id = 71308890 OR media_id = 67158751 OR media_id = 67158782" +
            " OR media_id = 70142883 OR media_id = 67158855 OR media_id = 67158868 OR media_id = 67158897 OR media_id = 67158900 OR media_id = 67158925 OR media_id = 67158933 " +
            "OR media_id = 67158974 OR media_id = 67159044 OR media_id = 67159084 OR media_id = 67159099 OR media_id = 67159100 OR media_id = 67159111 OR media_id = 67159139 " +
            "OR media_id = 67159140 OR media_id = 67159141 OR media_id = 67159193 OR media_id = 67159214 OR media_id = 67159233 OR media_id = 67159273 OR media_id = 67159312 " +
            "OR media_id = 67159341 OR media_id = 67159360 OR media_id = 67159387 OR media_id = 67159394 OR media_id = 67159454 OR media_id = 67159455 OR media_id = 67159477 " +
            "OR media_id = 67159481 OR media_id = 67159498 OR media_id = 67159500 OR media_id = 67159519 OR media_id = 70193971 OR media_id = 70193981 OR media_id = 70194005 " +
            "OR media_id = 70194017 OR media_id = 70194043 OR media_id = 70194050 OR media_id = 67159570 " +
            "OR media_id = 67159594 OR media_id = 67159619 OR media_id = 67159639 OR media_id = 72465152 OR media_id = 67159661 OR media_id = 67159683 OR media_id = 67159690 " +
            "OR media_id = 67159702 OR media_id = 67159703 OR media_id = 67359839 OR media_id = 163 OR media_id = 65337999 OR media_id = 65721879 OR media_id = 66816650 " +
            "OR media_id = 67158392 OR media_id = 67340591 OR media_id = 67354125 OR media_id = 67365772 OR media_id = 68559245 OR media_id = 67431261 OR media_id = 72221593 " +
            "OR media_id = 72221630 " +
            "OR media_id = 67431324 OR media_id = 67431382 OR media_id = 72217459 OR media_id = 72217770 OR media_id = 72218008 OR media_id = 72218102 OR media_id = 72218199 " +
            "OR media_id = 67431437 OR media_id = 67431452 OR media_id = 67431531 OR media_id = 72242828 OR media_id = 72242874 OR media_id = 67431797 OR media_id = 72243527 " +
            "OR media_id = 72243577 OR media_id = 72243629 OR media_id = 72243678 OR media_id = 67431831 " +
            "OR media_id = 72195744 OR media_id = 72195853 OR media_id = 72196028 OR media_id = 72196066 OR media_id = 67461770 OR media_id = 67563780 OR media_id = 67579821 " +
            "OR media_id = 67672792 OR media_id = 67683881 OR media_id = 67693687 OR media_id = 67805875 OR media_id = 67805883 OR media_id = 67805891 OR media_id = 67805899 " +
            "OR media_id = 67805907 OR media_id = 67805915 OR media_id = 67805923 OR media_id = 67805931 OR media_id = 67805939 OR media_id = 67805947 OR media_id = 67805955 " +
            "OR media_id = 67805963 OR media_id = 67805979 OR media_id = 67805987 OR media_id = 67805995 OR media_id = 67806003 OR media_id = 67806019 OR media_id = 67806027 " +
            "OR media_id = 67806035 OR media_id = 67806043 OR media_id = 67806051 OR media_id = 67806059 OR media_id = 67806067 OR media_id = 67806083 OR media_id = 67806091 " +
            "OR media_id = 67806099 OR media_id = 67806107 OR media_id = 67806115 OR media_id = 67806123 OR media_id = 67806143 OR media_id = 67806151 OR media_id = 67806159 " +
            "OR media_id = 67806167 OR media_id = 67806175 OR media_id = 67806183 OR media_id = 67806191 OR media_id = 67806199 OR media_id = 67806207 OR media_id = 67806215 " +
            "OR media_id = 67806223 OR media_id = 67806231 OR media_id = 67806239 OR media_id = 67806255 OR media_id = 67806264 OR media_id = 67806272 OR media_id = 67806280 " +
            "OR media_id = 67806288 OR media_id = 67806296 OR media_id = 67806308 OR media_id = 67806317 OR media_id = 67693692 OR media_id = 68073856 OR media_id = 68073864 " +
            "OR media_id = 68073870 OR media_id = 68073874 OR media_id = 68073878 OR media_id = 68073882 OR media_id = 68073890 OR media_id = 68073894 OR media_id = 68073898 " +
            "OR media_id = 68073906 OR media_id = 68074823 OR media_id = 68074827 OR media_id = 68074831 OR media_id = 68074835 OR media_id = 68074839 OR media_id = 68074843 " +
            "OR media_id = 68074847 OR media_id = 68074851 OR media_id = 68074855 OR media_id = 68074859 OR media_id = 68074863 " +
            "OR media_id = 68074867 OR media_id = 68074875 OR media_id = 68074880 OR media_id = 68074888 OR media_id = 68074892 OR media_id = 68074896 OR media_id = 68074900 " +
            "OR media_id = 68074904 OR media_id = 68074908 OR media_id = 68074916 OR media_id = 68074923 OR media_id = 68074927 OR media_id = 68074931 OR media_id = 68074935 " +
            "OR media_id = 68074939 OR media_id = 68074943 OR media_id = 68074947 OR media_id = 68074952 OR media_id = 68074956 OR media_id = 68074960 OR media_id = 68074964 " +
            "OR media_id = 68074968 OR media_id = 68074972 OR media_id = 68074976 OR media_id = 68074980 OR media_id = 68074984 OR media_id = 68074988 OR media_id = 68074992 " +
            "OR media_id = 68074996 OR media_id = 68075005 OR media_id = 68075009 OR media_id = 68075013 OR media_id = 68075022 OR media_id = 71850153 OR media_id = 71850203 " +
            "OR media_id = 71850253 OR media_id = 71850303 OR media_id = 71850376 OR media_id = 71850426 OR media_id = 71850476 OR media_id = 71850547 OR media_id = 71850615 " +
            "OR media_id = 71850674 OR media_id = 71850724 OR media_id = 71850788 OR media_id = 71850853 OR media_id = 71850903 OR media_id = 71850953 OR media_id = 71851003 " +
            "OR media_id = 71851053 OR media_id = 71851103 OR media_id = 71851166 OR media_id = 71851234 OR media_id = 71851290 OR media_id = 71851346 OR media_id = 71851444 " +
            "OR media_id = 71851514 OR media_id = 71851669 OR media_id = 71851735 OR media_id = 71851799 OR media_id = 74419469 OR media_id = 74419520 OR media_id = 74419571 " +
            "OR media_id = 74419623 OR media_id = 74419676 OR media_id = 67745571 OR media_id = 70191162 OR media_id = 70191169 OR media_id = 70191184 OR media_id = 70191193 " +
            "OR media_id = 70191216 OR media_id = 70191241 OR media_id = 70191270 OR media_id = 70191286 OR media_id = 70191299 OR media_id = 70191345 OR media_id = 70191363 " +
            "OR media_id = 70191402 OR media_id = 70191411 OR media_id = 70191418 OR media_id = 70191436 OR media_id = 70191456 OR media_id = 70191468 OR media_id = 70191479 " +
            "OR media_id = 70191507 OR media_id = 70191527 OR media_id = 70191548 OR media_id = 70191597 OR media_id = 70191604 OR media_id = 70191619 OR media_id = 70191630 " +
            "OR media_id = 70191650 OR media_id = 67776760 OR media_id = 67788426 OR media_id = 67788584 OR media_id = 69879017 OR media_id = 67796331 OR media_id = 67905775 " +
            "OR media_id = 67912168 OR media_id = 67912197 OR media_id = 67912453 OR media_id = 67912471 OR media_id = 67950430 OR media_id = 67978748 OR media_id = 67987385 " +
            "OR media_id = 67999220 OR media_id = 67988000 OR media_id = 67988036 OR media_id = 67988117 OR media_id = 68022716 OR media_id = 68106126 OR media_id = 68073886 " +
            "OR media_id = 68073902 OR media_id = 68074871 OR media_id = 68074912 OR media_id = 68075038 OR media_id = 68074884 OR media_id = 68075026 OR media_id = 68075030 " +
            "OR media_id = 68075043 OR media_id = 68075034 OR media_id = 67805971 OR media_id = 67806011 OR media_id = 67806247 OR media_id = 67806325 OR media_id = 72504923 " +
            "OR media_id = 67806075 OR media_id = 68467080 OR media_id = 68565634 OR media_id = 68573928 OR media_id = 68574573 OR media_id = 68804326 OR media_id = 69109052 " +
            "OR media_id = 69294988 OR media_id = 69401711 OR media_id = 69401814 OR media_id = 69401871 OR media_id = 69500513 OR media_id = 69592775 OR media_id = 69642032 " +
            "OR media_id = 69784290 OR media_id = 70213927 OR media_id = 69843346 OR media_id = 70120660 OR media_id = 70190484 OR media_id = 70347554 OR media_id = 70377409 " +
            "OR media_id = 70613996 OR media_id = 72945395 OR media_id = 72945405 OR media_id = 72945432 OR media_id = 72945479 OR media_id = 72945584 OR media_id = 72945600 " +
            "OR media_id = 72945628 OR media_id = 74978656 OR media_id = 70628267 OR media_id = 70685034 OR media_id = 70718900 OR media_id = 77627190 OR media_id = 71098342 " +
            "OR media_id = 71244998 OR media_id = 73178776 OR media_id = 71316704 OR media_id = 71321832 OR media_id = 71963847 OR media_id = 72040872 OR media_id = 72235782 " +
            "OR media_id = 72235832 OR media_id = 72235917 OR media_id = 71331604 OR media_id = 72375211 OR media_id = 72427386 OR media_id = 72905164 OR media_id = 73761833 " +
            "OR media_id = 72947128 OR media_id = 72947184 OR media_id = 72947201 OR media_id = 72947160 OR media_id = 72947199 OR media_id = 72947395 OR media_id = 72947396 " +
            "OR media_id = 72947400 OR media_id = 72947478 OR media_id = 72947529 OR media_id = 72947530 OR media_id = 72947614 OR media_id = 72947616 OR media_id = 72947624 " +
            "OR media_id = 72947802 OR media_id = 72947809 OR media_id = 72947812 OR media_id = 72996147 OR media_id = 72996162 OR media_id = 73004815 OR media_id = 73061075 " +
            "OR media_id = 73063418 OR media_id = 73079429 OR media_id = 73099344 OR media_id = 73099372 OR media_id = 73099403 OR media_id = 73212162 OR media_id = 73212550 " +
            "OR media_id = 73212799 OR media_id = 73217399 OR media_id = 73223455 OR media_id = 73507212 OR media_id = 74801728 OR media_id = 77812797 OR media_id = 73474405 " +
            "OR media_id = 73474610 OR media_id = 73566981 OR media_id = 73571162 OR media_id = 74084976 OR media_id = 74084991 OR media_id = 74084997 OR media_id = 72079854 " +
            "OR media_id = 73764305 OR media_id = 75284108 OR media_id = 75286077 OR media_id = 75286108 OR media_id = 75286132 OR media_id = 75286173 OR media_id = 75286185 " +
            "OR media_id = 75286211 OR media_id = 75286241 OR media_id = 75286289 OR media_id = 75286344 OR media_id = 75286365 OR media_id = 73838089 OR media_id = 75398136 " +
            "OR media_id = 77423959 OR media_id = 73905637 OR media_id = 76568336 OR media_id = 76568785 OR media_id = 73913296 OR media_id = 74000395 OR media_id = 74002079 " +
            "OR media_id = 74590657 OR media_id = 74590766 " +
            "OR media_id = 74593744 OR media_id = 74593895 OR media_id = 74002394 OR media_id = 74371744 OR media_id = 74000105 OR media_id = 74235244 OR media_id = 75312461 " +
            "OR media_id = 75312570 OR media_id = 75312665 OR media_id = 75313030 OR media_id = 75313046 OR media_id = 75313071 OR media_id = 75313117 OR media_id = 74253324 " +
            "OR media_id = 74253634 OR media_id = 74253677 OR media_id = 74253719 OR media_id = 74253764 OR media_id = 74253806 OR media_id = 74272248 OR media_id = 74419103 " +
            "OR media_id = 77555016 OR media_id = 77555471 OR media_id = 74420758 OR media_id = 74442634 OR media_id = 74490584 OR media_id = 74541955 OR media_id = 74567250 " +
            "OR media_id = 74590739 OR media_id = 75394982 OR media_id = 74052206 OR media_id = 74594869 " +
            "OR media_id = 74595311 OR media_id = 74595403 OR media_id = 74595440 OR media_id = 74595313 OR media_id = 74621788 OR media_id = 74625235 OR media_id = 74630644 " +
            "OR media_id = 74636212 OR media_id = 74636301 OR media_id = 74637427 OR media_id = 74639139 OR media_id = 74639397 OR media_id = 74639579 OR media_id = 74639628 " +
            "OR media_id = 74637660 OR media_id = 74640890 OR media_id = 74642843 OR media_id = 74643363 OR media_id = 74645836 OR media_id = 74659244 OR media_id = 74659428 " +
            "OR media_id = 74659441 OR media_id = 74659467 OR media_id = 74659510 OR media_id = 74659528 OR media_id = 74659534 OR media_id = 74659601 OR media_id = 74659606 " +
            "OR media_id = 74659658 OR media_id = 74659697 OR media_id = 74659748 OR media_id = 74659753 OR media_id = 74659794 OR media_id = 74659873 OR media_id = 74659927 " +
            "OR media_id = 74659960 OR media_id = 74659982 OR media_id = 74659994 OR media_id = 74660030 OR media_id = 74660073 OR media_id = 74661677 OR media_id = 74661691 " +
            "OR media_id = 74661731 OR media_id = 74661751 OR media_id = 74661777 OR media_id = 74661803 OR media_id = 74661815 OR media_id = 74661831 OR media_id = 74661856 " +
            "OR media_id = 74661879 OR media_id = 74661914 OR media_id = 74661923 OR media_id = 74661942 OR media_id = 74661967 OR media_id = 74662001 OR media_id = 74662027 " +
            "OR media_id = 74671215 OR media_id = 74671283 OR media_id = 74683708 OR media_id = 74697880 OR media_id = 74698734 OR media_id = 74698844 OR media_id = 74698880 " +
            "OR media_id = 74698884 OR media_id = 74698773 OR media_id = 74698835 OR media_id = 74698853 OR media_id = 74698903 OR media_id = 74704012 OR media_id = 74708079 " +
            "OR media_id = 74713977 OR media_id = 74739486 OR media_id = 74747655 OR media_id = 74748569 OR media_id = 74749592 OR media_id = 74750408 OR media_id = 74751287 " +
            "OR media_id = 74751817 OR media_id = 74752209 OR media_id = 74752535 OR media_id = 74774067 OR media_id = 74775324 OR media_id = 74776775 OR media_id = 74786859 " +
            "OR media_id = 74790362 OR media_id = 74791637 OR media_id = 74796013 OR media_id = 74798491 OR media_id = 74803881 OR media_id = 74804650 OR media_id = 74805428 " +
            "OR media_id = 74807744 OR media_id = 74818208 OR media_id = 74821348 OR media_id = 74835252 OR media_id = 74837163 OR media_id = 74837615 OR media_id = 74838031 " +
            "OR media_id = 74838310 OR media_id = 74838425 OR media_id = 74838451 OR media_id = 74838452 OR media_id = 74838453 OR media_id = 74838709 OR media_id = 74849263 " +
            "OR media_id = 74850040 OR media_id = 74855872 OR media_id = 74856839 OR media_id = 74858270 OR media_id = 74858333 OR media_id = 74859164 OR media_id = 74861046 " +
            "OR media_id = 74862190 OR media_id = 74863293 OR media_id = 74864241 OR media_id = 74864884 OR media_id = 74866583 OR media_id = 74867270 OR media_id = 74868103 " +
            "OR media_id = 74869043 OR media_id = 74869488 OR media_id = 74881587 OR media_id = 74883901 OR media_id = 74884476 OR media_id = 74886154 OR media_id = 74886732 " +
            "OR media_id = 74887270 OR media_id = 74887374 OR media_id = 74888085 OR media_id = 74888599 OR media_id = 74890653 OR media_id = 74891546 OR media_id = 74909601 " +
            "OR media_id = 74916572 OR media_id = 74916755 OR media_id = 74916901 OR media_id = 77383098 OR media_id = 74917045 OR media_id = 74921211 OR media_id = 74922363 " +
            "OR media_id = 74923870 OR media_id = 74924429 OR media_id = 74924867 OR media_id = 74925875 OR media_id = 74926384 OR media_id = 74926877 OR media_id = 74928050 " +
            "OR media_id = 74932127 OR media_id = 74932703 OR media_id = 74934419 OR media_id = 74936838 OR media_id = 74938660 OR media_id = 74939591 OR media_id = 74941393 " +
            "OR media_id = 74950499 OR media_id = 74962402 OR media_id = 74966064 OR media_id = 74987580 OR media_id = 74988384 OR media_id = 74990935 OR media_id = 74991072 " +
            "OR media_id = 74991777 OR media_id = 74992049 OR media_id = 74997168 OR media_id = 74998045 OR media_id = 75004735 OR media_id = 75006320 OR media_id = 75020595 " +
            "OR media_id = 75023538 OR media_id = 75032719 OR media_id = 75032776 OR media_id = 75032807 OR media_id = 75032831 OR media_id = 75032857 OR media_id = 75032881 " +
            "OR media_id = 75032913 OR media_id = 75032944 OR media_id = 75032990 OR media_id = 75033009 OR media_id = 75033055 OR media_id = 75033102 OR media_id = 75033121 " +
            "OR media_id = 75033162 OR media_id = 75033169 OR media_id = 75033186 OR media_id = 75033209 OR media_id = 75033237 OR media_id = 75033302 OR media_id = 75033346 " +
            "OR media_id = 75311748 OR media_id = 77620747 OR media_id = 75033219 OR media_id = 75035675 OR media_id = 75037348 OR media_id = 75037407 OR media_id = 75037422 " +
            "OR media_id = 75038292 OR media_id = 75038851 OR media_id = 75038866 OR media_id = 75039779 OR media_id = 75043923 OR media_id = 75045300 OR media_id = 75045329 " +
            "OR media_id = 75045336 OR media_id = 75045352 OR media_id = 75045422 OR media_id = 75045843 OR media_id = 75047177 OR media_id = 75047287 OR media_id = 75047316 " +
            "OR media_id = 75047488 OR media_id = 75047499 OR media_id = 75047609 OR media_id = 75047619 OR media_id = 75063268 OR media_id = 75064566 OR media_id = 75067901 " +
            "OR media_id = 75076032 OR media_id = 75077406 OR media_id = 75158180 OR media_id = 75190140 OR media_id = 75223710 OR media_id = 75223721 OR media_id = 75223827 " +
            "OR media_id = 75239526 OR media_id = 75243813 OR media_id = 75245507 OR media_id = 75246495 OR media_id = 75249495 OR media_id = 75254557 OR media_id = 75303535 " +
            "OR media_id = 75317257 OR media_id = 75328026 OR media_id = 75349698 OR media_id = 75350448 OR media_id = 75353536 OR media_id = 75353850 OR media_id = 75354056 " +
            "OR media_id = 75354146 OR media_id = 75354475 OR media_id = 75354540 OR media_id = 75354641 OR media_id = 75354734 OR media_id = 75354824 OR media_id = 75369788 " +
            "OR media_id = 75401129 OR media_id = 75472826 OR media_id = 75611397 OR media_id = 75719017 OR media_id = 75868382 OR media_id = 75877662 OR media_id = 75885329 " +
            "OR media_id = 67158807 OR media_id = 76052084 OR media_id = 75984761 OR media_id = 76061832 OR media_id = 76087186 OR media_id = 76089277 OR media_id = 76100067 " +
            "OR media_id = 76123363 OR media_id = 76135929 OR media_id = 76204709 OR media_id = 76207613 OR media_id = 76209052 OR media_id = 76215744 OR media_id = 76216408 " +
            "OR media_id = 76216699 OR media_id = 76217392 OR media_id = 76225285 OR media_id = 76240138 OR media_id = 76241488 OR media_id = 76247819 OR media_id = 76259765 " +
            "OR media_id = 76268165 OR media_id = 76937651 OR media_id = 76272665 OR media_id = 65963736 OR media_id = 76287940 " +
            "OR media_id = 76289167 OR media_id = 76295833 OR media_id = 76296903 OR media_id = 76297152 OR media_id = 76297195 OR media_id = 76313108 OR media_id = 76318792 " +
            "OR media_id = 76322396 OR media_id = 76323658 OR media_id = 76325936 OR media_id = 76330598 " +
            "OR media_id = 76342816 OR media_id = 76354126 OR media_id = 76355114 OR media_id = 76355957 OR media_id = 76357043 OR media_id = 76358382 OR media_id = 76359221 " +
            "OR media_id = 76359759 OR media_id = 76362377 OR media_id = 76362810 OR media_id = 76369064 OR media_id = 76369473 OR media_id = 76369956 OR media_id = 76370442 " +
            "OR media_id = 77411334 OR media_id = 76644631 OR media_id = 76859109 OR media_id = 76880590 OR media_id = 76984196 OR media_id = 77034877 OR media_id = 77272573 " +
            "OR media_id = 77002118 OR media_id = 77329827 OR media_id = 77398619 OR media_id = 77451493 OR media_id = 77737203 OR media_id = 77738835 OR media_id = 77739241 " +
            "OR media_id = 77739427 OR media_id = 77739505 OR media_id = 77739720 OR media_id = 77739805 OR media_id = 77739886 OR media_id = 77738667 OR media_id = 77740168 " +
            "OR media_id = 77740281 OR media_id = 77740319 OR media_id = 77740346 OR media_id = 77740353 OR media_id = 77740378 OR media_id = 77740404 OR media_id = 77740528 " +
            "OR media_id = 73178594 OR media_id = 74698037 OR media_id = 74698129 OR media_id = 76429778 OR media_id = 76657481 OR media_id = 76657564 OR media_id = 76657614 " +
            "OR media_id = 76657666 OR media_id = 76657718 OR media_id = 76657751 OR media_id = 76657800 OR media_id = 76657895 OR media_id = 77537226 OR media_id = 77561790 " +
            "OR media_id = 77623827 OR media_id = 77667332 OR media_id = 77755797 OR media_id = 77798123 OR media_id = 77831696 OR media_id = 77833893 OR media_id = 77948949 " +
            "OR media_id = 77962336 OR media_id = 77970497 OR media_id = 78178704))";

    public String query2 = "disabled_for_upload = false " + //Done
            " AND ( media_id != -1 AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 " + //Done
            " AND ( ( media_content_approval_instruction != 3 AND content_approval_media_state != 19 AND content_approval_media_state != 17) " + // Done
            " OR ( media_clock_approval_instruction != 31 AND clock_approval_media_state != 41) OR ( media_instruction_approval_instruction != 51 " + //Done
            " AND ( num_instruction_approval_not_performed > 0 OR num_instruction_rejected > 0)) OR ( media_class_approval_instruction != 41 " + //Done
            " AND class_approval_media_state != 71) OR ( media_house_nr = '' AND media_house_nr_instruction = 10)) AND ( content_approval_media_state != 18 " + //Done
            " AND clock_approval_media_state != 42 AND class_approval_media_state != 72 AND num_instruction_rejected = 0 AND qc_approval_media_state != 22 " + //Done
            " AND approval_advert_state != 11) AND ( aired = false) AND ( approval_advert_state = 10 OR approval_advert_state = 9) AND ( NOT ( time_delivered <= 0 " + //Done
            " AND stopped = false AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 AND ( content_approval_media_state = 19 " + //Done
            " OR content_approval_media_state = 17 OR media_content_approval_instruction != 1) AND ( class_approval_media_state = 71 OR media_class_approval_instruction != 40) " + //Done
            "AND ( clock_approval_media_state = 41 OR media_clock_approval_instruction != 30) " + //Done
            "AND ( media_instruction_approval_instruction != 50 OR ( num_instruction_approval_not_performed = 0 AND num_instruction_rejected = 0)) " + //Done
            "AND ( approval_advert_state = 10 OR approval_advert_state = 9) AND ( media_house_nr != '' OR media_house_nr_instruction = 11))) " + //Done
            "AND ( media_id > 60000000))";

    public static String query3 = "disabled_for_upload = false " +
            " AND media_id != -1 AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 " +
            " AND ( ( media_content_approval_instruction != 3 AND content_approval_media_state != 19 AND content_approval_media_state != 17) " +
            " OR ( media_clock_approval_instruction != 31 AND clock_approval_media_state != 41) OR ( media_instruction_approval_instruction != 51 " +
            " AND ( num_instruction_approval_not_performed > 0 OR num_instruction_rejected > 0)) OR ( media_class_approval_instruction != 41 " +
            " AND class_approval_media_state != 71) OR ( media_house_nr = '' AND media_house_nr_instruction = 10)) " +
            " AND ( content_approval_media_state != 18 " +
            " AND clock_approval_media_state != 42 AND class_approval_media_state != 72 AND num_instruction_rejected = 0 AND qc_approval_media_state != 22 " +
            " AND approval_advert_state != 11) " +
            " AND ( aired = false) " +
            " AND ( approval_advert_state = 10 OR approval_advert_state = 9) " +
            " AND ( NOT ( time_delivered <= 0 " +
            " AND stopped = false AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 AND ( content_approval_media_state = 19 " +
            " OR content_approval_media_state = 17 OR media_content_approval_instruction != 1) AND ( class_approval_media_state = 71 OR media_class_approval_instruction != 40) " +
            " AND ( clock_approval_media_state = 41 OR media_clock_approval_instruction != 30) " +
            " AND ( media_instruction_approval_instruction != 50 OR ( num_instruction_approval_not_performed = 0 AND num_instruction_rejected = 0)) " +
            " AND ( approval_advert_state = 10 OR approval_advert_state = 9) AND ( media_house_nr != '' OR media_house_nr_instruction = 11)))";




    @Test
    public void testComplexQuery() {
        Predicate p = new SqlPredicate(query2);
//        Predicate p = complexQueryAsPredicate();
        System.out.println("Original: " + query2);
        System.out.println("Before: \n" + p);
        p = optimize(p);
        System.out.println("After: \n" + p);
    }

    private Predicate optimize(Predicate p) {
        if (p instanceof Visitable) {
            p = ((Visitable) p).accept(new NotEliminatingVisitor());
        }
        if (p instanceof Visitable) {
            p = ((Visitable) p).accept(new FlatteningVisitor());
        }
        if (p instanceof Visitable) {
            p = ((Visitable) p).accept(new BetweenVisitor());
        }
        if (p instanceof Visitable) {
            p = ((Visitable) p).accept(new OrToInVisitor());
        }
        return p;
    }

    public static Predicate complexQueryAsPredicate(){
        EntryObject eo = new PredicateBuilder().getEntryObject();
        Predicate p = eo.isNot("disabled_for_upload")
                .and(eo.get("media_id").notEqual(-1))
                .and(eo.isNot("inactive"))
                .and(eo.get("quality_check_state").equal(12))
                .and(eo.get("proxy_creation_state").equal(14))
                .and(eo.get("content_approval_media_state").notEqual(18))
                .and(eo.get("clock_approval_media_state").notEqual(42))
                .and(eo.get("class_approval_media_state").notEqual(72))
                .and(eo.get("num_instruction_rejected").equal(0))
                .and(eo.get("qc_approval_media_state").notEqual(22))
                .and(eo.get("approval_advert_state").notEqual(11))
                .and(eo.isNot("aired"))
                .and(eo.get("approval_advert_state").in(9,10));

        EntryObject eo1 = new PredicateBuilder().getEntryObject();
        Predicate p1 = eo1.get("media_content_approval_instruction").notEqual(3).and(eo1.get("content_approval_media_state").notEqual(19)).and(eo1.get("content_approval_media_state").notEqual(17));

        EntryObject eo2 = new PredicateBuilder().getEntryObject();
        Predicate p2 = eo2.get("media_clock_approval_instruction").notEqual(31).and(eo2.get("clock_approval_media_state").notEqual(41));

        EntryObject eo3 = new PredicateBuilder().getEntryObject();
        Predicate p3 = eo3.get("media_instruction_approval_instruction").notEqual(51)
                .and(eo3.get("num_instruction_approval_not_performed").greaterThan(0)
                                .or(eo3.get("num_instruction_rejected").greaterThan(0)
                                )
                );
        EntryObject eo4 = new PredicateBuilder().getEntryObject();
        Predicate p4 = eo4.get("media_class_approval_instruction").notEqual(41).and(eo4.get("class_approval_media_state").notEqual(71));

        EntryObject eo5 = new PredicateBuilder().getEntryObject();
        Predicate p5 = eo5.get("media_house_nr").equal("").and(eo5.get("media_house_nr_instruction").equal(10));

        Predicate pOr1 = Predicates.or(p1, p2);
        pOr1 = Predicates.or(pOr1, p3);
        pOr1 = Predicates.or(pOr1, p4);
        pOr1 = Predicates.or(pOr1, p5);

        Predicate pAnd = Predicates.and(p, pOr1);

        EntryObject eo6 = new PredicateBuilder().getEntryObject();
        Predicate p6 = eo6.get("time_delivered").lessEqual(0)
                .and(eo6.isNot("stopped"))
                .and(eo6.isNot("inactive"))
                .and(eo6.get("quality_check_state").equal(12))
                .and(eo6.get("proxy_creation_state").equal(14))
                .and(eo6.get("content_approval_media_state").equal(19).or(eo6.get("content_approval_media_state").equal(17).or(eo6.get("media_content_approval_instruction").notEqual(1))))
                .and(eo6.get("class_approval_media_state").equal(71).or(eo6.get("media_class_approval_instruction").notEqual(40)))
                .and(eo6.get("clock_approval_media_state").equal(41).or(eo6.get("media_clock_approval_instruction").notEqual(30)))
                .and(eo6.get("media_instruction_approval_instruction").notEqual(50).or(eo6.get("num_instruction_approval_not_performed").equal(0).and(eo6.get("num_instruction_rejected").equal(0))))
                .and(eo6.get("approval_advert_state").in(9,10))
                .and(eo6.get("media_house_nr").notEqual("").or(eo6.get("media_house_nr_instruction").equal(11)));

        pAnd = Predicates.and(pAnd, Predicates.not(p6));

        int[] mediaIds7 = {73474404,18,19,20,21,59673386,59673555,60386838,60386991,60387038,63063042,71930797,71930974,71931106,78158138,121,122,123,74724915,59791484,60474387,60995088,62718106,63355282,63355295,63803353,64695309,67657712,72587686,62017377,64854099,67102704,67102758,67102823,67102923,67102991,67409968,67425958,69581739,69902352,77041794,77042317,77042419,77042607,77133328,77133445,77133519,140,3264387,3264455,3264523,65982154,65982356,141,58721638,59698851,59698923,59805097,59805166,59805235,59805304,66225057,71930557,71930578,55097116,55101161,55103652,55106387,62046263,154,59738411,59738458,59738505,64397729,64897053,66485631,68213411,23,24,25,26,27,60505044,63293532,68582738,71916462,63294136,72,73,74,75,76,77,78,79,80,55874320,59809236,61011675,61886419,64693948,67322242,68273046,70072636};
        List<Integer> l7 = new ArrayList<Integer>();
        for(Integer mediaId : mediaIds7){
            l7.add(mediaId);
        }

        int[] mediaIds8 = {74534152,76342761,4294273,67387111,60362913,68253459,60362914,67820,64658131,68384425,71398673,62542181,3263119,59786652,59786673,16128,55831870,58381550,59949741,59820961,60422086,60422095,60422104,65113499,68280362,61162521,62827656,63150848,63152006,61190448,62017376,64854196,65709715,68039591,62149524,62149535,62259450,62259459,62307645,62325686,62470947,62506916,63528051,66406223,66406245,66406261,66406271,67529080,63592565,67160305,67160316,67160341,67160345,67160347,67160362,67274251,70194680,63593656,64854124,67551125,67596488,67643586,67643600,67643614,67643628,75213963,75213996,61905080,63817262,64205490,64944725,64944738,64944751,66378271,66759156,22,64572354,64631762,75336027,75336297,75336471,74807721,64792267,64794726,65450790,65450809,65450896,66333933,64858827,67565798,69945741,73766959,73767049,73767115,73767510,64975618,75669260,65040105,67428493,65879385,65196601,65196604,65196605,65196606,68039954,68040073,68040115,65302433,65850658,65850673,66274728,66276361};
        List<Integer> l8 = new ArrayList<Integer>();
        for(Integer mediaId : mediaIds8){
            l8.add(mediaId);
        }

        int[] mediaIds9 = {66276370,74834083,74834171,74834201,74834241,74834291,74834354,74834395,74834538,74834598,74835532,65337986,65337988,65337989,65337990,65337991,65337992,65337993,65850942,65881262,65881267,65881272,65881277,65881282,65881287,65881292,65881297,65881302,65881307,65881312,68310062,68332854,74911907,74912040,65337995,65337997,65337998,65338000,65338001,65338002,65338003,65850988,65882368,65882373,65882378,65882383,65882388,65882393,65882398,75829398,75845412,75845575,76234605,65338004,65851648,65851684,65851685,65851686,65851813,65851843,65851872,65851895,65851917,65338006,65338008,65338010,65338011,65338012,65874880,65874901,65874977,65874998,65875019,65875040,65875061,65875082,65338013,65338015,65963730,65963733,70583780,65338016,70685072,70685076,70685091,70685103,72415535,72433212,65338020,65534628,65554016,65604907,65338018,65659673,72587634,72605951,72650996,74709627,74709807,65674276,65736689,65737363,65737366,65737369,65737372,65737375,65737378,65737381,65737384,65737387};
        List<Integer> l9 = new ArrayList<Integer>();
        for(Integer mediaId : mediaIds9){
            l9.add(mediaId);
        }

        int[] mediaIds10 = {65737390,65773569,65773585,65773678,65773714,65773843,66083607,66083608,66083609,66083622,66710880,65707021,65773893,65773895,65773901,67051047,67051052,65794928,65796339,73908735,74030147,65796340,65796335,65985101,65985102,65985103,76797952,66024159,66044592,66055819,66097555,66102654,66137767,77463679,77463786,62677964,65338022,66251776,66395527,66409945,66409952,66409953,71933405,71933426,71933441,71933455,66453829,66453820,66453763,66453803,66478667,66482243,66969501,66969504,66969507,66969510,66969513,66969516,66969519,66969522,66506289,77375180,66547927,67159789,67159825,67159835,67159838,67159871,67159874,67159876,67159898,67159929,67159938,67159943,72637275,72639549,72901029,66580210,66583633,66584426,66605440,67160020,67160042,67160071,67160076,67160101,67160108,67160147,67160158,67160159,67160162,67160188,67160205,67160246,70183281,66633452,73752125,73752137,66768586,72602322,67736420,67158341,67158459,67158499,67158504,67158517,67158530,67158548,67158569,67158574,67158580,67158612,67158613,67158614,67158618,67158624,67158625,67158626,67158627,67158633,67158634,67158643,67158658,70178075,70178156,70178579,70178702,70178818,70179118,71308307,71308315,71308509};
        List<Integer> l10 = new ArrayList<Integer>();
        for(Integer mediaId : mediaIds10){
            l10.add(mediaId);
        }

        int[] mediaIds11 = {71308552,71308608,71308642,71308657,71308664,71308787,71308856,71308890,67158751,67158782,70142883,67158855,67158868,67158897,67158900,67158925,67158933,67158974,67159044,67159084,67159099,67159100,67159111,67159139,67159140,67159141,67159193,67159214,67159233,67159273,67159312,67159341,67159360,67159387,67159394,67159454,67159455,67159477,67159481,67159498,67159500,67159519,70193971,70193981,70194005,70194017,70194043,70194050,67159570,67159594,67159619,67159639,72465152,67159661,67159683,67159690,67159702,67159703,67359839,163,65337999,65721879,66816650,67158392,67340591,67354125,67365772,68559245,67431261,72221593,72221630,67431324,67431382,72217459,72217770,72218008,72218102,72218199,67431437,67431452,67431531,72242828,72242874,67431797,72243527,72243577,72243629,72243678,67431831,72195744,72195853,72196028,72196066,67461770,67563780,67579821,67672792,67683881,67693687,67805875,67805883,67805891,67805899,67805907,67805915,67805923,67805931,67805939,67805947,67805955,67805963,67805979,67805987,67805995,67806003,67806019,67806027,67806035,67806043,67806051,67806059,67806067,67806083,67806091,67806099,67806107,67806115,67806123,67806143,67806151,67806159,67806167,67806175};
        List<Integer> l11 = new ArrayList<Integer>();
        for(Integer mediaId : mediaIds11){
            l11.add(mediaId);
        }

        int[] mediaIds12 = {67806183,67806191,67806199,67806207,67806215,67806223,67806231,67806239,67806255,67806264,67806272,67806280,67806288,67806296,67806308,67806317,67693692,68073856,68073864,68073870,68073874,68073878,68073882,68073890,68073894,68073898,68073906,68074823,68074827,68074831,68074835,68074839,68074843,68074847,68074851,68074855,68074859,68074863,68074867,68074875,68074880,68074888,68074892,68074896,68074900,68074904,68074908,68074916,68074923,68074927,68074931,68074935,68074939,68074943,68074947,68074952,68074956,68074960,68074964,68074968,68074972,68074976,68074980,68074984,68074988,68074992,68074996,68075005,68075009,68075013,68075022,71850153,71850203,71850253,71850303,71850376,71850426,71850476,71850547,71850615,71850674,71850724,71850788,71850853,71850903,71850953,71851003,71851053,71851103,71851166,71851234,71851290,71851346,71851444,71851514,71851669,71851735,71851799,74419469,74419520,74419571,74419623,74419676,67745571,70191162,70191169,70191184,70191193,70191216,70191241,70191270,70191286,70191299,70191345,70191363,70191402,70191411,70191418,70191436,70191456,70191468,70191479,70191507,70191527,70191548,70191597,70191604,70191619,70191630,70191650,67776760,67788426,67788584};
        List<Integer> l12 = new ArrayList<Integer>();
        for(Integer mediaId : mediaIds12){
            l12.add(mediaId);
        }

        int[] mediaIds13 = {69879017,67796331,67905775,67912168,67912197,67912453,67912471,67950430,67978748,67987385,67999220,67988000,67988036,67988117,68022716,68106126,68073886,68073902,68074871,68074912,68075038,68074884,68075026,68075030,68075043,68075034,67805971,67806011,67806247,67806325,72504923,67806075,68467080,68565634,68573928,68574573,68804326,69109052,69294988,69401711,69401814,69401871,69500513,69592775,69642032,69784290,70213927,69843346,70120660,70190484,70347554,70377409,70613996,72945395,72945405,72945432,72945479,72945584,72945600,72945628,74978656,70628267,70685034,70718900,77627190,71098342,71244998,73178776,71316704,71321832,71963847,72040872,72235782,72235832,72235917,71331604,72375211,72427386,72905164,73761833,72947128,72947184,72947201,72947160,72947199,72947395,72947396,72947400,72947478,72947529,72947530,72947614,72947616,72947624,72947802,72947809,72947812,72996147,72996162,73004815,73061075,73063418,73079429,73099344,73099372,73099403,73212162,73212550,73212799,73217399,73223455,73507212,74801728,77812797,73474405,73474610,73566981,73571162,74084976,74084991,74084997,72079854,73764305,75284108,75286077,75286108,75286132,75286173,75286185,75286211};
        List<Integer> l13 = new ArrayList<Integer>();
        for(Integer mediaId : mediaIds13){
            l13.add(mediaId);
        }

        int[] mediaIds14 = {75286241,75286289,75286344,75286365,73838089,75398136,77423959,73905637,76568336,76568785,73913296,74000395,74002079,74590657,74590766,74593744,74593895,74002394,74371744,74000105,74235244,75312461,75312570,75312665,75313030,75313046,75313071,75313117,74253324,74253634,74253677,74253719,74253764,74253806,74272248,74419103,77555016,77555471,74420758,74442634,74490584,74541955,74567250,74590739,75394982,74052206,74594869,74595311,74595403,74595440,74595313,74621788,74625235,74630644,74636212,74636301,74637427,74639139,74639397,74639579,74639628,74637660,74640890,74642843,74643363,74645836,74659244,74659428,74659441,74659467,74659510,74659528,74659534,74659601,74659606,74659658,74659697,74659748,74659753,74659794,74659873,74659927,74659960,74659982,74659994,74660030,74660073,74661677,74661691,74661731};
        List<Integer> l14 = new ArrayList<Integer>();
        for(Integer mediaId : mediaIds14){
            l14.add(mediaId);
        }

        int[] mediaIds15 = {74661751,74661777,74661803,74661815,74661831,74661856,74661879,74661914,74661923,74661942,74661967,74662001,74662027,74671215,74671283,74683708,74697880,74698734,74698844,74698880,74698884,74698773,74698835,74698853,74698903,74704012,74708079,74713977,74739486,74747655,74748569,74749592,74750408,74751287,74751817,74752209,74752535,74774067,74775324,74776775,74786859,74790362,74791637,74796013,74798491,74803881,74804650,74805428,74807744,74818208,74821348,74835252,74837163,74837615,74838031,74838310,74838425,74838451,74838452,74838453,74838709,74849263,74850040,74855872,74856839,74858270,74858333,74859164,74861046,74862190,74863293,74864241,74864884,74866583,74867270,74868103,74869043,74869488,74881587,74883901,74884476,74886154,74886732,74887270,74887374,74888085,74888599,74890653,74891546,74909601,74916572,74916755,74916901,77383098,74917045,74921211,74922363,74923870,74924429,74924867,74925875,74926384,74926877,74928050,74932127,74932703,74934419,74936838,74938660,74939591,74941393,74950499};
        List<Integer> l15 = new ArrayList<Integer>();
        for(Integer mediaId : mediaIds15){
            l15.add(mediaId);
        }

        int[] mediaIds16 = {74962402,74966064,74987580,74988384,74990935,74991072,74991777,74992049,74997168,74998045,75004735,75006320,75020595,75023538,75032719,75032776,75032807,75032831,75032857,75032881,75032913,75032944,75032990,75033009,75033055,75033102,75033121,75033162,75033169,75033186,75033209,75033237,75033302,75033346,75311748,77620747,75033219,75035675,75037348,75037407,75037422,75038292,75038851,75038866,75039779,75043923,75045300,75045329,75045336,75045352,75045422,75045843,75047177,75047287,75047316,75047488,75047499,75047609,75047619,75063268,75064566,75067901,75076032,75077406,75158180,75190140,75223710,75223721,75223827,75239526,75243813,75245507,75246495,75249495,75254557,75303535,75317257,75328026,75349698,75350448,75353536,75353850,75354056,75354146,75354475,75354540,75354641,75354734,75354824,75369788,75401129,75472826,75611397,75719017,75868382,75877662,75885329,67158807,76052084,75984761,76061832,76087186,76089277,76100067,76123363,76135929,76204709,76207613,76209052,76215744,76216408,76216699};
        List<Integer> l16 = new ArrayList<Integer>();
        for(Integer mediaId : mediaIds16){
            l16.add(mediaId);
        }

        int[] mediaIds17 = {76217392,76225285,76240138,76241488,76247819,76259765,76268165,76937651,76272665,65963736,76287940,76289167,76295833,76296903,76297152,76297195,76313108,76318792,76322396,76323658,76325936,76330598,76342816,76354126,76355114,76355957,76357043,76358382,76359221,76359759,76362377,76362810,76369064,76369473,76369956,76370442,77411334,76644631,76859109,76880590,76984196,77034877,77272573,77002118,77329827,77398619,77451493,77737203,77738835,77739241,77739427,77739505,77739720,77739805,77739886,77738667,77740168,77740281,77740319,77740346,77740353,77740378,77740404,77740528,73178594,74698037,74698129,76429778,76657481,76657564,76657614,76657666,76657718,76657751,76657800,76657895,77537226,77561790,77623827,77667332,77755797,77798123,77831696,77833893,77948949,77962336,77970497,78178704};
        List<Integer> l17 = new ArrayList<Integer>();
        for(Integer mediaId : mediaIds17){
            l17.add(mediaId);
        }

        List<Integer> total = new ArrayList<Integer>();
        total.addAll(l7);
        total.addAll(l8);
        total.addAll(l9);
        total.addAll(l10);
        total.addAll(l11);
        total.addAll(l12);
        total.addAll(l13);
        total.addAll(l14);
        total.addAll(l15);
        total.addAll(l16);
        total.addAll(l17);

        EntryObject eoInMedias = new PredicateBuilder().getEntryObject();
        //Predicate inMedias = eoInMedias.get("media_id").in(total.toArray(new Integer[]{}));
        Predicate inMedias = eoInMedias.get("media_id").in(74,80,67806143,67806151,67806264,67806272,67806288,67806296,67806308,71321832);
        pAnd = Predicates.and(pAnd, inMedias);
        return pAnd;
    }
}
