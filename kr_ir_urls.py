"""한국 주요 바이오·제약(≥5천억) 큐레이션 IR **자료/발표자료** 페이지 URL.

각 회사 홈페이지에서 **IR 자료(IR덱·실적발표·IR PDF)가 올라오는 페이지**를 가리킨다
(공시/DART 페이지나 단순 뉴스 페이지 아님). discover.discover()가 한국(6자리) 종목에서
**가장 먼저** 이 맵을 확인(DART hm_url/크롤보다 우선). 새 종목/수정은 여기 추가하거나
모달의 수동 IR URL 입력(ticker_urls.json)으로 개별 override.
일부는 외부 IR 포털(*.irpage.co.kr / irupsite.co.kr), 일부는 전용 자료 페이지가 없어 IR 루트.
"""
from __future__ import annotations

KR_IR_URLS: dict[str, str] = {
    "207940": "https://samsungbiologics.com/kr/ir/financial-info/earning-release",  # 삼성바이오로직스
    "068270": "https://www.celltrion.com/ko-kr/investment/ir/presentations",   # 셀트리온
    "196170": "https://www.alteogen.com/kr/sub/ir/information.php?bid=2",       # 알테오젠
    "950160": "https://www.tissuegene.com/ko/investors/ir-material",           # 코오롱티슈진
    "326030": "https://www.skbp.com/kor/invest/presentationList.do",          # SK바이오팜
    "000100": "https://www.yuhan.co.kr/Invest/IR/Event/",                     # 유한양행
    "141080": "https://www.legochembio.com/invest/irdata.php?lang=k",          # 리가켐바이오
    "000250": "http://www.scd.co.kr/advertise/advertising_ir.jsp",            # 삼천당제약
    "298380": "https://www.ablbio.com/kr/company/reference",                  # 에이비엘바이오 (IR자료)
    "128940": "https://www.hanmi.co.kr/about/investor-relations/ir/financial-result/list.hm",  # 한미약품
    "087010": "https://peptron.irupsite.co.kr/",                             # 펩트론 (IR포털)
    "347850": "https://ddpharmatech.irpage.co.kr/",                          # 디앤디파마텍 (IRPage)
    "310210": "https://voronoi.irpage.co.kr/",                               # 보로노이 (IRPage)
    "145020": "https://www.hugel-inc.com/kr/investors/archives",            # 휴젤
    "009420": "https://hanall.com/kr/board/board.php?bo_table=irdata3",       # 한올바이오파마
    "302440": "https://www.skbioscience.com/kr/ir/stock_03",                 # SK바이오사이언스
    "226950": "https://www.olixpharma.com/ir/irbook.php",                    # 올릭스 (IR북)
    "237690": "https://www.stpharm.co.kr/ko/ir/ir-materials",                # 에스티팜
    "008930": "https://hanmiscience.co.kr/science/handler/Invest-Finance",    # 한미사이언스
    "068760": "https://www.celltrionph.com/ko-kr/ir/resultlist",             # 셀트리온제약
    "007390": "https://www.naturecell.co.kr/shareholder",                    # 네이처셀
    "006280": "https://www.gcbiopharma.com/kor/ir.do",                       # 녹십자 (JS SPA)
    "039200": "https://www.oscotec.co.kr/IRMaterials",                       # 오스코텍
    "096530": "https://kr.seegene.com/investors/earning-release",            # 씨젠 (JS SPA)
    "069620": "https://www.daewoong.co.kr/ko/ir/archives/event",             # 대웅제약 (JS SPA)
    "476830": "https://rznomics.irpage.co.kr/",                              # 알지노믹스 (IRPage)
    "475830": "https://orumrx.irpage.co.kr/",                                # 오름테라퓨틱 (IRPage)
    "127120": "https://jslink.co.kr/ir",                                     # 제이에스링크
    "195940": "https://www.inno-n.com/ir/report/ir_report/list",            # HK이노엔
    "003090": "https://www.daewoongholdings.com/daewoongkr/investment/investment_announce_list.web",  # 대웅(지주)
    "397030": "https://aprilbio.irpage.co.kr/",                              # 에이프릴바이오 (IRPage)
    "185750": "https://www.ckdpharm.com/invest/IR.do",                       # 종근당
    "115180": "http://www.qurient.com/bbs/board.php?bo_table=notice",        # 큐리언트 (Notice에 IR게재)
    "086450": "https://www.dkpharm.co.kr/boards/list.php?btype=ir",          # 동국제약
    "445680": "https://curiox.co.kr/sub/ir/notion.asp",                      # 큐리옥스바이오시스템즈
    "358570": "https://www.gi-innovation.com/kr/sub/investors/ir.asp",       # 지아이이노베이션
    "003850": "https://pharm.boryung.co.kr/ir/resource.do",                  # 보령
    "456160": "https://www.g2gbio.com/",                                     # 지투지바이오 (전용 IR자료 없음)
    "476060": "http://onconic.co.kr/kr/investors/pr.php",                    # 온코닉테라퓨틱스
    "317450": "https://myunginph.irpage.co.kr/",                             # 명인제약 (IRPage)
    "001060": "https://www.jw-pharma.co.kr/pharma/ko/prcenter/annual_report.jsp",  # JW중외제약
    "041960": "https://komipharm.co.kr/kr/invest_announce.php",              # 코미팜
    "102940": "https://www.kolonls.co.kr/ir/ir_list",                        # 코오롱생명과학
    "174900": "http://www.abclon.com/board_skin/board_list.asp?bbs_code=9",  # 앱클론 (bbs_code 미검증)
    "086900": "https://www.medytox.com/page/ir1",                            # 메디톡스
    "249420": "https://www.ildong.com/kor/bbs/ir/performance/list.id",       # 일동제약 (실적발표)
    "389470": "https://inventagelab.irupsite.co.kr/#section009",             # 인벤티지랩 (IR포털)
    "000640": "https://www.donga.co.kr/investment/ir-2",                     # 동아쏘시오홀딩스
}
