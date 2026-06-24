"""한국 주요 바이오·제약(≥5천억) 큐레이션 IR 페이지 URL.

discover.discover()가 한국(6자리) 종목에서 **가장 먼저** 이 맵을 확인한다
(DART hm_url/크롤보다 우선 — DART 홈페이지가 낡았거나 IR이 JS 렌더인 케이스 대응).
새 종목/수정은 여기 추가하거나, 모달의 수동 IR URL 입력(ticker_urls.json)으로 개별 override.
일부는 전용 IR 페이지가 없어 홈페이지만(네이처셀/지투지바이오), 일부는 외부 IR 포털(IRPage 등).
"""
from __future__ import annotations

KR_IR_URLS: dict[str, str] = {
    "207940": "https://samsungbiologics.com/kr/ir/overview",          # 삼성바이오로직스
    "068270": "https://www.celltrion.com/ko-kr/investment/ir",        # 셀트리온
    "196170": "https://www.alteogen.com/kr/sub/ir/information.php",   # 알테오젠
    "950160": "https://www.tissuegene.com/ko/investors/ir-material",  # 코오롱티슈진
    "326030": "https://www.skbp.com/kor/invest/presentationList.do",  # SK바이오팜
    "000100": "https://www.yuhan.co.kr/Invest/IR/Event/",             # 유한양행
    "141080": "https://www.legochembio.com/invest/irdata.php?lang=k",  # 리가켐바이오(구 도메인 IR)
    "000250": "http://www.scd.co.kr/advertise/advertising_ir.jsp",    # 삼천당제약
    "298380": "https://www.ablbio.com/kr/company/disclosure",         # 에이비엘바이오
    "128940": "https://www.hanmi.co.kr/hanmi/handler/Investment-IRnotice",  # 한미약품
    "087010": "http://www.peptron.co.kr",                             # 펩트론(홈페이지)
    "347850": "https://ddpharmatech.com/_view.php?bo_table=ir",       # 디앤디파마텍
    "310210": "https://voronoi.irpage.co.kr/",                        # 보로노이(IRPage)
    "145020": "https://www.hugel-inc.com/kr/investors/stock-summary",  # 휴젤
    "009420": "https://hanall.com/kr/board/board.php?bo_table=irdata",  # 한올바이오파마(hanall.com)
    "302440": "https://www.skbioscience.com/kr/ir/stock_01",          # SK바이오사이언스
    "226950": "https://www.olixpharma.com/ir/stock_info.php",         # 올릭스
    "237690": "https://www.stpharm.co.kr/ir",                         # 에스티팜
    "008930": "https://www.hanmiscience.co.kr/science/handler/Invest-Finance",  # 한미사이언스
    "068760": "https://www.celltrionph.com/ko-kr/ir/stockinfo",       # 셀트리온제약
    "007390": "https://www.naturecell.co.kr/",                        # 네이처셀(홈페이지)
    "006280": "https://www.gcbiopharma.com/kor/ir/ir_official_data_list.do",  # 녹십자
    "039200": "https://oscotec.co.kr/IRMaterials",                    # 오스코텍
    "096530": "https://www.seegene.co.kr/ir_event",                   # 씨젠
    "069620": "https://daewoong.co.kr/kr/invest/stock/info",          # 대웅제약
    "476830": "https://rznomics.irpage.co.kr/",                       # 알지노믹스(IRPage)
    "475830": "https://orumrx.irpage.co.kr/",                         # 오름테라퓨틱(IRPage)
    "127120": "https://jslink.co.kr/ir",                              # 제이에스링크
    "195940": "https://www.inno-n.com/ir/stock",                      # HK이노엔
    "003090": "https://daewoong.co.kr/ko/ir/holdings/highlights",     # 대웅(지주)
    "397030": "https://aprilbio.irpage.co.kr/",                       # 에이프릴바이오(IRPage)
    "185750": "https://www.ckdpharm.com/invest/IR.do",               # 종근당
    "115180": "http://www.qurient.com/",                             # 큐리언트(홈페이지)
    "086450": "http://www.dkpharm.co.kr/boards/list.php?btype=ir",   # 동국제약
    "445680": "https://curiox.co.kr/sub/ir/release.asp",            # 큐리옥스바이오시스템즈
    "358570": "https://www.gi-innovation.com/kr/sub/investors/ir.asp",  # 지아이이노베이션
    "003850": "https://pharm.boryung.co.kr/ir/resource.do",          # 보령
    "456160": "https://www.g2gbio.com/",                             # 지투지바이오(홈페이지)
    "476060": "http://onconic.co.kr/kr/investors/pr.php",            # 온코닉테라퓨틱스(http)
    "317450": "https://myunginph.irpage.co.kr/",                     # 명인제약(IRPage)
    "001060": "https://www.jw-pharma.co.kr/pharma/ko/investment/disclosure.jsp",  # JW중외제약
    "041960": "https://www.komipharm.co.kr/bbs/board.php?bo_table=invest_announce",  # 코미팜
    "102940": "https://www.kolonls.co.kr/ir/ir_list",                # 코오롱생명과학
    "174900": "https://abclon.com/board/bbs/board.php?bo_table=ir",  # 앱클론
    "086900": "https://www.medytoxkorea.com/IR",                     # 메디톡스
    "249420": "https://www.ildong.com/kor/bbs/ir/data/list.id",      # 일동제약
    "389470": "https://inventagelab.irupsite.co.kr/Default2.aspx",   # 인벤티지랩(외부 포털)
    "000640": "https://www.donga.co.kr/investment/ir-2?lang=ko",     # 동아쏘시오홀딩스
}
