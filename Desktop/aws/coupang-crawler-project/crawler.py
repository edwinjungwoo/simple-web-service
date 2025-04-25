#!/usr/bin/env python3
# crawler.py - 쿠팡 웹사이트 크롤링 스크립트 (최적화 구조)

import time
import json
import random
import pandas as pd
import os
import re
import traceback
import sys
import logging
from datetime import datetime
import argparse
from playwright.sync_api import sync_playwright, TimeoutError
import requests
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CRAWLER_CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'crawler_config.json')
CRAWLER_STATUS_PATH = os.path.join(BASE_DIR, 'config', 'crawler_status.json')
SELECTORS_PATH = os.path.join(BASE_DIR, 'config', 'selectors.json')

with open(CRAWLER_CONFIG_PATH, 'r', encoding='utf-8') as f:
    crawler_config = json.load(f)

with open(CRAWLER_STATUS_PATH, 'r', encoding='utf-8') as f:
    crawler_status = json.load(f)

with open(SELECTORS_PATH, 'r', encoding='utf-8') as f:
    selectors = json.load(f)

today = datetime.now().strftime("%Y-%m-%d")

# 로깅 설정
def setup_logging(log_level=logging.INFO):
    """로깅 시스템 설정"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    # playwright 로깅 레벨 설정
    logging.getLogger('playwright').setLevel(logging.WARNING)
    
    return logging.getLogger('coupang_crawler')

# 설정 관리 모듈
class ConfigManager:
    """설정 관리 클래스"""
    STATUS_FILE = "crawler_status.json"
    CONFIG_FILE = "crawler_config.json"
    SELECTORS_FILE = "selectors.json"
    
    USER_AGENTS = [
        # WebKit (Safari) User Agents
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6.1 Safari/605.1.15"
    ]
    
    def __init__(self, logger, base_dir=None):
        self.logger = logger
        self.base_dir = base_dir or os.path.dirname(os.path.abspath(__file__))
        self.config_path = os.path.join(self.base_dir, "config", self.CONFIG_FILE)
        self.status_path = os.path.join(self.base_dir, "config", self.STATUS_FILE)
        self.selectors_path = os.path.join(self.base_dir, "config", self.SELECTORS_FILE)
        self.config = self.load_config()
        self.selectors = self.load_selectors()
    
    def load_config(self):
        """설정 파일 로드"""
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    config = json.load(f)
                    self.logger.info(f"설정 파일 로드 완료: {self.config_path}")
                    return config
            except Exception as e:
                self.logger.error(f"설정 파일 로드 중 오류: {e}")
                
        # 설정 파일이 없는 경우에는 프로그램 실행 불가
        self.logger.error(f"{self.CONFIG_FILE} 파일이 없거나 손상되었습니다.")
        self.logger.info("프로그램을 실행하려면 유효한 설정 파일이 필요합니다.")
        sys.exit(1)  # 프로그램 종료
    
    def load_selectors(self):
        """셀렉터 설정 파일 로드"""
        if os.path.exists(self.selectors_path):
            try:
                with open(self.selectors_path, "r", encoding="utf-8") as f:
                    selectors = json.load(f)
                    self.logger.info(f"셀렉터 파일 로드 완료: {self.selectors_path}")
                    return selectors
            except Exception as e:
                self.logger.error(f"셀렉터 파일 로드 중 오류: {e}")
                
        # 셀렉터 파일이 없는 경우에는 프로그램 실행 불가
        self.logger.error(f"{self.SELECTORS_FILE} 파일이 없거나 손상되었습니다.")
        self.logger.info("프로그램을 실행하려면 유효한 셀렉터 파일이 필요합니다.")
        sys.exit(1)  # 프로그램 종료
        
    def save_status(self, file_path, batch_idx, overall_idx, blocked_url_idx=None):
        """크롤링 진행 상태 저장 (차단된 URL 인덱스 추가)"""
        status = {
            "file_path": file_path,
            "last_batch": batch_idx,
            "last_index": overall_idx,
            "blocked_url_idx": blocked_url_idx,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        try:
            with open(self.status_path, "w", encoding="utf-8") as f:
                json.dump(status, f)
                f.flush()  # 파일에 즉시 기록되도록 보장
            
            self.logger.info(f"상태 저장 완료: {status}")
            
            if blocked_url_idx is not None:
                self.logger.info(f"현재 상태 저장: 배치 {batch_idx}, 인덱스 {overall_idx}, 차단된 URL 인덱스: {blocked_url_idx}")
            else:
                self.logger.info(f"현재 상태 저장: 배치 {batch_idx}, 인덱스 {overall_idx}")
        except Exception as e:
            self.logger.error(f"상태 저장 중 오류: {e}")
    
    def load_status(self):
        """저장된 상태 불러오기"""
        if os.path.exists(self.status_path):
            try:
                with open(self.status_path, "r", encoding="utf-8") as f:
                    status = json.load(f)
                    self.logger.info(f"상태 파일 로드: {status}")  # 로드된 상태 출력
                    return status
            except Exception as e:
                self.logger.error(f"상태 파일 로드 중 오류: {e}")
        return None
    
    def get_random_wait_time(self, wait_type="url"):
        """랜덤 대기 시간 생성"""
        wait_config = self.config.get(f"{wait_type}_wait_time", {})
        min_time = wait_config.get("min")
        max_time = wait_config.get("max")
        
        # 설정 값 확인 및 기본값 설정
        if min_time is None or max_time is None:
            if wait_type == "url":
                min_time = 10  # URL 간 대기 시간 기본값
                max_time = 40
            else:  # batch
                min_time = 600  # 배치 간 대기 시간 기본값
                max_time = 900
            
            self.logger.warning(f"{wait_type} 대기 시간 설정이 올바르지 않습니다. 기본값 사용: {min_time}-{max_time}초")
        
        wait_time = random.uniform(min_time, max_time)
        self.logger.debug(f"생성된 {wait_type} 대기 시간: {wait_time:.2f}초")
        return wait_time
    
    def get_user_agent(self):
        """사용자 에이전트 설정 가져오기"""
        stealth_config = self.config.get("stealth_mode", {})
        user_agent = stealth_config.get("user_agent", "auto")
        
        if user_agent == "auto":
            return random.choice(self.USER_AGENTS)
        return user_agent
    
    def get_browser_type(self, batch_idx):
        """브라우저 타입 선택"""
        browsers = self.config.get("browsers", ["webkit"])
        return browsers[batch_idx % len(browsers)]
    
# 스텔스 모듈
class StealthManager:
    """브라우저 탐지 우회 관리 클래스"""
    
    def __init__(self, config_manager, logger):
        self.config_manager = config_manager
        self.logger = logger
    
    def apply_stealth_script(self, context):
        """스텔스 스크립트 적용"""
        stealth_config = self.config_manager.config.get("stealth_mode", {})
        if not stealth_config.get("enabled", True):
            self.logger.info("스텔스 모드 비활성화됨")
            return
        
        self.logger.info("스텔스 스크립트 적용")
        
        # 스텔스 스크립트 주입
        context.add_init_script("""
            // WebKit/Chromium용 자동화 감지 우회
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false
            });
            Object.defineProperty(window, 'navigator', {
                 value: new Proxy(navigator, {
                     has: (target, key) => key !== 'webdriver' && key in target,
                     get: (target, key) =>
                         key === 'webdriver'
                         ? false
                         : typeof target[key] === 'function'
                         ? target[key].bind(target)
                         : target[key]
                     })
            });
            
            // permissions API 우회 - 개선
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(parameters)
            );
            
             // 플러그인 에뮬레이션 - Chrome 기준 자연스럽게
            function MimeType(type, suffixes, description) {
                this.type = type;
                this.suffixes = suffixes;
                this.description = description;
            }
            Object.defineProperty(MimeType.prototype, 'enabledPlugin', { get: function() { return this._enabledPlugin; }});

            function Plugin(name, description, filename, mimeTypes) {
                this.name = name;
                this.description = description;
                this.filename = filename;
                this.length = mimeTypes.length;
                for (let i = 0; i < mimeTypes.length; i++) {
                    const mimeType = mimeTypes[i];
                    this[i] = mimeType;
                    Object.defineProperty(this, mimeType.type, { value: mimeType });
                    mimeType._enabledPlugin = this;
                }
            }
            Object.defineProperty(Plugin.prototype, 'length', { get: function() { return this._length || 0; }, set: function(val) { this._length = val; }});

            function PluginArray() { 
                this.length = 0;
                this.plugins = [];
            }
            PluginArray.prototype.item = function(index) { return this.plugins[index] || null; };
            PluginArray.prototype.namedItem = function(name) { return this.plugins.find(p => p.name === name) || null; };
            PluginArray.prototype.refresh = function() {};
            PluginArray.prototype.addPlugin = function(plugin) {
                this[this.length++] = plugin;
                this.plugins.push(plugin);
            };

            const pluginArray = new PluginArray();
            pluginArray.addPlugin(new Plugin('Chrome PDF Plugin', 'Portable Document Format', 'internal-pdf-viewer', [new MimeType('application/pdf', 'pdf', 'Portable Document Format')]));
            pluginArray.addPlugin(new Plugin('Chrome PDF Viewer', '', 'mhjfbmdgcfjbbpaeojofohoefgiehjai', [new MimeType('application/pdf', 'pdf', '')]));
            pluginArray.addPlugin(new Plugin('Native Client', '', 'internal-nacl-plugin', [new MimeType('application/x-nacl', 'nexe', 'Native Client Executable'), new MimeType('application/x-pnacl', 'pexe', 'Portable Native Client Executable')]));
            
            Object.defineProperty(navigator, 'plugins', { get: () => pluginArray });
            Object.defineProperty(navigator, 'mimeTypes', {
                 get: () => {
                     const mimeTypes = [];
                     pluginArray.plugins.forEach(p => {
                         for(let i=0; i<p.length; i++) mimeTypes.push(p[i]);
                     });
                     mimeTypes.item = (idx) => mimeTypes[idx] || null;
                     mimeTypes.namedItem = (name) => mimeTypes.find(m => m.type === name) || null;
                     return mimeTypes;
                 }
            });
            
            // 언어 설정 유지
            Object.defineProperty(navigator, 'languages', {
                get: () => ['ko-KR', 'ko', 'en-US', 'en']
            });
            
            // 누락된 navigator 속성 추가 (보다 인간적으로)
            if (!navigator.oscpu) {
                 Object.defineProperty(navigator, 'oscpu', { get: () => undefined }); // Chromium은 oscpu 없음
            }
             if (!navigator.vendor) {
                 Object.defineProperty(navigator, 'vendor', { get: () => 'Google Inc.'});
             }
             if (!navigator.platform) {
                 const platforms = ['Win32', 'Win64', 'MacIntel', 'Linux x86_64'];
                 Object.defineProperty(navigator, 'platform', { get: () => platforms[Math.floor(Math.random() * platforms.length)] });
            }
            
            // Canvas 지문 랜덤화 - 개선
            try {
                const proto = CanvasRenderingContext2D.prototype;
                const originalGetImageData = proto.getImageData;
                const originalIsPointInPath = proto.isPointInPath;
                const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;

                // 픽셀 데이터에 노이즈 추가
                proto.getImageData = function() {
                    const imageData = originalGetImageData.apply(this, arguments);
                    const data = imageData.data;
                    for (let i = 0; i < data.length; i += 4) {
                        const noise = Math.floor(Math.random() * 10) - 5; // -5 to 4
                        data[i] = Math.max(0, Math.min(255, data[i] + noise));
                        data[i+1] = Math.max(0, Math.min(255, data[i+1] + noise));
                        data[i+2] = Math.max(0, Math.min(255, data[i+2] + noise));
                    }
                    return imageData;
                };
                
                // isPointInPath 결과 살짝 변경
                 proto.isPointInPath = function() {
                     const result = originalIsPointInPath.apply(this, arguments);
                     return Math.random() < 0.01 ? !result : result; // 1% 확률로 결과 반전
                 };

                // toDataURL 결과에 미세한 변화 추가
                HTMLCanvasElement.prototype.toDataURL = function() {
                     const result = originalToDataURL.apply(this, arguments);
                     // 마지막 몇 바이트 변경
                     return result.substring(0, result.length - 8) + 
                            Array(8).fill(0).map(() => Math.floor(Math.random() * 10)).join('');
                 };
            } catch (err) { console.warn("Canvas 스푸핑 실패: ", err); }
            
            // WebGL 렌더러 정보 변경 - 더욱 다양한 값
            try {
                 const getParameter = WebGLRenderingContext.prototype.getParameter;
                 WebGLRenderingContext.prototype.getParameter = function(parameter) {
                     // UNMASKED_VENDOR_WEBGL
                     if (parameter === 37445) {
                         const vendors = ['Intel Open Source Technology Center', 'NVIDIA Corporation', 'Google Inc. (NVIDIA)', 'Apple Inc. (Apple M1)', 'Qualcomm Adreno (TM)'];
                         return vendors[Math.floor(Math.random() * vendors.length)];
                     }
                     // UNMASKED_RENDERER_WEBGL
                     if (parameter === 37446) {
                         const gpus = [
                            'Mesa DRI Intel(R) Iris(R) Xe Graphics (TGL GT2)',
                             'ANGLE (NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0)',
                             'ANGLE (Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0)',
                             'ANGLE (Apple M1 Pro)',
                             'Adreno (TM) 650'
                         ];
                         return gpus[Math.floor(Math.random() * gpus.length)];
                     }
                     return getParameter.apply(this, arguments);
                 };
            } catch (err) { console.warn("WebGL 스푸핑 실패: ", err); }

            // 화면 해상도 및 색상 깊이 설정 (viewport와 일치하도록)
            if (window.screen) {
                 try {
                     const viewportWidth = ${stealth_config.get("viewport", {}).get("width", 1920)};
                     const viewportHeight = ${stealth_config.get("viewport", {}).get("height", 1080)};
                     const depths = [24, 30, 32];
                     Object.defineProperty(window.screen, 'width', { get: () => viewportWidth });
                     Object.defineProperty(window.screen, 'height', { get: () => viewportHeight });
                     Object.defineProperty(window.screen, 'availWidth', { get: () => viewportWidth - Math.floor(Math.random() * 10) }); // 약간 작게
                     Object.defineProperty(window.screen, 'availHeight', { get: () => viewportHeight - Math.floor(Math.random() * 50) }); // 약간 작게
                     Object.defineProperty(window.screen, 'colorDepth', { get: () => depths[Math.floor(Math.random() * depths.length)] });
                     Object.defineProperty(window.screen, 'pixelDepth', { get: () => window.screen.colorDepth });
                 } catch (err) { console.warn("Screen 스푸핑 실패: ", err); }
             }
            
            // 자동화 감지를 위한 추가 속성 수정
            Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => [4, 8, 12, 16][Math.floor(Math.random() * 4)] });
            Object.defineProperty(navigator, 'deviceMemory', { get: () => [4, 8, 16][Math.floor(Math.random() * 3)] });
            
            // 사용자 에이전트 일관성 유지 (이미 설정됨)
            
            // CDP(Chrome DevTools Protocol) 관련 속성 제거
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
            delete window.chrome;
            
            // 콘솔 로그 감지 차단 (기존 유지)
            const originalConsoleLog = console.log;
            console.log = function() {
                 const args = Array.from(arguments).join(' ').toLowerCase();
                 if (args.includes('detection') || args.includes('botguard')) return;
                 return originalConsoleLog.apply(this, arguments);
            };

            // 타이밍 공격 방어 (약간의 랜덤 지연 추가)
             const originalGetTime = Date.prototype.getTime;
             Date.prototype.getTime = function() {
                 const realTime = originalGetTime.apply(this, arguments);
                 return realTime + Math.floor(Math.random() * 10); // 0-9ms 지연
             };
             const originalPerformanceNow = performance.now;
             performance.now = function() {
                 const realNow = originalPerformanceNow.apply(this, arguments);
                 return realNow + Math.random() * 1; // 0-1ms 지연
             };

        """)
    
    def setup_browser_context(self, browser):
        """브라우저 컨텍스트 설정"""
        stealth_config = self.config_manager.config.get("stealth_mode", {})
        proxy_config = self.config_manager.config.get("proxy", {})
        
        context_settings = {
            "viewport": stealth_config.get("viewport", {"width": 1920, "height": 1080}),
            "user_agent": self.config_manager.get_user_agent(),
            "bypass_csp": True,
            "ignore_https_errors": True,
        }
        
        # 프록시 설정 추가
        if proxy_config.get("enabled", False):
            proxy_server = proxy_config.get("server")
            username = proxy_config.get("username")
            password = proxy_config.get("password")
            
            if proxy_server and username and password:
                try:
                    # 프록시 연결 테스트
                    test_url = "https://ip.decodo.com/json"
                    proxy_string = f'http://{username}:{password}@{proxy_server}'
                    self.logger.info(f"프록시 서버 연결 테스트 시작: {proxy_server}")
                    
                    response = requests.get(test_url, proxies={
                        'http': proxy_string,
                        'https': proxy_string
                    }, timeout=10)
                    
                    if response.status_code == 200:
                        ip_info = response.json()
                        self.logger.info(f"프록시 연결 성공! IP: {ip_info.get('proxy', {}).get('ip')}")
                        self.logger.info(f"위치: {ip_info.get('city', {}).get('name')}, {ip_info.get('country', {}).get('name')}")
                        
                        context_settings["proxy"] = {
                            "server": f"http://{proxy_server}",
                            "username": username,
                            "password": password
                        }
                    else:
                        raise Exception(f"프록시 테스트 실패: 상태 코드 {response.status_code}")
                except Exception as e:
                    self.logger.error(f"프록시 연결 실패: {e}")
                    self.logger.warning("프록시 없이 진행합니다.")
                    # 프록시 설정 제거
                    context_settings.pop("proxy", None)
            else:
                self.logger.warning("프록시 설정이 불완전합니다. 프록시 없이 진행합니다.")
        
        try:
            context = browser.new_context(**context_settings)
            self.set_default_cookies(context)
            return context
        except Exception as e:
            self.logger.error(f"브라우저 컨텍스트 설정 중 오류 발생: {e}")
            raise
    
    def set_default_cookies(self, context):
        """기본 쿠키 설정"""
        # 기본 쿠키 (예: 언어 설정, 알림 거부 등)
        context.add_cookies([
            {
                "name": "language",
                "value": "ko",
                "domain": ".coupang.com",
                "path": "/"
            },
            {
                "name": "noPopup",
                "value": "true",
                "domain": ".coupang.com",
                "path": "/"
            }
        ])

# 데이터 추출 모듈
class DataExtractor:
    """제품 정보 추출 클래스"""
    
    def __init__(self, config_manager, logger):
        self.config_manager = config_manager
        self.logger = logger
        self.selectors = config_manager.selectors
    
    def detect_block(self, page, response=None):
        """차단 감지 함수"""
        # 상태 코드로 차단 감지
        if response and response.status in [403, 429, 503]:
            self.logger.warning(f"차단 감지: 상태 코드 {response.status}")
            return True
        
        try:
            # 차단 페이지 텍스트 패턴 검사
            block_patterns = [
                "access denied", 
                "액세스가 차단되었습니다",
                "비정상적인 접근", 
                "차단되었습니다", 
                "권한이 없습니다",
                "서비스 이용에 불편을 드려",
                "비정상적인 트래픽"
            ]
            
            html_content = page.content()
            
            for pattern in block_patterns:
                if pattern.lower() in html_content.lower():
                    self.logger.warning(f"차단 감지: '{pattern}' 패턴 발견")
                    return True
            
            # 주요 요소 부재 확인
            selectors = self.selectors.get("block_indicators", [
                "#contents", 
                ".prod-buy-header", 
                ".prod-price", 
                ".prod-sale-price"
            ])
            
            missing_elements = 0
            for selector in selectors:
                try:
                    if page.locator(selector).count() == 0:
                        missing_elements += 1
                except:
                    missing_elements += 1
            
            # 주요 요소 절반 이상 누락 시 차단으로 간주
            if missing_elements >= len(selectors) / 2:
                self.logger.warning(f"차단 감지: 주요 요소 {missing_elements}/{len(selectors)} 누락")
                return True
            
            # 페이지 타이틀 검사
            try:
                title = page.title()
                if "로봇" in title or "차단" in title or "접근 제한" in title:
                    self.logger.warning(f"차단 감지: 의심스러운 페이지 제목 '{title}'")
                    return True
            except:
                pass
            
            return False
        except Exception as e:
            self.logger.error(f"차단 감지 중 오류: {e}")
            return False
    
    def extract_product_info(self, page, url, original_index, prod_id=None):
        """제품 정보 추출 함수"""
        product_data = {"URL": url, "ORIGINAL_INDEX": original_index}
        
        # 원본 파일에서 가져온 PROD_ID 추가
        if prod_id is not None:
            product_data["PROD_ID"] = prod_id
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # 추출 시작 시간 기록
                start_time = time.time()
                
                # 1. 제품명 추출
                self._extract_product_name(page, product_data)
                
                # 2. 가격 정보 추출
                self._extract_price_info(page, product_data)
                
                # 추출 성공 로그
                end_time = time.time()
                extraction_time = end_time - start_time
                self.logger.info(f"제품 정보 추출 완료 ({extraction_time:.2f}초)")
                
                # 추출 시간을 데이터에 추가
                product_data['EXTRACTION_TIME'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                product_data['DATE'] = datetime.now().strftime("%Y-%m-%d")
                
                return product_data
                
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 5 * retry_count  # 재시도 간격 증가
                    self.logger.warning(f"제품 정보 추출 실패 ({retry_count}/{max_retries}). {wait_time}초 후 재시도...")
                    time.sleep(wait_time)
                    continue
                
                # 최대 재시도 횟수 도달
                self.logger.error(f"제품 정보 추출 최대 재시도 횟수 초과: {e}")
                self.logger.debug(traceback.format_exc())
                
                # 최소한의 정보라도 반환
                product_data.setdefault("COUPANG_PROD_NAME", None)
                product_data.setdefault("PRICE", None)
                product_data.setdefault("ORIGIN_PRICE", None)
                product_data.setdefault("COUPON", 0)
                product_data.setdefault("COUPON_PRICE", None)
                product_data.setdefault("AC_PRICE", None)
                product_data['EXTRACTION_TIME'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                product_data['ERROR'] = str(e)
                
                return product_data
    
    def _extract_product_name(self, page, product_data):
        """제품명 추출"""
        try:
            product_name_selectors = self.selectors.get("product_name", [
                "#contents .prod-buy-header h1",
                "h2.prod-buy-header__title",
                ".prod-buy-header h1"
            ])
            
            found_name = False
            for selector in product_name_selectors:
                try:
                    if page.locator(selector).count() > 0:
                        product_data['COUPANG_PROD_NAME'] = page.locator(selector).text_content().strip()
                        self.logger.info(f"제품명 추출: {product_data['COUPANG_PROD_NAME'][:30]}...")
                        found_name = True
                        break
                except Exception as e:
                    self.logger.debug(f"셀렉터({selector}) 제품명 추출 실패: {e}")
                    continue
            
            if not found_name:
                # 제품명을 찾지 못했다면 더 일반적인 셀렉터로 시도
                generic_name_selector = "h2"
                if page.locator(generic_name_selector).count() > 0:
                    h2_elements = page.locator(generic_name_selector).all()
                    longest_text = ""
                    for element in h2_elements:
                        text = element.text_content().strip()
                        if len(text) > len(longest_text):
                            longest_text = text
                    
                    if longest_text:
                        product_data['COUPANG_PROD_NAME'] = longest_text
                        self.logger.info(f"일반 셀렉터로 제품명 추출: {product_data['COUPANG_PROD_NAME'][:30]}...")
                        found_name = True
            
            if not found_name:
                product_data['COUPANG_PROD_NAME'] = "NA"
                self.logger.warning("제품명을 찾을 수 없습니다.")
                
        except Exception as e:
            self.logger.error(f"제품명 추출 오류: {e}")
            product_data['COUPANG_PROD_NAME'] = "NA"
    
    def _extract_price_info(self, page, product_data):
        """가격 정보 추출"""
        try:
            # 가격 추출 헬퍼 함수 - 여러 요소 처리 및 예외 처리
            def extract_price_safely(selectors, field_name):
                for selector in selectors:
                    try:
                        elements = page.locator(selector)
                        count = elements.count()
                        
                        if count == 1:
                            price_text = elements.text_content().strip()
                        elif count > 1:
                            self.logger.warning(f"{field_name} 셀렉터({selector})가 {count}개 요소를 찾았습니다. 첫 번째 요소 사용.")
                            price_text = elements.first.text_content().strip()
                        else:
                            continue
                        
                        # 숫자만 추출하여 정수로 변환
                        price_text = price_text.replace("원", "").replace(",", "").strip()
                        price_match = re.search(r'(\d+)', price_text)
                        if price_match:
                            return int(price_match.group(1))
                    except Exception as e:
                        self.logger.debug(f"{field_name} 셀렉터({selector}) 처리 중 오류: {e}")
                return None
            
            # 쿠폰 존재 여부 확인 함수
            def check_coupon_exists():
                coupon_selectors = self.selectors.get("coupon_indicators", [
                    ".price-txt-info.font-medium:text('쿠폰할인')",
                    "span.price-txt-info:text('쿠폰할인')",
                    ".coupon-price:not(:empty)"
                ])
                
                for selector in coupon_selectors:
                    try:
                        if page.locator(selector).count() > 0:
                            self.logger.info(f"쿠폰 확인: 셀렉터 '{selector}'에서 쿠폰 표시 발견")
                            return True
                    except Exception as e:
                        self.logger.debug(f"쿠폰 확인 셀렉터({selector}) 처리 중 오류: {e}")
                
                html_content = page.content()
                if 'class="price-txt-info font-medium">쿠폰할인<' in html_content:
                    self.logger.info("쿠폰 확인: HTML에서 '쿠폰할인' 클래스 발견")
                    return True
                
                self.logger.info("쿠폰 확인: 쿠폰 없음")
                return False
            
            # 1. 일반 가격(PRICE) 추출
            price_selectors = self.selectors.get("price", [
                "#contents > div.prod-atf > div.prod-atf-main .prod-sale-price.price-align span.total-price strong",
                "#contents > div.prod-atf > div.prod-atf-main .total-price:not(.price-strike) strong",
                "#contents > div.prod-atf > div.prod-atf-main .prod-price span.total-price"
            ])
            
            price = extract_price_safely(price_selectors, "PRICE")
            if price is not None:
                product_data['PRICE'] = price
                self.logger.info(f"가격(PRICE) 추출: {price:,}원")
            else:
                product_data['PRICE'] = "NA"
                self.logger.warning("가격(PRICE)을 찾을 수 없습니다.")
                
            # 2. 원래 가격(ORIGIN_PRICE) 추출 - 없으면 PRICE와 동일하게 설정
            origin_price_selectors = self.selectors.get("origin_price", [
                "#contents > div.prod-atf > div.prod-atf-main .prod-origin-price span.origin-price",
                "#contents > div.prod-atf > div.prod-atf-main .origin-price", 
                "#contents > div.prod-atf > div.prod-atf-main .price-strike"
            ])
            
            origin_price = extract_price_safely(origin_price_selectors, "ORIGIN_PRICE")
            if origin_price is not None:
                product_data['ORIGIN_PRICE'] = origin_price
                self.logger.info(f"원가(ORIGIN_PRICE) 추출: {origin_price:,}원")
            elif price is not None:
                # ORIGIN_PRICE가 없으면 PRICE와 동일하게 설정 (할인이 없는 경우)
                product_data['ORIGIN_PRICE'] = price
                self.logger.info(f"원가(ORIGIN_PRICE) 없음, PRICE 값 사용: {price:,}원")
            else:
                product_data['ORIGIN_PRICE'] = "NA"
                self.logger.warning("원가(ORIGIN_PRICE)와 가격(PRICE) 모두 찾을 수 없습니다.")
            
            # 3. 쿠폰 여부 확인 및 쿠폰 적용가 추출
            has_coupon = check_coupon_exists()
            
            if has_coupon:
                self.logger.info("쿠폰이 존재합니다.")
                product_data['COUPON'] = 1
                
                coupon_price_selectors = self.selectors.get("coupon_price", [
                    "#contents > div.prod-atf > div.prod-atf-main .prod-coupon-price span.total-price strong",
                    "#contents > div.prod-atf > div.prod-atf-main .major-price-coupon .total-price strong"
                ])
                
                coupon_price = extract_price_safely(coupon_price_selectors, "COUPON_PRICE")
                if coupon_price is not None:
                    product_data['COUPON_PRICE'] = coupon_price
                    self.logger.info(f"쿠폰가(COUPON_PRICE) 추출: {coupon_price:,}원")
                else:
                    product_data['COUPON_PRICE'] = None
                    self.logger.warning("쿠폰가(COUPON_PRICE)를 찾을 수 없습니다.")
                    product_data['COUPON'] = 0  # 쿠폰 적용가를 찾지 못했으므로 쿠폰 없음으로 재설정
            else:
                product_data['COUPON'] = 0
                product_data['COUPON_PRICE'] = None
            
            # 4. AppleCare 가격 추출
            ac_price_selectors = self.selectors.get("ac_price", [
                ".insurance-content__header__price",
                ".apple-care-price"
            ])
            
            ac_price = extract_price_safely(ac_price_selectors, "AC_PRICE")
            if ac_price is not None:
                product_data['AC_PRICE'] = ac_price
                self.logger.info(f"AppleCare 가격(AC_PRICE) 추출: {ac_price:,}원")
            else:
                # 대체 방법: 정규식으로 찾기
                try:
                    html_content = page.content()
                    ac_section_match = re.search(r'AppleCare[^<>]*?(\d{1,3}(?:,\d{3})+원)', html_content, re.IGNORECASE)
                    if ac_section_match:
                        ac_price_text = ac_section_match.group(1)
                        ac_price = int(ac_price_text.replace("원", "").replace(",", "").strip())
                        product_data['AC_PRICE'] = ac_price
                        self.logger.info(f"정규식으로 AppleCare 가격(AC_PRICE) 추출: {ac_price:,}원")
                    else:
                        product_data['AC_PRICE'] = "NA"
                        self.logger.info("AppleCare 가격(AC_PRICE)을 찾을 수 없습니다.")
                except Exception as e:
                    product_data['AC_PRICE'] = "NA"
                    self.logger.warning(f"AppleCare 가격 정규식 추출 실패: {e}")
        except Exception as e:
            self.logger.error(f"가격 정보 추출 오류: {e}")
            product_data['PRICE'] = "NA"
            product_data['ORIGIN_PRICE'] = "NA"
            product_data['COUPON'] = 0
            product_data['COUPON_PRICE'] = "NA"
            product_data['AC_PRICE'] = "NA"

# 크롤러 메인 클래스
class CoupangCrawler:
    """쿠팡 크롤러 메인 클래스"""
    
    def __init__(self, logger=None):
        # 로거 설정
        self.logger = logger if logger else setup_logging()
        self.logger.info("쿠팡 크롤러 초기화")
        
        # 설정 관리자 초기화
        self.config_manager = ConfigManager(self.logger)
        
        # 스텔스 관리자 초기화
        self.stealth_manager = StealthManager(self.config_manager, self.logger)
        
        # 데이터 추출기 초기화
        self.data_extractor = DataExtractor(self.config_manager, self.logger)
        
        # 통계 정보 초기화
        self.stats = {
            "total_urls": 0,
            "processed_urls": 0,
            "success_count": 0,
            "fail_count": 0,
            "block_count": 0,
            "start_time": None,
            "end_time": None
        }
    
    def process_urls(self, xlsx_file_path, batch_size=None, start_index=0, end_index=None, auto_restart=True, auto_validate=True):
        """URL 프로세싱 메인 함수 (auto_validate 옵션 추가)"""
        # 설정 로드
        config = self.config_manager.config
        batch_size = batch_size or config.get("batch_size", 15)
        
        try:
            # 실행 시작 시간 기록
            self.stats["start_time"] = datetime.now()
            
            # 현재 설정 출력
            self.logger.info(f"URL 대기 시간 설정: {config.get('url_wait_time')}초")
            self.logger.info(f"배치 대기 시간 설정: {config.get('batch_wait_time')}초")
            self.logger.info(f"차단 감지 시 대기 시간: {config.get('recovery_wait')}초 ({config.get('recovery_wait')//60}분)")
            
            # 엑셀 파일 확인
            if not os.path.exists(xlsx_file_path):
                self.logger.error(f"오류: '{xlsx_file_path}' 파일을 찾을 수 없습니다.")
                return
            
            # 엑셀 파일에서 데이터 읽기
            self.logger.info(f"파일 로드 중: {xlsx_file_path}")
            df = pd.read_excel(xlsx_file_path)
            
            # URL 컬럼 확인
            if 'URL' not in df.columns:
                self.logger.error(f"오류: URL 컬럼이 필요합니다.")
                return
            
            # 전체 URL 수 확인 - 디버깅용
            total_file_urls = len(df)
            self.logger.info(f"파일에서 로드된 총 URL 수: {total_file_urls}")
            
            # 모든 URL 중복 체크 - 디버깅용
            unique_urls = df['URL'].nunique()
            self.logger.info(f"파일의 고유 URL 수: {unique_urls} (중복 URL: {total_file_urls - unique_urls}개)")
            
            # 처리할 인덱스 범위 설정
            if end_index is None:
                end_index = len(df)
            else:
                end_index = min(end_index, len(df))
            
            # 자동 재시작 상태 확인
            blocked_url_idx = None
            if auto_restart:
                saved_status = self.config_manager.load_status()
                self.logger.info(f"로드된 상태: {saved_status}")  # 디버깅
                
                if saved_status and saved_status.get("file_path") == xlsx_file_path:
                    last_index = saved_status.get("last_index", 0)
                    blocked_url_idx = saved_status.get("blocked_url_idx")
                    
                    if blocked_url_idx is not None:
                        self.logger.info(f"이전 상태에서 차단된 URL부터 재시작: 인덱스 {blocked_url_idx}")
                        start_index = blocked_url_idx
                        self.logger.info(f"시작 인덱스 설정: {start_index}")
                    elif last_index > start_index:
                        saved_batch = saved_status.get("last_batch", 0)
                        self.logger.info(f"이전 상태에서 재시작: 배치 {saved_batch}, 인덱스 {last_index}")
                        start_index = last_index
                        self.logger.info(f"시작 인덱스 설정: {start_index}")
            
            # 지정된 범위의 데이터만 처리 (복사본 생성하여 원본 데이터 보존)
            df_to_process = df.iloc[start_index:end_index].copy().reset_index(drop=True)
            total_urls = len(df_to_process)
            self.stats["total_urls"] = total_urls
            
            # 처리할 URL 범위 로깅 - 디버깅용 
            self.logger.info(f"처리할 URL 범위: {start_index} ~ {end_index-1} (총 {total_urls}개)")
            if total_urls > 0:
                self.logger.info(f"첫 번째 URL: {df_to_process.iloc[0]['URL']}")
                if total_urls > 1:
                    self.logger.info(f"마지막 URL: {df_to_process.iloc[-1]['URL']}")
            
            self.logger.info(f"총 {total_urls}개의 URL을 {batch_size}개씩 배치 처리합니다.")
            
            # 연속 실패 카운터 초기화 (차단 감지용)
            consecutive_failures = 0
            max_consecutive_failures = min(batch_size // 7, 5)
            
            # 결과 파일 경로 설정
            output_dir = "RAW"
            os.makedirs(output_dir, exist_ok=True)

            # 항상 오늘 날짜를 포함한 파일명 사용
            output_basename = config.get("output_basename", "coupang_results")
            output_file = os.path.join(output_dir, f"{today}_{output_basename}.xlsx")
            
            # 배치별로 처리
            batch_idx = 0
            while batch_idx * batch_size < total_urls:
                batch_start = batch_idx * batch_size
                batch_end = min(batch_start + batch_size, total_urls)
                
                # 현재 배치 정보 저장 (자동 재시작을 위해)
                self.config_manager.save_status(xlsx_file_path, batch_idx, start_index + batch_start)
                
                self.logger.info(f"배치 {batch_idx + 1} 시작 (URL {batch_start+1}~{batch_end}/{total_urls})")
                
                # 배치에서 처리할 URL들 출력 - 디버깅용
                for debug_idx in range(batch_start, batch_end):
                    relative_debug_idx = debug_idx - batch_start
                    debug_url = df_to_process.iloc[relative_debug_idx]['URL']
                    self.logger.info(f"배치 내 URL {relative_debug_idx+1}: {debug_url}")
                
                # 일반 처리 (start_index 전달 추가)
                blocked, batch_results, blocked_idx = self._process_batch(df_to_process, batch_start, batch_end, consecutive_failures, start_index)
                
                # 배치 처리 결과 저장
                if batch_results:
                    # 배치 결과 저장 전 로깅
                    self.logger.info(f"저장할 배치 결과 수: {len(batch_results)}")
                    for i, result in enumerate(batch_results):
                        self.logger.info(f"결과 {i+1}: URL={result['URL']}")
                    
                    self._save_results_to_single_file(batch_results, output_file)
                
                # --- 배치 성공 후 상태 저장 추가 --- 
                # 배치 성공 후 상태 저장
                if not blocked:
                    self.logger.info(f"배치 {batch_idx + 1} 성공적으로 완료. 상태 저장 중...")
                    # 다음 시작 인덱스는 현재 배치의 끝 인덱스
                    next_start_index = start_index + batch_end 
                    # 다음 시작 배치는 현재 배치 번호 + 1
                    next_batch_idx = batch_idx + 1
                    self.config_manager.save_status(xlsx_file_path, next_batch_idx, next_start_index)
                    self.logger.info(f"상태 저장 완료. 다음 시작 인덱스: {next_start_index}, 다음 배치: {next_batch_idx}")
                # --- 상태 저장 끝 ---

                # 차단 감지 시 즉시 프로그램 종료 (3번 요구사항)
                if blocked:
                    self.logger.error("네트워크 차단이 감지되었습니다. 프로그램을 즉시 종료합니다.")
                    if blocked_idx is not None:
                        global_blocked_idx = start_index + batch_start + blocked_idx
                        self.config_manager.save_status(xlsx_file_path, batch_idx, start_index + batch_start, global_blocked_idx)
                        self.logger.info(f"차단된 URL 인덱스 저장: {global_blocked_idx}")
                    
                    # 차단 시 즉시 종료
                    return
                
                # 다음 배치로 넘어가기 전 대기
                if batch_idx * batch_size + batch_size < total_urls:  # 마지막 배치가 아니면 대기
                    wait_time = self.config_manager.get_random_wait_time("batch")
                    minutes = int(wait_time // 60)
                    seconds = int(wait_time % 60)
                    self.logger.info(f"다음 배치로 넘어가기 전 {minutes}분 {seconds}초 ({wait_time:.2f}초) 대기...")
                    time.sleep(wait_time)       
                
                # 다음 배치로 이동
                batch_idx += 1
                self.logger.info(f"다음 배치로 이동. 새 배치 인덱스: {batch_idx}")
                
                # 모든 배치가 완료되면 종료
                if batch_idx * batch_size >= total_urls:
                    self.stats["end_time"] = datetime.now()
                    
                    # 최종 통계 정보 출력
                    duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
                    self.logger.info("\n===== 크롤링 완료 =====")
                    self.logger.info(f"처리된 URL: {self.stats['processed_urls']}/{self.stats['total_urls']}")
                    self.logger.info(f"성공: {self.stats['success_count']} URLs ({self.stats['success_count']/max(self.stats['processed_urls'], 1)*100:.1f}%)")
                    self.logger.info(f"실패: {self.stats['fail_count']} URLs")
                    self.logger.info(f"차단 감지: {self.stats['block_count']} 회")
                    self.logger.info(f"총 소요 시간: {duration//3600:.0f}시간 {(duration%3600)//60:.0f}분 {duration%60:.0f}초")
                    self.logger.info(f"최종 결과 파일: {output_file}")
                    self.logger.info("========================")
                    
                    # 여기에 상태 파일 초기화 코드 추가
                    self.logger.info("크롤링이 완료되어 상태 파일을 초기화합니다.")
                    if os.path.exists(self.config_manager.STATUS_FILE):
                        initial_status = {
                            "file_path": "",
                            "last_batch": 0, 
                            "last_index": 0,
                            "blocked_url_idx": None,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        try:
                            with open(self.config_manager.STATUS_FILE, "w") as f:
                                json.dump(initial_status, f)
                                f.flush()
                            self.logger.info(f"{self.config_manager.STATUS_FILE} 파일이 초기화되었습니다.")
                        except Exception as e:
                            self.logger.error(f"상태 파일 초기화 중 오류: {e}")
                    
                    # 결과 검증 및 오류 URL 재수집 시작 (새로 추가됨)
                    if auto_validate and os.path.exists(output_file):
                        self.logger.info("\n===== 크롤링 완료, 자동 결과 검증 및 오류 URL 재수집 시작 =====")
                        validator = ResultValidator(self.logger)
                        validator.validate_and_recrawl(output_file)
                    
                    break
        
            self.stats["fail_count"] = 0
            self.stats["block_count"] = 0
            self.stats["start_time"] = None
            self.stats["end_time"] = None
        
        except Exception as e:
            self.logger.error(f"오류 발생: {e}")
            self.logger.error(traceback.format_exc())
    
    def _process_batch(self, df_to_process, batch_start, batch_end, consecutive_failures, start_index):
        """배치 처리 함수"""
        config = self.config_manager.config
        batch_results = []
        blocked = False
        blocked_idx = None
        
        # Playwright 시작
        with sync_playwright() as p:
            # 브라우저 시작 (배치당 한 번)
            browser_type = config.get("browsers", ["webkit"])[0] # 기본값 webkit 사용
            browser_instance = getattr(p, browser_type)
            
            is_headless = config.get("headless", False) 
            self.logger.info(f"Headless 모드: {'활성화' if is_headless else '비활성화'}")

            browser = browser_instance.launch(
                headless=is_headless, 
                args=[
                    '--disable-http2',
                    '--disable-gpu',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--disable-accelerated-jpeg-decoding',
                    '--disable-accelerated-mpeg4-decode',
                    '--disable-accelerated-video-decode',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-site-isolation-trials'
                ]
            )
            
            # 배치 범위 재확인
            self.logger.info(f"배치 처리 범위: batch_start={batch_start}, batch_end={batch_end}")
            self.logger.info(f"데이터프레임 크기: {len(df_to_process)}")
            
            # 배치 내 URL 처리
            for idx in range(batch_start, batch_end):
                relative_idx = idx - batch_start
                
                # 인덱스 유효성 검사 추가
                if relative_idx >= len(df_to_process):
                    self.logger.error(f"인덱스 오류: relative_idx({relative_idx})가 df_to_process 크기({len(df_to_process)})를 초과합니다")
                    break
                    
                row = df_to_process.iloc[idx]  # idx는 전체 데이터프레임의 절대 인덱스
                url = row['URL']
                
                # 원본 인덱스 계산 (전체 데이터프레임 기준)
                original_index = start_index + idx
                
                # PROD_ID 가져오기 (2번 요구사항)
                prod_id = None
                if 'PROD_ID' in row:
                    prod_id = row['PROD_ID']
                
                self.logger.info(f"URL {relative_idx+1}/{batch_end-batch_start} 처리 중: {url} (원본 인덱스: {original_index})")
                self.stats["processed_urls"] += 1
                
                context = None # 컨텍스트 초기화
                page = None    # 페이지 초기화
                
                try:
                    # --- 매 URL마다 새로운 컨텍스트 및 페이지 생성 --- 
                    self.logger.info("새 브라우저 컨텍스트 및 페이지 생성 중...")
                    context = self.stealth_manager.setup_browser_context(browser) # 스텔스 적용된 새 컨텍스트
                    self.stealth_manager.apply_stealth_script(context) # 스텔스 스크립트 주입
                    page = context.new_page()
                    page.set_default_timeout(60000) # 페이지 타임아웃 설정
                    self.logger.info("새 컨텍스트 및 페이지 생성 완료.")
                    # --- 생성 끝 ---

                    # --- 페이지 로딩 재시도 로직 (기존 유지) ---
                    max_goto_retries = 3
                    goto_retry_count = 0
                    response = None
                    
                    while goto_retry_count < max_goto_retries:
                        try:
                            response = page.goto(
                                url,
                                timeout=60000, # 타임아웃 60초 유지
                                wait_until="domcontentloaded" # domcontentloaded로 변경
                            )
                            if response and response.ok: # 응답이 있고 상태 코드가 2xx 인 경우 성공
                                self.logger.debug(f"페이지 로딩 성공 (시도 {goto_retry_count + 1}/{max_goto_retries})")
                                break # 성공 시 루프 탈출
                            else:
                                status = response.status if response else "No response"
                                raise Exception(f"페이지 로드 실패: 상태 코드 {status}")

                        except Exception as goto_error:
                            goto_retry_count += 1
                            if goto_retry_count < max_goto_retries:
                                wait_time = 5 * goto_retry_count
                                self.logger.warning(f"페이지 로딩 실패 (시도 {goto_retry_count}/{max_goto_retries}): {goto_error}. {wait_time}초 후 재시도...")
                                time.sleep(wait_time)
                                # 페이지 새로고침 시도 (선택적)
                                try:
                                    page.reload(wait_until="domcontentloaded", timeout=60000) # domcontentloaded로 변경
                                    self.logger.info("페이지 새로고침 시도 완료.")
                                except Exception as reload_error:
                                    self.logger.warning(f"페이지 새로고침 실패: {reload_error}")
                                    # 새로고침 실패 시 다음 시도로 넘어감
                            else:
                                self.logger.error(f"페이지 로딩 최대 재시도 횟수 초과: {goto_error}")
                                # 마지막 시도 실패 시 현재 URL 처리를 중단하고 다음 URL로 넘어감
                                raise goto_error # 바깥쪽 try-except 블록에서 처리하도록 예외 다시 발생
                    # --- 재시도 로직 끝 ---

                    if not response or not response.ok: # 최종적으로 로딩 실패 시
                         self.logger.error(f"페이지 최종 로드 실패: {url}")
                         self.stats["fail_count"] += 1
                         # 오류 데이터 저장
                         error_data = {
                             "URL": url, 
                             "ORIGINAL_INDEX": original_index,
                             "ERROR": "Page load failed after retries",
                             "COUPANG_PROD_NAME": "NA", 
                             "PRICE": "NA", 
                             "ORIGIN_PRICE": "NA", 
                             "COUPON": 0, 
                             "COUPON_PRICE": "NA", 
                             "AC_PRICE": "NA", 
                             "EXTRACTION_TIME": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                         }
                         # PROD_ID 추가 (있는 경우)
                         if prod_id is not None:
                             error_data["PROD_ID"] = prod_id
                             
                         batch_results.append(error_data)
                         continue # 다음 URL로 이동
                        
                    # 페이지 로딩 완료 대기 (추가) - 유지
                    try:
                        page.wait_for_selector("#contents", timeout=5000)
                    except:
                        self.logger.warning("기본 콘텐츠 요소를 찾을 수 없습니다.")
                    
                    # 차단 감지
                    if self.data_extractor.detect_block(page, response):
                        blocked = True
                        blocked_idx = relative_idx  # 차단된 URL의 상대 인덱스 저장
                        self.stats["block_count"] += 1
                        self.logger.warning(f"URL {relative_idx+1} 처리 중 차단 감지! 차단된 URL 인덱스: {blocked_idx}")
                        
                        # --- 차단된 페이지 HTML 저장 --- 
                        try:
                            blocked_page_content = page.content()
                            blocked_pages_dir = "blocked_pages"
                            os.makedirs(blocked_pages_dir, exist_ok=True)
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            # URL에서 파일 이름으로 사용하기 어려운 문자 제거
                            safe_url_part = re.sub(r'[^a-zA-Z0-9_-]', '_', url)[:50] 
                            file_path = os.path.join(blocked_pages_dir, f"blocked_{timestamp}_idx{start_index + batch_start + blocked_idx}_{safe_url_part}.html")
                            with open(file_path, "w", encoding="utf-8") as f:
                                f.write(blocked_page_content)
                            self.logger.info(f"차단된 페이지 HTML 저장 완료: {file_path}")
                        except Exception as save_error:
                            self.logger.error(f"차단된 페이지 HTML 저장 실패: {save_error}")
                        # --- 저장 로직 끝 ---
                        
                        break # 현재 배치 처리 중단
                    
                    # 제품 정보 추출 - 원본 인덱스 및 PROD_ID 전달
                    product_data = self.data_extractor.extract_product_info(page, url, original_index, prod_id)
                    
                    # 성공 시 결과 추가 및 통계 업데이트
                    batch_results.append(product_data)
                    self.stats["success_count"] += 1
                    
                    # 랜덤 대기 시간
                    if idx < batch_end - 1:  # 마지막 URL이 아닌 경우만 대기
                        wait_time = self.config_manager.get_random_wait_time("url")
                        minutes = int(wait_time // 60)
                        seconds = int(wait_time % 60)
                        self.logger.info(f"다음 URL로 넘어가기 전 {seconds}초 ({wait_time:.2f}초) 대기...")
                        time.sleep(wait_time)
                    
                except Exception as e:
                    # 페이지 로딩 실패 또는 차단 감지 외의 다른 오류 처리
                    self.logger.error(f"URL 처리 중 예외 발생: {e}") 
                    self.logger.debug(traceback.format_exc())
                    self.stats["fail_count"] += 1
                    
                    # 오류 발생 시에도 최소한의 정보 저장
                    error_data = {
                        "URL": url, 
                        "ORIGINAL_INDEX": original_index,  # 원본 인덱스 추가
                        "COUPANG_PROD_NAME": "NA", 
                        "PRICE": "NA", 
                        "ORIGIN_PRICE": "NA", 
                        "COUPON": 0, 
                        "COUPON_PRICE": "NA", 
                        "AC_PRICE": "NA", 
                        "EXTRACTION_TIME": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "ERROR": str(e)
                    }
                    # PROD_ID 추가 (있는 경우)
                    if prod_id is not None:
                        error_data["PROD_ID"] = prod_id
                        
                    batch_results.append(error_data)
                
                finally:
                    # --- 각 URL 처리 후 페이지 및 컨텍스트 닫기 --- 
                    if page:
                        try:
                            page.close()
                            self.logger.debug("페이지 닫힘.")
                        except Exception as page_close_error:
                            self.logger.warning(f"페이지 닫기 실패: {page_close_error}")
                    if context:
                        try:
                            context.close()
                            self.logger.debug("컨텍스트 닫힘.")
                        except Exception as context_close_error:
                            self.logger.warning(f"컨텍스트 닫기 실패: {context_close_error}")
                    # --- 닫기 끝 ---

            # 브라우저 종료 (배치 완료 후)
            browser.close()
            self.logger.info("브라우저 종료.")
        
        return blocked, batch_results, blocked_idx
    
    def _save_results_to_single_file(self, new_results, output_file):
        """결과를 단일 파일에 저장 (1번 요구사항)"""
        if not new_results:
            self.logger.warning("저장할 결과가 없습니다.")
            return
        
        # 결과를 데이터프레임으로 변환
        new_df = pd.DataFrame(new_results)
        self.logger.info(f"저장할 새 결과: {len(new_df)}개 항목")
        
        try:
            # 기존 파일이 있는지 확인
            if os.path.exists(output_file):
                # 기존 파일 로드
                existing_df = pd.read_excel(output_file)
                existing_count = len(existing_df)
                self.logger.info(f"기존 파일 로드: {existing_count}개 항목")
                
                # 데이터프레임 연결 (새 결과 추가)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                # 직접 최종 파일에 저장 (임시 파일 사용 없이)
                combined_df.to_excel(output_file, index=False)
                self.logger.info(f"결과가 {output_file}에 누적 저장되었습니다. (기존:{existing_count} + 새로운:{len(new_df)} = 총:{len(combined_df)}개 항목)")
            else:
                # 파일이 없는 경우 새로 생성
                new_df.to_excel(output_file, index=False)
                self.logger.info(f"결과가 {output_file}에 저장되었습니다. ({len(new_df)}개 항목)")
        except Exception as e:
            self.logger.error(f"파일 저장 중 오류 발생: {e}")
            self.logger.error(traceback.format_exc())
            
            # 오류 발생 시에도 고유한 백업 파일 생성
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = output_file.replace(".xlsx", f"_backup_{timestamp}.xlsx")
            new_df.to_excel(backup_file, index=False)
            self.logger.info(f"결과가 {backup_file}에 저장되었습니다. ({len(new_df)}개 항목)")

#############################################################################
#  결과 검증 및 오류 URL 자동 재수집 기능 (2025-04-25 추가)
#############################################################################

class ResultValidator:
    """크롤링 결과 검증 및 오류 URL 재수집 클래스"""
    
    def __init__(self, logger=None):
        # 로거 설정
        self.logger = logger if logger else setup_logging()
        self.logger.info("결과 검증기 초기화")
        
        # 설정 관리자 초기화
        self.config_manager = ConfigManager(self.logger)
        
        # 통계 정보 초기화
        self.stats = {
            "total_records": 0,
            "error_records": 0,
            "recrawled_records": 0,
            "fixed_records": 0,
            "remaining_errors": 0
        }
    
    def validate_and_recrawl(self, result_file, batch_size=5, wait_between_batches=True):
        """결과 파일을 검증하고 오류 URL 재수집"""
        self.logger.info(f"결과 파일 검증 및 재수집 시작: {result_file}")
        
        try:
            # 파일 존재 확인
            if not os.path.exists(result_file):
                self.logger.error(f"결과 파일이 존재하지 않습니다: {result_file}")
                return False
            
            # 결과 파일 로드
            df = pd.read_excel(result_file)
            self.stats["total_records"] = len(df)
            self.logger.info(f"총 {self.stats['total_records']}개 레코드 로드됨")
            
            # 원본 결과 파일 백업 (처음 크롤링한 버전 유지)
            orig_backup_file = result_file.replace(".xlsx", f"_original.xlsx")
            df.to_excel(orig_backup_file, index=False)
            self.logger.info(f"원본 크롤링 결과 백업 완료: {orig_backup_file}")
            
            # 오류 레코드 식별
            error_df = self._identify_error_records(df)
            self.stats["error_records"] = len(error_df)
            
            if self.stats["error_records"] == 0:
                self.logger.info("오류가 있는 레코드가 없습니다. 재수집이 필요하지 않습니다.")
                
                # 검증 결과 파일 생성 (원본과 동일)
                validated_file = result_file.replace(".xlsx", f"_validated.xlsx")
                df.to_excel(validated_file, index=False)
                self.logger.info(f"오류가 없어 원본과 동일한 검증 결과 파일 생성: {validated_file}")
                
                return True
            
            self.logger.info(f"재수집이 필요한 레코드: {self.stats['error_records']}개")
            
            # 오류 URLs 재수집
            fixed_records = self._recrawl_error_urls(error_df, result_file, batch_size, wait_between_batches)
            
            # 통계 업데이트
            self.stats["recrawled_records"] = len(fixed_records)
            self.stats["fixed_records"] = sum(1 for r in fixed_records if "ERROR" not in r or r.get("ERROR") is None)
            self.stats["remaining_errors"] = self.stats["recrawled_records"] - self.stats["fixed_records"]
            
            # 재수집 결과로 업데이트된 데이터프레임 생성
            updated_df = self._update_dataframe(df.copy(), fixed_records)
            
            # 검증 완료된 결과 파일 저장 (새 파일로)
            validated_file = result_file.replace(".xlsx", f"_validated_{today}.xlsx")
            updated_df.to_excel(validated_file, index=False)
            self.logger.info(f"검증 완료된 결과 파일 저장: {validated_file}")
            
            # 최종 통계 출력
            self._print_stats()
            
            return True
            
        except Exception as e:
            self.logger.error(f"결과 검증 및 재수집 중 오류 발생: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _identify_error_records(self, df):
        """오류가 있는 레코드 식별"""
        # 다음 조건 중 하나를 만족하는 레코드를 오류로 분류:
        # 1. ERROR 컬럼이 존재하며 값이 있는 경우
        # 2. 필수 필드(PRICE, ORIGIN_PRICE, COUPANG_PROD_NAME)가 "NA"인 경우
        
        error_conditions = []
        
        # ERROR 컬럼 존재 확인 및 값이 있는 레코드 식별
        if "ERROR" in df.columns:
            error_conditions.append(df["ERROR"].notna())
        
        # 필수 필드가 "NA"인 레코드 식별
        critical_fields = ["PRICE", "ORIGIN_PRICE", "COUPANG_PROD_NAME"]
        for field in critical_fields:
            if field in df.columns:
                error_conditions.append(df[field] == "NA")
        
        # 모든 조건을 OR 연산으로 결합
        if error_conditions:
            error_mask = error_conditions[0]
            for condition in error_conditions[1:]:
                error_mask = error_mask | condition
            
            error_df = df[error_mask].copy()
            return error_df
        
        # 오류 조건이 없는 경우 빈 DataFrame 반환
        return pd.DataFrame()
    
    def _recrawl_error_urls(self, error_df, result_file, batch_size=5, wait_between_batches=True):
        """오류 URL을 재수집"""
        if len(error_df) == 0:
            return []
        
        self.logger.info(f"오류 URL {len(error_df)}개 재수집 시작")
        
        # URL과 원본 인덱스만 포함하는 새 DataFrame 생성
        recrawl_df = pd.DataFrame({
            "URL": error_df["URL"],
            "ORIGINAL_INDEX": error_df["ORIGINAL_INDEX"]
        })
        
        # PROD_ID가 있으면 포함
        if "PROD_ID" in error_df.columns:
            recrawl_df["PROD_ID"] = error_df["PROD_ID"]
        
        # 재수집 결과를 저장할 리스트
        recrawled_results = []
        
        # 배치별로 처리
        for i in range(0, len(recrawl_df), batch_size):
            batch_df = recrawl_df.iloc[i:i+batch_size].copy()
            self.logger.info(f"배치 {i//batch_size + 1}/{(len(recrawl_df)-1)//batch_size + 1} 재수집 중 ({i+1}~{min(i+batch_size, len(recrawl_df))}/{len(recrawl_df)})")
            
            # 각 URL 처리
            batch_results = []
            with sync_playwright() as p:
                # 브라우저 시작
                browser_type = self.config_manager.config.get("browsers", ["webkit"])[0]
                browser_instance = getattr(p, browser_type)
                is_headless = self.config_manager.config.get("headless", False)
                
                browser = browser_instance.launch(
                    headless=is_headless,
                    args=[
                        '--disable-http2',
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-web-security'
                    ]
                )
                
                # 스텔스 관리자 초기화 (직접 생성)
                stealth_manager = StealthManager(self.config_manager, self.logger)
                data_extractor = DataExtractor(self.config_manager, self.logger)
                
                for idx, row in batch_df.iterrows():
                    url = row["URL"]
                    original_index = row["ORIGINAL_INDEX"]
                    prod_id = row.get("PROD_ID") if "PROD_ID" in row else None
                    
                    self.logger.info(f"URL 재수집 중: {url} (원본 인덱스: {original_index})")
                    
                    context = None
                    page = None
                    
                    try:
                        # 컨텍스트 및 페이지 생성
                        context = stealth_manager.setup_browser_context(browser)
                        stealth_manager.apply_stealth_script(context)
                        page = context.new_page()
                        page.set_default_timeout(60000)
                        
                        # 페이지 로딩
                        max_retries = 3
                        retry_count = 0
                        response = None
                        
                        while retry_count < max_retries:
                            try:
                                response = page.goto(
                                    url,
                                    timeout=60000,
                                    wait_until="domcontentloaded"
                                )
                                if response and response.ok:
                                    break
                                else:
                                    status = response.status if response else "No response"
                                    raise Exception(f"페이지 로드 실패: 상태 코드 {status}")
                            except Exception as goto_error:
                                retry_count += 1
                                if retry_count < max_retries:
                                    wait_time = 5 * retry_count
                                    self.logger.warning(f"페이지 로딩 실패 (시도 {retry_count}/{max_retries}): {goto_error}. {wait_time}초 후 재시도...")
                                    time.sleep(wait_time)
                                    try:
                                        page.reload(wait_until="domcontentloaded", timeout=60000)
                                    except Exception as reload_error:
                                        self.logger.warning(f"페이지 새로고침 실패: {reload_error}")
                                else:
                                    raise goto_error
                        
                        # 차단 감지
                        if data_extractor.detect_block(page, response):
                            self.logger.warning(f"URL 재수집 중 차단 감지: {url}")
                            raise Exception("차단 감지됨")
                        
                        # 제품 정보 추출
                        product_data = data_extractor.extract_product_info(page, url, original_index, prod_id)
                        batch_results.append(product_data)
                        self.logger.info(f"URL 재수집 성공: {url}")
                        
                        # URL 간 대기
                        if idx < len(batch_df) - 1:
                            wait_time = self.config_manager.get_random_wait_time("url")
                            self.logger.info(f"다음 URL로 넘어가기 전 {wait_time:.2f}초 대기...")
                            time.sleep(wait_time)
                    
                    except Exception as e:
                        self.logger.error(f"URL 재수집 중 오류 발생: {e}")
                        # 오류 발생 시 기존 정보 유지를 위해 원본 데이터를 유지
                        error_row = error_df.loc[error_df["ORIGINAL_INDEX"] == original_index].iloc[0].to_dict()
                        error_row["ERROR"] = f"재수집 실패: {str(e)}"
                        batch_results.append(error_row)
                    
                    finally:
                        # 페이지 및 컨텍스트 닫기
                        if page:
                            try:
                                page.close()
                            except Exception as page_close_error:
                                self.logger.warning(f"페이지 닫기 실패: {page_close_error}")
                        if context:
                            try:
                                context.close()
                            except Exception as context_close_error:
                                self.logger.warning(f"컨텍스트 닫기 실패: {context_close_error}")
                
                # 브라우저 종료
                browser.close()
                self.logger.info("브라우저 종료.")
            
            # 배치 결과 저장
            recrawled_results.extend(batch_results)
            
            # 배치 간 대기
            if wait_between_batches and i + batch_size < len(recrawl_df):
                wait_time = self.config_manager.get_random_wait_time("batch")
                self.logger.info(f"다음 배치로 넘어가기 전 {wait_time:.2f}초 대기...")
                time.sleep(wait_time)
        
        self.logger.info(f"오류 URL 재수집 완료: {len(recrawled_results)}/{len(error_df)} 처리됨")
        return recrawled_results
    
    def _update_dataframe(self, original_df, fixed_records):
        """데이터프레임 업데이트"""
        if not fixed_records:
            self.logger.info("업데이트할 레코드가 없습니다.")
            return original_df
        
        self.logger.info(f"데이터프레임 업데이트 중: {len(fixed_records)}개 레코드")
        
        # 오류 레코드 업데이트
        for fixed_record in fixed_records:
            if "ORIGINAL_INDEX" in fixed_record and fixed_record["ORIGINAL_INDEX"] is not None:
                orig_idx = fixed_record["ORIGINAL_INDEX"]
                
                # 해당 인덱스가 있는지 확인
                mask = original_df["ORIGINAL_INDEX"] == orig_idx
                if mask.any():
                    # ERROR 필드가 없거나 재수집에 성공한 경우만 업데이트
                    if "ERROR" not in fixed_record or fixed_record["ERROR"] is None:
                        # updated_df에서 해당 행 업데이트
                        for col, value in fixed_record.items():
                            if col in original_df.columns:
                                original_df.loc[mask, col] = value
                        
                        # ERROR 필드가 있으면 제거
                        if "ERROR" in original_df.columns:
                            original_df.loc[mask, "ERROR"] = None
                        
                        self.logger.info(f"레코드 업데이트 성공: 인덱스 {orig_idx}")
                    else:
                        self.logger.warning(f"레코드 재수집 실패, 원본 유지: 인덱스 {orig_idx}")
                else:
                    self.logger.warning(f"인덱스 {orig_idx}에 해당하는 원본 레코드를 찾을 수 없습니다.")
        
        return original_df
    
    def _print_stats(self):
        """결과 검증 및 재수집 통계 출력"""
        self.logger.info("\n===== 결과 검증 및 재수집 통계 =====")
        self.logger.info(f"총 레코드 수: {self.stats['total_records']}")
        self.logger.info(f"오류 레코드 수: {self.stats['error_records']} ({self.stats['error_records']/max(self.stats['total_records'], 1)*100:.1f}%)")
        self.logger.info(f"재수집 처리된 레코드 수: {self.stats['recrawled_records']}")
        self.logger.info(f"성공적으로 수정된 레코드 수: {self.stats['fixed_records']} ({self.stats['fixed_records']/max(self.stats['error_records'], 1)*100:.1f}%)")
        self.logger.info(f"여전히 오류가 있는 레코드 수: {self.stats['remaining_errors']}")
        self.logger.info("=====================================")

# 메인 함수
def main():
    # 명령줄 인자 파서 설정
    parser = argparse.ArgumentParser(description='쿠팡 제품 정보 크롤링 스크립트')
    parser.add_argument('--file', type=str, help='처리할 엑셀 파일 경로')
    parser.add_argument('--batch', type=int, default=None, help='배치당 URL 수 (기본값: 15)')
    parser.add_argument('--start', type=int, default=0, help='시작 인덱스 (기본값: 0, 첫 번째 URL부터)')
    parser.add_argument('--end', type=int, default=None, help='종료 인덱스 (기본값: None, 마지막 URL까지)')
    parser.add_argument('--no-restart', action='store_true', help='자동 재시작 비활성화 (기본값: 활성화)')
    parser.add_argument('--no-validate', action='store_true', help='자동 결과 검증 비활성화 (기본값: 활성화)')
    parser.add_argument('--config', action='store_true', help='설정 확인 및 변경')
    parser.add_argument('--debug', action='store_true', help='디버그 모드 활성화')
    parser.add_argument('--output', type=str, default=None, help='출력 파일 이름 (기본값: coupang.xlsx)')
    parser.add_argument('--validate-only', type=str, help='크롤링 없이 결과 파일만 검증 및 재수집')

    args = parser.parse_args()

    # 로깅 설정
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger = setup_logging(log_level)

    # 단일 검증 모드 (크롤링 없이 검증만 수행)
    if args.validate_only:
        logger.info(f"검증 전용 모드 시작: {args.validate_only}")
        validator = ResultValidator(logger)
        validator.validate_and_recrawl(args.validate_only)
        return

    # 크롤러 초기화
    crawler = CoupangCrawler(logger)

    # 출력 파일 설정
    if args.output:
        crawler.config_manager.config["output_file"] = args.output

    # 설정 확인 및 변경
    if args.config:
        config = crawler.config_manager.config
        logger.info("\n===== 현재 설정 =====")
        for key, value in config.items():
            logger.info(f"{key}: {value}")
        logger.info("\n설정은 'crawler_config.json' 파일을 직접 수정하여 변경할 수 있습니다.")
        return

    # 파일 경로가 지정되지 않은 경우
    if not args.file:
        logger.error("엑셀 파일 경로를 지정해야 합니다.")
        logger.info("사용법: python crawler.py --file [엑셀 파일 경로]")
        logger.info("\n자세한 사용법:")
        logger.info("  python crawler.py --file [엑셀 파일 경로] --batch 15")
        logger.info("  python crawler.py --file [엑셀 파일 경로] --start 100 --end 200")
        logger.info("  python crawler.py --file [엑셀 파일 경로] --no-restart")
        logger.info("  python crawler.py --file [엑셀 파일 경로] --no-validate")
        logger.info("  python crawler.py --config")
        logger.info("  python crawler.py --file [엑셀 파일 경로] --debug")
        logger.info("  python crawler.py --file [엑셀 파일 경로] --output my_results.xlsx")
        logger.info("  python crawler.py --validate-only [결과 파일 경로]")
        return

    # 자동 재시작 옵션 설정
    auto_restart = not args.no_restart
    logger.info(f"자동 재시작: {'비활성화' if args.no_restart else '활성화'}")

    # 자동 검증 옵션 설정
    auto_validate = not args.no_validate
    logger.info(f"자동 결과 검증: {'비활성화' if args.no_validate else '활성화'}")

    # 크롤링 실행
    crawler.process_urls(
        xlsx_file_path=args.file, 
        batch_size=args.batch, 
        start_index=args.start, 
        end_index=args.end, 
        auto_restart=auto_restart,
        auto_validate=auto_validate
    )

if __name__ == "__main__":
    main()