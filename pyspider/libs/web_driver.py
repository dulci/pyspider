# -*- encoding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from pyquery import PyQuery
from pyspider.libs.response import Response
from pyspider.libs.utils import *
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver import ActionChains
import time
import re

class Mydriver(object):
    @classmethod
    def phantomjs_driver(self):
        dcap = dict(DesiredCapabilities.PHANTOMJS)  #设置userAgent
        dcap["phantomjs.page.settings.userAgent"] = ("Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:16.0) Gecko/20121026 Firefox/16.0")
        dcap["phantomjs.page.settings.loadImages"] = False
        driver = webdriver.PhantomJS(service_args=['--ignore-ssl-errors=true', '--ssl-protocol=TLSv1'], executable_path='/Users/zhuyf/Software/phantomjs-2.1.1-macosx/bin/phantomjs',desired_capabilities=dcap)

        driver.start_session(webdriver.DesiredCapabilities.PHANTOMJS)
        driver.set_page_load_timeout(20)
        driver.maximize_window()
        driver.implicitly_wait(10)
        return driver

    @classmethod
    def chrome_driver(self, load_img=False):
        chrome_options = Options()
        chrome_options.add_argument('window-size=1920x3000') #指定浏览器分辨率
        chrome_options.add_argument('--disable-gpu') #谷歌文档提到需要加上这个属性来规避bug
        chrome_options.add_argument('--hide-scrollbars') #隐藏滚动条, 应对一些特殊页面
        if load_img != True:
            prefs = { "profile.managed_default_content_settings.images": 2 }
            chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument('--headless') #浏览器不提供可视化页面. linux下如果系统不支持可视化不加这条会启动失败
        chrome_options.add_argument('--no-sandbox')
        #chrome_options.binary_location = r"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" #手动指定使用的浏览器位置
        chrome_options.binary_location = r"/opt/google/chrome/google-chrome"
        driver = webdriver.Chrome(chrome_options=chrome_options)
        driver.implicitly_wait(10)
        return driver

    @classmethod
    def firefox_driver(self):
        user_agent = "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:16.0) Gecko/20121026 Firefox/16.0"
        caps = webdriver.DesiredCapabilities().FIREFOX
        caps["marionette"] = True
        options = webdriver.FirefoxOptions()
        options.add_argument('-headless')
        options.add_argument('--disable-gpu')
        # options.add_argument('window-size=1920x3000') 
        # options.add_argument('--hide-scrollbars')
        # options.add_argument('blink-settings=imagesEnabled=false')
        options.binary_location = '/Applications/Firefox.app/Contents/MacOS/firefox'
        profile = webdriver.FirefoxProfile()
        profile.set_preference('permissions.default.image', 2)
        profile.set_preference("browser.link.open_newwindow", 3)
        # profile.set_preference("general.useragent.override", user_agent)
        
        driver = webdriver.Firefox(capabilities=caps, firefox_profile=profile, firefox_options=options)
        # driver.maximize_window()
        driver.set_window_size(1920, 3000)
        driver.implicitly_wait(10)
        return driver

    @classmethod
    def crawler(self, **kwargs):
        assert kwargs.get('url') or (kwargs.get('web_driver') and kwargs.get('element')), 'no legal arguments, least use url or web_driver and element_xpath'
        if kwargs.get('url'):
            url = kwargs.get('url')
            driver = self.chrome_driver()
            driver.get(url)
            res = Response(url=driver.current_url, content='<html>%s</html>'%(re.sub(r'</?html>', '', driver.page_source, flags=re.IGNORECASE)))
            yield res.doc, driver
        elif kwargs.get('web_driver') and kwargs.get('element'):
            driver = kwargs.get('web_driver')
            element_xpath = get_xpath(kwargs.get('element'))
            source_handle = driver.current_window_handle
            actions = ActionChains(driver)
            element = driver.find_element_by_xpath(element_xpath)
            WebDriverWait(driver,10).until(EC.element_to_be_clickable((By.XPATH, element_xpath)))
            driver.execute_script("arguments[0].scrollIntoView()", element)
            ActionChains(driver).move_to_element(element).click().perform()
            # driver.find_element_by_xpath(element_xpath).click()
            # time.sleep(3)
            driver.switch_to_window(driver.window_handles[-1])
            url = driver.current_url
            window_handle = driver.current_window_handle
            res = Response(url=driver.current_url, content='<html>%s</html>'%(re.sub(r'</?html>', '', driver.page_source, flags=re.IGNORECASE)))
            if kwargs.get('is_final'):
                if window_handle == source_handle:
                    driver.back()
                else:
                    driver.close()
                    driver.switch_to_window(driver.window_handles[-1])
            yield res.doc, url
if __name__ == "__main__":
    for doc, driver in Mydriver.crawler(url='http://www.020gj.com/cs-wenti.html'):
        for each in doc('#newslist li').items():
            for details, url in Mydriver.crawler(web_driver=driver, element=each.find('a'), is_final=True):
                print(details.find('title').text())
                print(url)
        driver.quit()
                
    