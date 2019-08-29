import os, sys
sys.path.append( os.path.join( os.path.abspath(os.path.dirname(__file__)) , '../'))
from pyspider.database.sqlalchemy.resultdb import ResultDB
from pyspider.database.redis.proxypooldb import Proxypooldb
from pyspider.fetcher.proxy_pool import ProxyPool
import time
import requests
import re
import logging
from pyquery import PyQuery
from pyspider.libs.utils import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import math
import json
from concurrent.futures import ThreadPoolExecutor
from country_crawler_thread_pool import *
import chardet
logger = logging.getLogger('guangzhou')
pool = ThreadPoolExecutor(max_workers=4)
@sleep(time=0)
def get(url,method=None,data=None,cookies=None,timeout=60,retry_num=0, proxy=None):
    try:
        proxy = proxypool.getProxy(proxy=proxy)
        if method and method.lower() == 'post':
            response = requests.post(url,data=data,cookies=cookies,headers=headers,timeout=timeout,proxies={'http': proxy}, verify=False)
        else:
            response = requests.get(url,params=data,cookies=cookies,headers=headers,timeout=timeout,proxies={'http': proxy}, verify=False)
        if response.status_code == 200:
            html = response.content
            cookies = requests.utils.dict_from_cookiejar(response.cookies)
            adchar = chardet.detect(html)
            html = html.decode(adchar['encoding'])
            return PyQuery(html), cookies
        else:
            logger.error('response code is not 200 is %s' % (response.status_code))
            return None, None
    except requests.exceptions.ProxyError as proxy_error:
        if retry_num < 3:
            logger.error('proxy is error will be retry proxy is %s, retry num is %s'%(proxy, retry_num+1))
            #proxypooldb.deleteIndexByProxy(proxy, 'http')
            return get(url, method, data, cookies, timeout, retry_num+1, proxy)
        else:
            logger.error('retry 3 is still error')
            return None, None
    except Exception as e:
        logger.error('request is error %s' % (e))
        return None, None

company_types = ['01','02','03','04','05','06','07','09','11','13','16','cgr','zfcgzrr']
dqsx = {'7':'港澳台地区','1':'本地市属','2':'本地省属','3':'本地部属','4':'省内进穗','5':'省外进穗','6':'国外'}
bz = {'SGD':'新加坡元','JPY':'日元','GPB':'英镑','TWD':'台币','CHF':'瑞士法郎','AUD':'澳元','RMB':'人民币','HKD':'港币','USD':'美元','EUR':'欧元'}
jjxz = {'04':'其它','01':'内资','02':'港澳台资','03':'国外投资'}
gtype = {'1':'房建','2':'市政'}
def onstart():
    for company_type in company_types:
        company_list('http://qyk.gzcc.gov.cn/qyww/wz/view/sccx/qygs.jsp',method='post',data={'page':1,'qyyj_qymc':'','qyyj_qylx':company_type})
    logger.error('finish')

def company_list(url, method=None, data=None, cookies=None):
    companies_pyquery, cookies = get(url,method,data,cookies)
    for each in companies_pyquery('.wsbs-table tr').items():
        if each.find('td'):
            logger.error(each.find('a').text())
            company = {'company_code':each.find('a').parent().prev().text(),
                       'company_name': each.find('a').text(),
                       'registration_matters': each.find('a').parent().next().attr.title,
                       'notice_start':each.find('a').parent().next().next().text(),
                       'notice_end': each.find('a').parent().next().next().next().text()}
            result = company_crawler('http://qyk.gzcc.gov.cn/qyww/sccx/basicInfoview.jsp?qybh=%s'%(each.find('a').parent().prev().text()),company)
            pool.submit(company_crawler, 'http://qyk.gzcc.gov.cn/qyww/sccx/basicInfoview.jsp?qybh=%s'%(each.find('a').parent().prev().text()), company)
    pages = int(re.search('共(\d+)页', response.doc('.pagination ul li:nth-last-child(1)').text()).group(1))
    page = data['page']
    if (pages-page) > 0:
        data['page'] = data['page'] + 1
        company_list(url,method=method,data=data,cookies=cookies)

def company_crawler(url, company_info):
    company_pyquery, cookies = get(url)
    if company_pyquery is None:
        return False
    # 企业类型
    company_types = list()
    for each in company_pyquery('#table1 tr').items():
        if re.match('\d{4}-\d{2}-\d{2}', each.find('td:nth-child(3)').text()):
            each.remove('script')
            company_types.append(
                {'type': each.find('td:nth-child(1)').text(), 'failure_time': each.find('td:nth-child(3)').text(),
                 'status': each.find('td:nth-child(4)').text()})
    company_info['types'] = company_types
    # 基础信息
    basic_info = json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([company_info['company_code']]),'method': 'findQyjczl', 'service': 'TQyQyjczlBS'}).content)
    company_info['basic_info'] = {'company_name': basic_info['qymc'], 'company_id': basic_info['yhid'],
                                       'company_address': basic_info['zzdz'],
                                       'region_type': dqsx.get(basic_info['dqsx']), 'start_time': basic_info['jlsj'],
                                       'register_capital': basic_info['czzb'], 'currency': bz.get(basic_info['bz']),
                                       'business_license_gov': basic_info['yyzzfzjg'],
                                       'business_license_code': basic_info['yyzzbh'],
                                       'license_start': basic_info['yyzzfzrq'],
                                       'economic_nature': jjxz.get(basic_info['jjxz']),
                                       'business_range': basic_info['zyxm'], 'other_range': basic_info['jycp'],
                                       'website': basic_info['gswz'], 'gov_catalogue': basic_info['zfcgml'],
                                       'supplier_type': basic_info['zfcglb'], 'industry': basic_info['zfcgjjlb'],
                                       'org_leader': basic_info['frdbxm'], 'org_code': basic_info['zzjgdm'],
                                       'org_code_end': basic_info['zzjgdmyxq'],
                                       'safe_certificate': basic_info['aqxkzbh'],
                                       'safe_certificate_gov': basic_info['aqxkzfzjg'],
                                       'safe_certificate_end': basic_info['aqxkzyxqend'],
                                       'branch_license_code': basic_info['fzjgyyzzh'], 'contact': basic_info['mc'],
                                       'email': basic_info['email'], 'tel': basic_info['tel'], 'fax': basic_info['fax']}
    # 重合同守信用企业
    zhtsxy = json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 1000, company_info['company_code']]),'method': 'findTQyZhtsxydwInfo', 'service': 'TQyZhtsxydwBS'}).content)
    keep_promise = list()
    for each in zhtsxy['data']:
        keep_promise.append({'year': each['nd'], 'keep_promise_gov': each['fzjg'], 'start_time': each['fzdate']})
    company_info['keep_promises'] = keep_promise
    # 企业年度纳税额
    ns = json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 1000, company_info['company_code']]),'method': 'findTQyQynswhInfo', 'service': 'TQyQynswhBS'}).content)
    taxes = list()
    for each in ns['data']:
        taxes.append({'year': each['nd'], 'income_tax': each['qysds'], 'sales_tax': each['qyyys'], 'all_tax': each['nsze']})
    company_info['taxes'] = taxes
    # 资质
    qualifications = json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 1000, company_info['company_code']]),'method': 'findTQyQyzzInfo', 'service': 'TQyQyzzBS'}).content)
    qualification_infos = list()
    for qualification in qualifications['data']:
        q = {'qualification_code': qualification['zzzsh'], 'failure_time': qualification['zsyxq'], 'infos': []}
        for each in json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 1000, qualification['qyzzid']]),'method': 'findZzxxInfo', 'service': 'TQyZzxxBS'}).content)['data']:
            q['infos'].append({'level': each['zzdj'], 'attr': each['sx'], 'gov': each['zszgbm'], 'start_time': each['fzrq'],'end_time': each['yxqjzrq'], 'content': each['zznrname']})
        qualification_infos.append(q)
    company_info['qualifications'] = qualification_infos
    # 企业获奖情况
    company_prizes = list()
    prizes = json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 1000, company_info['company_code']]),'method': 'findTQyQyjxInfo', 'service': 'TQyQyjxBS'}).content)
    for each in prizes['data']:
        company_prizes.append({'name': each['jxmc'], 'code': each['gcode'],'type': gtype[each['gtype']] if gtype.get(each['gtype']) else each['gtype'],'content': each['gnr'], 'start_time': each['fzdate']})
    company_info['prizes'] = company_prizes
    # 企业领导人
    leaders = json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 1000, company_info['company_code']]),'method': 'findTQyQyldryInfo', 'service': 'TQyQyldryBS'}).content)
    company_leaders = list()
    for each in leaders['data']:
        leader = {'name': each['xm'], 'type': each['ldlx'], 'gov': each['aqkhzsfzjg'],'failure_time': each['aqkhzsyxq'], 'code': each['aqkhzsh']}
        company_leaders.append(leader)
    company_info['leaders'] = company_leaders
    # 企业技术人员
    artisans = json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/', data={'arguments': json.dumps([0, 1000, {'qybh': company_info['company_code']}]), 'method': 'findQybhInfo','service': 'TQyZyjsryBS'}).content)
    company_artisans = list()
    for each in artisans['data']:
        artisan = {'code': each['rybh'], 'name': each['xm'], 'status': each['isdel'], 'certificate_type': each['zjlx'],'certificate_code': each['sfzh'], 'birthday': each['csrq'], 'qualifications': [],'safety_assessments': [], 'technical_level': []}
        # 资质信息
        for q in json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 1000, {'rybh': each['rybh']}]),'method': 'findTQyZyjsryzcxxInfo','service': 'TQyZyjsryzcxxBS'}).content)['data']:
            artisan['qualifications'].append(
                {'type': q['zclx'], 'level': q['zcdj'], 'code': q['zgzsh'], 'failure_time': q['zcyxq']})
        # 安全考核证书
        for s in json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 1000, {'rybh': each['rybh']}]),'method': 'findTQyAqkhzsInfo', 'service': 'TQyAqkhzsBS'}).content)['data']:
            artisan['safety_assessments'].append({'code': s['aqsckhzsh'], 'start_time': s['aqsckhzsfzrq'], 'end_time': s['aqsckhzsyxq'],'gov': s['fzjg'], 'type': s['zsxh']})
        # 技术职称
        for t in json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 1000, {'rybh': each['rybh']}]),'method': 'findTQyJszcTempInfo', 'service': 'TQyAqkhzsBS'}).content)['data']:
            artisan['technical_level'].append({'professional': t['jszczczy'], 'level': t['jszczcjb'], 'gov': t['jszcfzbm'],'start_time': t['jszcfzrq']})
        company_artisans.append(artisan)
    company_info['artisans'] = company_artisans
    # 项目业绩
    projects = json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 1000, company_info['company_code']]),'method': 'findTQyXmyjInfo', 'service': 'TQyXmyjBS'}).content)
    company_projects = list()
    for each in projects['data']:
        project = {'code': each['yjxmbh'], 'name': each['xmmc'], 'bid_code': each['zbtzs'],
                   'winning_date': each['zbtzsrq'], 'win_price': each['zbj'], 'build_company': each['jsdw'],
                   'project_manager': each['xmjlxm'], 'build_address': each['jsdd'], 'project_territory': each['xmsd'],
                   'contracting_content': each['sgcbnr'], 'contract_price': each['htj'],
                   'check_file_name': each['jgysclbhmc'], 'check_date': each['jgysrq'], 'design_company': each['sjdw'],
                   'supervise_company': each['jldw'], 'qualifications': [], 'scale': [], 'prizes': []}
        # 项目信息
        project_info = json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([{'xmyjid': each['xmyjid']}]),'method': 'findQyyj', 'service': 'XmyjBS'}).content)
        project.update({'contract_code': project_info['htbah'], 'project_address': project_info['xmdz'],
                        'project_manager': project_info['jg_xmjlxm'],
                        'project_manager_code': project_info['jg_xmjlrybh'], 'licence': project_info['sgxkzh'],
                        'licence_date': project_info['sgxkzrq'], 'licence_scale': project_info['xkzjsgm'],
                        'licence_build_company': project_info['xkzjsdw'], 'licence_price': project_info['xkzhtj'],
                        'licence_gov': project_info['xkzfzjg'], 'licence_main_person': project_info['xkzxmfzr'],
                        'evidence': project_info['qtzmclmc'], 'remark': project_info['bz'],
                        'company_code': project_info['qybh'], 'company_name': project_info['qymc']})
        # 工程对应企业资质
        for q in json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 500, {'xmyjid': each['xmyjid']}]),'method': 'findQyzz', 'service': 'XmyjBS'}).content)['data']:
            project['qualifications'].append({'name': q['zzmc']})
        # 项目规模
        for s in json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 500, {'xmyjid': each['xmyjid']}]),'method': 'findXmgm', 'service': 'XmyjBS'}).content)['data']:
            project['scale'].append({'type': s['gclb'], 'feature': s['fbtz'], 'target': s['gmzb'], 'amount': s['sl'], 'unit': s['dw']})
        # 获奖情况
        for p in json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 500, {'xmyjid': each['xmyjid']}]),'method': 'findHjqk', 'service': 'XmyjBS'}).content)['data']:
            project['prizes'].append({'year': p['nd'], 'name': p['hjmc'], 'prize_date': p['bjsj'], 'gov': p['bjdw'],'is_build': p['sfcjdw']})
        company_projects.append(project)
    company_info['projects'] = company_projects
    # 企业中标情况
    bids = list()
    for each in json.loads(requests.post('http://qyk.gzcc.gov.cn/qyww/json/',data={'arguments': json.dumps([0, 1000, company_info['company_code']]),'method': 'findJyBlzbtzsInfox', 'service': 'JyBlzbtzsBS'}).content)['data']:
        bids.append({'code': each['xmbh'], 'name': each['xmmc'], 'main_person': each['xmfzr'], 'start_date': each['ffsj']})
    company_info['bids'] = bids
    resultdb.save('guangzhou', company_info['company_code'], url, company_info, 'self_crawler')
if __name__ == '__main__':
    onstart()
    pool.shutdown()