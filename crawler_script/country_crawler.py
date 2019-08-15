from pyspider.database.sqlalchemy.resultdb import ResultDB
from pyspider.fetcher.proxy_pool import ProxyPool
import time
import requests
import re
from pyquery import PyQuery
from pyspider.libs.utils import *

def sleep(**dkargs):
    def wrapper(func):
        def __wrapper(*args, **kwargs):
            time.sleep(dkargs.get('time', 0.5))
            return func(*args, **kwargs)
        return __wrapper
    return wrapper

class CountryCrawler(object):
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:65.0) Gecko/20100101 Firefox/65.0'}
    def __init__(self):
        self.resultdb = ResultDB('mysql+mysqlconnector://gcj_admin:test@192.168.133.176:3306/caijia_zbxxcl')

    @sleep(time=1)
    def get(self, url,method=None,data=None,cookies=None,timeout=60):
        try:
            if method and method.lower() == 'post':
                response = requests.post(url,data=data,cookies=cookies,headers=self.headers,timeout=timeout)
            else:
                response = requests.get(url,params=data,cookies=cookies,headers=self.headers,timeout=timeout)
            html = response.content
            cookies = requests.utils.dict_from_cookiejar(response.cookies)
            return PyQuery(html.decode('utf-8')), cookies
        except Exception as e:
            return None, None

    def on_start(self, url, method=None, data=None, cookies=None):
        company_list_pyquery, cookies = self.get('http://jzsc.mohurd.gov.cn/dataservice/query/comp/list', method=method, data=data, cookies=cookies)
        print('company list page params is %s'%(data))
        if company_list_pyquery is None:
            print('company list page can not visit')
            return
        for each in company_list_pyquery('.cursorDefault tr').items():
            self.company_page('http://jzsc.mohurd.gov.cn%s'%(each.find('a').attr.href),{'company_code': each.find('td:nth-child(2)').text(), 'company_name': each.find('td:nth-child(3)').text(), 'company_id':md5string(each.find('td:nth-child(3)').text()), 'org_leader':each.find('td:nth-child(4)').text(), 'region':each.find('td:nth-child(5)').text()})
        total = re.search('tt:(\d+)', str(company_list_pyquery)).group(1)
        reload = re.search('\"\$reload\":(\d+)', str(company_list_pyquery)).group(1)
        page = re.search('pg:(\d+)', str(company_list_pyquery)).group(1)
        page_size = re.search('ps:(\d+)', str(company_list_pyquery)).group(1)
        page_count = re.search('pc:(\d+)', str(company_list_pyquery)).group(1)
        if int(page_count) > int(page):
            self.on_start('http://jzsc.mohurd.gov.cn/dataservice/query/comp/list',method='post',data={'$total':total,'$reload':reload,'$pg':int(page)+1,'$pgsz':page_size},cookies=cookies)

    def company_page(self, url, save={}):
        company_pyquery, cookies = self.get(url)
        if company_pyquery is None:
            print('company can not visit url is %s'%(url))
            return
        company_info = {'company_code': company_pyquery('.datas_table tr:nth-child(1) td').text().replace(chr(0xa0), ' '),
                        'company_name': save['company_name'], 'company_id': save['company_id'],
                        'org_leader': save['org_leader'], 'region': save['region'],
                        'register_type': company_pyquery('td[data-header=企业登记注册类型]').text(),
                        'company_address': company_pyquery('td[data-header=企业经营地址]').text()}
        company_info['qualifications'] = self.qualification_page('http://jzsc.mohurd.gov.cn/dataservice/query/comp/caDetailList/%s'%(url.split('/')[-1]))
        projects = list()
        self.project_list('http://jzsc.mohurd.gov.cn/dataservice/query/comp/compPerformanceListSys/%s'%(url.split('/')[-1]), projects=projects)
        company_info['projects'] = projects
        persons = list()
        self.person_list('http://jzsc.mohurd.gov.cn/dataservice/query/comp/regStaffList/%s'%(url.split('/')[-1]), persons=persons)
        company_info['persons'] = persons
        company_info['company_break_promises'] = self.company_break_promises('http://jzsc.mohurd.gov.cn/dataservice/query/comp/compCreditRecordList/%s/0' % (url.split('/')[-1]))
        company_info['black_list'] = self.black_list('http://jzsc.mohurd.gov.cn/dataservice/query/comp/compCreditBlackList/%s'%(url.split('/')[-1]))
        company_info['lose_credits'] = self.lose_credits('http://jzsc.mohurd.gov.cn/dataservice/query/comp/compPunishList/%s'%(url.split('/')[-1]))
        self.resultdb.save('country',company_info['company_id'],url,company_info,'self_crawler')
        return company_info

    def qualification_page(self, url):
        qualifications = list()
        qualification_pyquery, cookies = self.get(url)
        if qualification_pyquery is None:
            print('qualifications can not visit url is %s'%(url))
            return
        for each in qualification_pyquery('.row').items():
            qualifications.append(
                {'type': each.find('td:nth-child(2)').text(), 'code': each.find('td:nth-child(3)').text(),
                 'name': each.find('td:nth-child(4)').text(), 'start_date': each.find('td:nth-child(5)').text(),
                 'end_date': each.find('td:nth-child(6)').text(), 'gov': each.find('td:nth-child(7)').text()})
        return qualifications

    def project_list(self, url, method=None, data=None, cookies=None, projects=[]):
        project_list_pyquery, cookies = self.get(url,method=method,data=data,cookies=cookies)
        if project_list_pyquery is None:
            print('project list can not visit url is %s, data is %s'%(url, data))
            return
        for each in project_list_pyquery('.pro_table_box tbody tr').items():
            if not each.find('td[data-header]'):
                continue
            project_info = self.project_page('http://jzsc.mohurd.gov.cn/dataservice/query/project/projectDetail/%s' % (each.find('td:nth-child(2)').text()))
            if project_info:
                projects.append(project_info)
        if project_list_pyquery('.clearfix'):
            total = re.search('tt:(\d+)', str(project_list_pyquery)).group(1)
            reload = re.search('\"\$reload\":(\d+)', str(project_list_pyquery)).group(1)
            page = re.search('pg:(\d+)', str(project_list_pyquery)).group(1)
            page_size = re.search('ps:(\d+)', str(project_list_pyquery)).group(1)
            page_count = re.search('pc:(\d+)', str(project_list_pyquery)).group(1)
            if int(page_count) > int(page):
                self.project_list(url, method='post', data={'$total':total,'$reload':reload,'$pg':int(page)+1,'$pgsz':page_size},cookies=cookies, projects=projects)

    def project_page(self, url):
        project_pyquery, cookies = self.get(url)
        if project_pyquery is None:
            print('project page can not visit url is %s'%(url))
            return
        project_pyquery('.activeTinyTabContent dl').remove('span')
        investment = project_pyquery('dd:nth-child(9)').text()
        build_area = project_pyquery('dd:nth-child(10)').text()
        investment_value = re.search('\d+(.\d+)?', investment)
        build_area_value = re.search('\d+(.\d+)?', build_area)
        investment_unit = re.search('[(（]([\s\S]+)[）)]', investment)
        build_area_unit = re.search('[(（]([\s\S]+)[）)]', build_area)
        project_info = {'code': project_pyquery('dd:nth-child(1)').text(),
                        'name': project_pyquery('.user_info > b').attr.title,
                        'region': project_pyquery('dd:nth-child(3)').text(),
                        'type': project_pyquery('dd:nth-child(6)').text(),
                        'build_compay': project_pyquery('dd:nth-child(4)').text(),
                        'province_project_code': project_pyquery('dd:nth-child(2)').text(),
                        'build_company_code': project_pyquery('dd:nth-child(5)').text(),
                        'build_company_id': md5string(project_pyquery('dd:nth-child(4)').text()),
                        'type': project_pyquery('dd:nth-child(6)').text(),
                        'nature': project_pyquery('dd:nth-child(7)').text(),
                        'purpose': project_pyquery('dd:nth-child(8)').text(),
                        'investment': investment_value.group() if investment_value else None,
                        'investment_unit': investment_unit.group(1) if investment_unit else None,
                        'build_area': build_area_value.group() if build_area_value else None,
                        'build_area_unit': build_area_unit.group(1) if build_area_unit else None,
                        'level': project_pyquery('dd:nth-child(11)').text(),
                        'permission': project_pyquery('dd:nth-child(12)').text()}
        # 招投标信息
        bids = list()
        for one in project_pyquery('#tab_ztb tr').items():
            if one.find('td:nth-last-child(1)').find('a'):
                bid_pyquery, cookies = self.get('http://jzsc.mohurd.gov.cn%s' % (one.find('td:nth-last-child(1)').find('a').attr('data-url')), cookies=cookies)
                if bid_pyquery is None:
                    continue
                bids.append({'project_name': bid_pyquery('td[data-header=项目名称]').text(),
                             'project_code': bid_pyquery('td[data-header=项目编号]').text(),
                             'province_project_code': bid_pyquery('td[data-header=省级项目编号]').text(),
                             'build_company': bid_pyquery('td[data-header=建设单位]').text(),
                             'build_company_code': bid_pyquery('td[data-header=建设单位]').next().next().text(),
                             'build_company_id': md5string(bid_pyquery('td[data-header=建设单位]').text()),
                             'type': bid_pyquery('td[data-header=项目分类]').text(),
                             'region': bid_pyquery('td[data-header=项目属地]').text(),
                             'permission': bid_pyquery('td[data-header=立项文号]').text(),
                             'level': bid_pyquery('td[data-header=立项级别]').text(),
                             'investment': bid_pyquery('td[data-header=总投资（万元）]').text(),
                             'all_area': bid_pyquery('td[data-header=总面积（平方米）]').text(),
                             'nature': bid_pyquery('td[data-header=建设性质]').text(),
                             'purpose': bid_pyquery('td[data-header=工程用途]').text(),
                             'bid_code': bid_pyquery('td[data-header=中标通知书编号]').text(),
                             'province_bid_code': bid_pyquery('td[data-header=省级中标通知书编号]').text(),
                             'bid_type': bid_pyquery('td[data-header=招标类型]').text(),
                             'bid_mode': bid_pyquery('td[data-header=招标方式]').text(),
                             'winning_date': bid_pyquery('td[data-header=中标日期]').text(),
                             'winning_price': bid_pyquery('td[data-header=中标金额（万元）]').text(),
                             'scale': bid_pyquery('td[data-header=建设规模]').text(),
                             'build_area': bid_pyquery('td[data-header=面积（平方米）]').text(),
                             'agency_company': bid_pyquery('td[data-header=招标代理单位名称]').text(),
                             'agency_company_id': md5string(bid_pyquery('td[data-header=招标代理单位名称]').text()),
                             'agency_company_code': bid_pyquery('td[data-header=招标代理单位组织机构代码]').text(),
                             'winning_company': bid_pyquery('td[data-header=中标单位名称]').text(),
                             'winning_company_id': md5string(bid_pyquery('td[data-header=中标单位名称]').text()),
                             'winning_company_code': bid_pyquery('td[data-header=中标单位组织机构代码]').text(),
                             'project_manager': bid_pyquery('td[data-header=项目经理\/总监理工程师姓名]').text(),
                             'project_manager_code': bid_pyquery('td[data-header=项目经理\/总监理工程师身份证号码]').text(),
                             'register_date': bid_pyquery('td[data-header=记录登记时间]').text()})
        project_info['bids'] = bids
        # 施工图审查
        drawing_reviews = list()
        for one in project_pyquery('#tab_sgtsc tr').items():
            if one.find('td:nth-last-child(1)').find('a'):
                drawing_review_pyquery, cookies = self.get('http://jzsc.mohurd.gov.cn%s' % (one.find('td:nth-last-child(1)').find('a').attr('data-url')), cookies=cookies)
                if drawing_review_pyquery is None:
                    continue
                drawing_review = {'project_name': drawing_review_pyquery('td[data-header=项目名称]').text(),
                                  'project_code': drawing_review_pyquery('td[data-header=项目编号]').text(),
                                  'province_project_code': drawing_review_pyquery('td[data-header=省级项目编号]').text(),
                                  'build_company': drawing_review_pyquery('td[data-header=建设单位]').text(),
                                  'build_company_code': drawing_review_pyquery(
                                      'td[data-header=建设单位]').next().next().text(),
                                  'build_company_id': md5string(drawing_review_pyquery('td[data-header=建设单位]').text()),
                                  'type': drawing_review_pyquery('td[data-header=项目分类]').text(),
                                  'region': drawing_review_pyquery('td[data-header=项目属地]').text(),
                                  'permission': drawing_review_pyquery('td[data-header=立项文号]').text(),
                                  'level': drawing_review_pyquery('td[data-header=立项级别]').text(),
                                  'investment': drawing_review_pyquery('td[data-header=总投资（万元）]').text(),
                                  'all_area': drawing_review_pyquery('td[data-header=总面积（平方米）]').text(),
                                  'nature': drawing_review_pyquery('td[data-header=建设性质]').text(),
                                  'purpose': drawing_review_pyquery('td[data-header=工程用途]').text(),
                                  'review_company': drawing_review_pyquery('td[data-header=施工图审查机构名称]').text(),
                                  'review_company_id': md5string(
                                      drawing_review_pyquery('td[data-header=施工图审查机构名称]').text()),
                                  'review_company_code': drawing_review_pyquery('td[data-header=施工图审查机构组织机构代码]').text(),
                                  'drawing_review_code': drawing_review_pyquery('td[data-header=施工图审查合格书编号]').text(),
                                  'province_drawing_review_code': drawing_review_pyquery(
                                      'td[data-header=省级施工图审查合格书编号]').text(),
                                  'drawing_review_date': drawing_review_pyquery('td[data-header=审查完成日期]').text(),
                                  'scale': drawing_review_pyquery('td[data-header=建设规模]').text()}
                # 企业主体和从业人员信息
                for child in drawing_review_pyquery('.pro_table_borderright').items():
                    if child.find('thead>tr>th').size() == 4:
                        drawing_review_companies = list()
                        for c in child.find('tbody>tr').items():
                            drawing_review_companies.append({'company_type': c.find('td:nth-child(1)').text(),
                                                             'company_name': c.find('td:nth-child(2)').text(),
                                                             'company_id': md5string(c.find('td:nth-child(2)').text()),
                                                             'company_code': c.find('td:nth-child(3)').text(),
                                                             'company_region': c.find('td:nth-child(4)').text()})
                        drawing_review['companies'] = drawing_review_companies
                    elif child.find('thead>tr>th').size() == 7:
                        drawing_review_persons = list()
                        for p in child.find('tbody>tr').items():
                            drawing_review_persons.append({'company': p.find('td:nth-child(1)').text(),
                                                           'company_id': md5string(p.find('td:nth-child(1)').text()),
                                                           'major': p.find('td:nth-child(2)').text(),
                                                           'role': p.find('td:nth-child(3)').text(),
                                                           'name': p.find('td:nth-child(4)').text(),
                                                           'certificate_code': p.find('td:nth-child(5)').text(),
                                                           'major_type': p.find('td:nth-child(6)').text(),
                                                           'major_code': p.find('td:nth-child(7)').text()})
                        drawing_review['persons'] = drawing_review_persons
                drawing_reviews.append(drawing_review)
        project_info['drawing_reviews'] = drawing_reviews
        # 合同备案
        contracts = list()
        for one in project_pyquery('#tab_htba tr').items():
            if one.find('td:nth-last-child(1)').find('a'):
                contract_pyquery, cookies = self.get('http://jzsc.mohurd.gov.cn%s' % (one.find('td:nth-last-child(1)').find('a').attr('data-url')), cookies=cookies)
                if contract_pyquery is None:
                    continue
                contract = {'project_name': contract_pyquery('td[data-header=项目名称]').text(),
                            'project_code': contract_pyquery('td[data-header=项目编号]').text(),
                            'province_project_code': contract_pyquery('td[data-header=省级项目编号]').text(),
                            'build_company': contract_pyquery('td[data-header=建设单位]').text(),
                            'build_company_code': contract_pyquery('td[data-header=建设单位]').next().next().text(),
                            'build_company_id': md5string(contract_pyquery('td[data-header=建设单位]').text()),
                            'type': contract_pyquery('td[data-header=项目分类]').text(),
                            'region': contract_pyquery('td[data-header=项目属地]').text(),
                            'permission': contract_pyquery('td[data-header=立项文号]').text(),
                            'level': contract_pyquery('td[data-header=立项级别]').text(),
                            'investment': contract_pyquery('td[data-header=总投资（万元）]').text(),
                            'all_area': contract_pyquery('td[data-header=总面积（平方米）]').text(),
                            'nature': contract_pyquery('td[data-header=建设性质]').text(),
                            'purpose': contract_pyquery('td[data-header=工程用途]').text(),
                            'contract_backup_code': contract_pyquery('td[data-header=合同备案编号]').text(),
                            'province_contract_backup_code': contract_pyquery('td[data-header=省级合同备案编号]').text(),
                            'contract_code': contract_pyquery('td[data-header=合同编号]').text(),
                            'contract_classify': contract_pyquery('td[data-header=合同分类]').text(),
                            'contract_type': contract_pyquery('td[data-header=合同类别]').text(),
                            'contract_price': contract_pyquery('td[data-header=合同金额（万元）]').text(),
                            'scale': contract_pyquery('td[data-header=建设规模]').text(),
                            'contract_date': contract_pyquery('td[data-header=合同签订日期]').text(),
                            'employer_company': contract_pyquery('td[data-header=发包单位名称]').text(),
                            'employer_company_id': md5string(contract_pyquery('td[data-header=发包单位名称]').text()),
                            'employer_company_code': contract_pyquery('td[data-header=发包单位组织机构代码]').text(),
                            'employee_company': contract_pyquery('td[data-header=承包单位名称]').text(),
                            'employee_company_id': md5string(contract_pyquery('td[data-header=承包单位名称]').text()),
                            'employee_company_code': contract_pyquery('td[data-header=承包单位组织机构代码]').text(),
                            'union_employee_company': contract_pyquery('td[data-header=联合体承包单位名称]').text(),
                            'union_employee_company_id': md5string(contract_pyquery('td[data-header=联合体承包单位名称]').text()),
                            'union_employee_company_code': contract_pyquery('td[data-header=联合体承包单位组织机构代码]').text(),
                            'register_date': contract_pyquery('td[data-header=记录登记时间]').text()}
                contracts.append(contract)
        project_info['contracts'] = contracts
        # 施工许可
        construction_permits = list()
        for one in project_pyquery('#tab_sgxk tr').items():
            if one.find('td:nth-last-child(1)').find('a'):
                construction_permit_pyquery, cookies = self.get('http://jzsc.mohurd.gov.cn%s' % (one.find('td:nth-last-child(1)').find('a').attr('data-url')), cookies=cookies)
                if construction_permit_pyquery is None:
                    continue
                construction_permit = {'project_name': construction_permit_pyquery('td[data-header=项目名称]').text(),
                                       'project_code': construction_permit_pyquery('td[data-header=项目编号]').text(),
                                       'province_project_code': construction_permit_pyquery('td[data-header=省级项目编号]').text(),
                                       'build_company': construction_permit_pyquery('td[data-header=建设单位]').text(),
                                       'build_company_code': construction_permit_pyquery('td[data-header=建设单位]').next().next().text(),
                                       'build_company_id': md5string(construction_permit_pyquery('td[data-header=建设单位]').text()),
                                       'type': construction_permit_pyquery('td[data-header=项目分类]').text(),
                                       'region': construction_permit_pyquery('td[data-header=项目属地]').text(),
                                       'permission': construction_permit_pyquery('td[data-header=立项文号]').text(),
                                       'level': construction_permit_pyquery('td[data-header=立项级别]').text(),
                                       'investment': construction_permit_pyquery('td[data-header=总投资（万元）]').text(),
                                       'all_area': construction_permit_pyquery('td[data-header=总面积（平方米）]').text(),
                                       'nature': construction_permit_pyquery('td[data-header=建设性质]').text(),
                                       'purpose': construction_permit_pyquery('td[data-header=工程用途]').text(),
                                       'construction_permit_code': construction_permit_pyquery('td[data-header=施工许可证编号]').text(),
                                       'province_construction_permit_code': construction_permit_pyquery('td[data-header=省级施工许可证编号]').text(),
                                       'drawing_review_code': construction_permit_pyquery('td[data-header=施工图审查合格书编号]').text(),
                                       'contract_price': construction_permit_pyquery('td[data-header=合同金额（万元）]').text(),
                                       'project_manager': construction_permit_pyquery('td[data-header=项目经理]').text(),
                                       'project_manager_code': construction_permit_pyquery('td[data-header=项目经理身份证号]').text(),
                                       'project_director': construction_permit_pyquery('td[data-header=项目总监]').text(),
                                       'project_director_code': construction_permit_pyquery('td[data-header=项目总监身份证号]').text(),
                                       'area': construction_permit_pyquery('td[data-header=面积（平方米）]').text(),
                                       'register_date': construction_permit_pyquery('td[data-header=记录登记时间]').text()}
                construction_permit_companies = list()
                for c in construction_permit_pyquery('.pro_table_borderright tbody tr').items():
                    construction_permit_companies.append({'company_type': c.find('td:nth-child(1)').text(),
                                                          'company_name': c.find('td:nth-child(2)').text(),
                                                          'company_id': md5string(c.find('td:nth-child(2)').text()),
                                                          'company_code': c.find('td:nth-child(3)').text(),
                                                          'company_region': c.find('td:nth-child(4)').text()})
                construction_permit['companies'] = construction_permit_companies
                construction_permits.append(construction_permit)
        project_info['construction_permits'] = construction_permits
        # 竣工验收备案
        completed_reviews = list()
        for one in project_pyquery('#tab_jgysba tr').items():
            if one.find('td:nth-last-child(1)').find('a'):
                completed_review_pyquery, cookies = self.get('http://jzsc.mohurd.gov.cn%s' % (one.find('td:nth-last-child(1)').find('a').attr('data-url')), cookies=cookies)
                if completed_review_pyquery is None:
                    continue
                completed_review = {'project_name': completed_review_pyquery('td[data-header=项目名称]').text(),
                                    'project_code': completed_review_pyquery('td[data-header=项目编号]').text(),
                                    'province_project_code': completed_review_pyquery('td[data-header=省级项目编号]').text(),
                                    'build_company': completed_review_pyquery('td[data-header=建设单位]').text(),
                                    'build_company_code': completed_review_pyquery('td[data-header=建设单位]').next().next().text(),
                                    'build_company_id': md5string(completed_review_pyquery('td[data-header=建设单位]').text()),
                                    'type': completed_review_pyquery('td[data-header=项目分类]').text(),
                                    'region': completed_review_pyquery('td[data-header=项目属地]').text(),
                                    'permission': completed_review_pyquery('td[data-header=立项文号]').text(),
                                    'level': completed_review_pyquery('td[data-header=立项级别]').text(),
                                    'investment': completed_review_pyquery('td[data-header=总投资（万元）]').text(),
                                    'all_area': completed_review_pyquery('td[data-header=总面积（平方米）]').text(),
                                    'nature': completed_review_pyquery('td[data-header=建设性质]').text(),
                                    'purpose': completed_review_pyquery('td[data-header=工程用途]').text(),
                                    'completed_review_code': completed_review_pyquery('td[data-header=竣工备案编号]').text(),
                                    'province_completed_review_code': completed_review_pyquery('td[data-header=省级竣工备案编号]').text(),
                                    'actual_price': completed_review_pyquery('td[data-header=实际造价（万元）]').text(),
                                    'actual_area': completed_review_pyquery('td[data-header=实际面积（平方米）]').text(),
                                    'actual_scale': completed_review_pyquery('td[data-header=实际建设规模]').text(),
                                    'structure': completed_review_pyquery('td[data-header=结构体系]').text(),
                                    'actual_start_date': completed_review_pyquery('td[data-header=实际开工日期]').text(),
                                    'actual_end_date': completed_review_pyquery('td[data-header=实际竣工验收日期]').text(),
                                    'register_date': completed_review_pyquery('td[data-header=记录登记时间]').text(),
                                    'remark': completed_review_pyquery('td[data-header=备注]').text()}
                # 企业主体和从业人员信息
                for child in completed_review_pyquery('.pro_table_borderright').items():
                    if child.find('thead>tr>th').size() == 4:
                        completed_review_companies = list()
                        for c in child.find('tbody>tr').items():
                            completed_review_companies.append({'company_type': c.find('td:nth-child(1)').text(),
                                                               'company_name': c.find('td:nth-child(2)').text(),
                                                               'company_id': md5string(c.find('td:nth-child(2)').text()),
                                                               'company_code': c.find('td:nth-child(3)').text(),
                                                               'company_region': c.find('td:nth-child(4)').text()})
                        completed_review['companies'] = completed_review_companies
                    elif child.find('thead>tr>th').size() == 5:
                        completed_review_persons = list()
                        for p in child.find('tbody>tr').items():
                            completed_review_persons.append(
                                {'role': p.find('td:nth-child(1)').text(),
                                 'name': p.find('td:nth-child(2)').text(),
                                 'certificate_code': p.find('td:nth-child(3)').text(),
                                 'major_type': p.find('td:nth-child(4)').text(),
                                 'major_code': p.find('td:nth-child(5)').text()})
                        completed_review['persons'] = completed_review_persons
                completed_reviews.append(completed_review)
        project_info['completed_reviews'] = completed_reviews
        return project_info

    def person_list(self, url, method=None, data=None, cookies=None, persons=[]):
        person_list_pyquery, cookies = self.get(url,method=method,data=data,cookies=cookies)
        if person_list_pyquery is None:
            print('person list can not visit url is %s'%(url))
            return
        for each in person_list_pyquery('.pro_table_box tbody tr').items():
            if not each.find('td[data-header]'):
                continue
            person_info = self.person_page('http://jzsc.mohurd.gov.cn%s' % (re.search('href=\'([\s\S]+?)\'', each.find('td:nth-child(2) a').attr.onclick).group(1)))
            if person_info:
                persons.append(person_info)
        if person_list_pyquery('.clearfix'):
            total = re.search('tt:(\d+)', str(project_list_pyquery)).group(1)
            reload = re.search('\"\$reload\":(\d+)', str(project_list_pyquery)).group(1)
            page = re.search('pg:(\d+)', str(project_list_pyquery)).group(1)
            page_size = re.search('ps:(\d+)', str(project_list_pyquery)).group(1)
            page_count = re.search('pc:(\d+)', str(project_list_pyquery)).group(1)
            if int(page_count) > int(page):
                self.person_list(url, method='post', data={'$total':total,'$reload':reload,'$pg':int(page)+1,'$pgsz':page_size},cookies=cookies, persons=persons)

    def person_page(self, url):
        person_pyquery, cookies = self.get(url)
        if person_pyquery is None:
            print('person page can not visit url is %s'%(url))
            return
        person_pyquery('.activeTinyTabContent dl').remove('span')
        person_info = {'name': response.doc('.user_info > b').text(),
                       'sex': response.doc('.query_info_box .activeTinyTabContent dl dd:nth-child(1)').text(),
                       'certificate_type': response.doc('.query_info_box .activeTinyTabContent dl dd:nth-child(2)').text(),
                       'certificate_code': response.doc('.query_info_box .activeTinyTabContent dl dd:nth-child(3)').text()}
        # 人员资质
        qualifications = list()
        for one in person_pyquery('#regcert_tab > dl').items():
            one.remove('span')
            if one.find('dd').size() == 4:
                qualifications.append(
                    {'register_type': one.find('dd').eq(0).text(), 'certificate_code': one.find('dd').eq(1).text(),
                     'seal_code': one.find('dd').eq(2).text(), 'validate_date': one.find('dd').eq(3).text(),
                     'register_company': one.find('dt').eq(0).text(),
                     'register_company_id': md5string(one.find('dt').eq(0).text())})
            elif one.find('dd').size() == 5:
                qualifications.append(
                    {'register_type': one.find('dd').eq(0).text(), 'register_major': one.find('dd').eq(1).text(),
                     'certificate_code': one.find('dd').eq(2).text(), 'seal_code': one.find('dd').eq(3).text(),
                     'validate_date': one.find('dd').eq(4).text(), 'register_company': one.find('dt').eq(0).text(),
                     'register_company_id': md5string(one.find('dt').eq(0).text())})
        person_info['qualifications'] = qualifications
        # 变更记录
        change_infos = list()
        change_info_pyquery, cookies = self.get('http://jzsc.mohurd.gov.cn/dataservice/query/staff/staffWorkRecordList/%s' % (url.split('/')[-1]), cookies=cookies)
        if change_info_pyquery and change_info_pyquery('#table tbody tr').eq(0).find('td').size() == 2:
            for one in change_info_pyquery('#table tbody tr').items():
                one.find('.curQy').remove('small')
                changes = list()
                change_info = {'register_type': one.find('td').eq(0).text(), 'company': one.find('.curQy').text(),'company_id': md5string(one.find('.curQy').text())}
                for change in one.find('ul li').items():
                    companies = [x.text() for x in change.find('.cbp_tmlabel span').items()]
                    changes.append({'change_date': '/'.join(re.findall('\d+', change.find('.cbp_tmtime span').text())),
                                    'old_company': companies[0], 'old_company_id': md5string(companies[0]),
                                    'new_company': companies[1], 'new_company_id': md5string(companies[1])})
                change_info['changes'] = changes
                change_infos.append(change_info)
        person_info['change_infos'] = change_infos
        # 不良行为
        break_promises = list()
        break_promise_pyquery, cookies = self.get('http://jzsc.mohurd.gov.cn/dataservice/query/staff/staffCreditRecordList/%s/0' % (url.split('/')[-1]), cookies=cookies)
        if break_promise_pyquery and break_promise_pyquery('.pro_table_box tbody tr').eq(0).find('td').size() == 5:
            for one in break_promise_pyquery('.pro_table_box tbody tr').items():
                break_promise = {'break_promise_code': one.find('td').eq(0).find('span').text(),
                                 'break_promiser': one.find('td').eq(1).text(),
                                 'punish_date': re.search('\d{4}-\d{2}-\d{2}',one.find('td').eq(2).find('div').text()).group(),
                                 'punish_reason': one.find('td').eq(2).find('a').attr('data-text'),
                                 'punish_gov_code': one.find('td').eq(3).find('div').text(),
                                 'validate_date': one.find('td').eq(4).text()}
                one.find('td').eq(2).remove('div')
                one.find('td').eq(3).remove('div')
                break_promise['punish_content'] = one.find('td').eq(2).text()
                break_promise['punish_gov'] = one.find('td').eq(3).text()
                break_promises.append(break_promise)
        person_info['break_promises'] = break_promises
        return person_info

    def company_break_promises(self, url):
        # 企业不良行为
        company_break_promises = list()
        company_break_promise_pyquery, cookies = self.get(url)
        if company_break_promise_pyquery and company_break_promise_pyquery('.pro_table_box tbody tr').eq(0).find('td').size() == 5:
            for one in company_break_promise_pyquery('.pro_table_box tbody tr').items():
                company_break_promise = {'break_promise_code': one.find('td').eq(0).find('span').text(),
                                         'break_promiser': one.find('td').eq(1).text(),
                                         'punish_date': re.search('\d{4}-\d{2}-\d{2}',one.find('td').eq(2).find('div').text()).group(),
                                         'punish_reason': one.find('td').eq(2).find('a').attr('data-text'),
                                         'punish_gov_code': one.find('td').eq(3).find('div').text(),
                                         'validate_date': one.find('td').eq(4).text()}
                one.find('td').eq(2).remove('div')
                one.find('td').eq(3).remove('div')
                company_break_promise['punish_content'] = one.find('td').eq(2).text()
                company_break_promise['punish_gov'] = one.find('td').eq(3).text()
                company_break_promises.append(company_break_promise)
        return company_break_promises

    def black_list(self, url):
        # 黑名单
        black_list = list()
        black_list_pyquery, cookies = self.get(url)
        if black_list_pyquery and black_list_pyquery('.table_box tbody tr').eq(0).find('td').size() == 6:
            for one in black_lists_pyquery('.table_box tbody tr').items():
                one.find('td').eq(0).remove('div')
                black_record = {'black_record_code': one.find('td').eq(0).text(),
                                'black_record_object': one.find('td').eq(1).text(),
                                'reason': one.find('td').eq(2).text(), 'gov': one.find('td').eq(3).text(),
                                'start_date': one.find('td').eq(4).text(), 'end_date': one.find('td').eq(5).text()}
                black_list.append(black_record)
        return black_list

    def lose_credits(self, url):
        lose_credits = list()
        lose_credits_pyquery, cookies = self.get(url)
        if lose_credits_pyquery and lose_credits_pyquery('.table_box tbody tr').eq(0).find('td').size() == 6:
            for one in lose_credits_pyquery('.table_box tbody tr').items():
                one.find('td:nth-child(1)').remove('div')
                org_leader = one.find('td:nth-child(3) div').text()
                one.find('td:nth-child(3)').remove('div')
                lose_credit_gov_code = one.find('td:nth-child(4) div span').text()
                lose_credit_name = one.find('td:nth-child(4) div a').attr('data-text')
                one.find('td:nth-child(4)').remove('div')
                lose_credit = {'lose_credit_code': one.find('td:nth-child(1) span').text(),
                               'lose_credit_object': one.find('td:nth-child(2)').text(), 'org_leader': org_leader,
                               'org_leader_code': one.find('td:nth-child(3)').text(),
                               'lose_credit_gov_code': lose_credit_gov_code, 'lose_credit_name': lose_credit_name,
                               'lose_credit_reason': one.find('td:nth-child(4)').text(),
                               'lose_credit_gov': one.find('td:nth-child(5)').text(),
                               'punish_date': one.find('td:nth-child(6)').text()}
                lose_credits.append(lose_credit)
        return lose_credits

if __name__ == '__main__':
    cc = CountryCrawler()
    print(cc.on_start('http://jzsc.mohurd.gov.cn/dataservice/query/comp/list'))
    print('finish')